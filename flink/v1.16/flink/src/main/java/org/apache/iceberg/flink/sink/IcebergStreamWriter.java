/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.iceberg.flink.log.LogSinkFunction;
import org.apache.iceberg.flink.log.LogWriteCallback;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

class IcebergStreamWriter<T> extends AbstractStreamOperator<WriteResult>
    implements OneInputStreamOperator<T, WriteResult>, BoundedOneInput {

  private static final long serialVersionUID = 1L;

  private final String fullTableName;
  private final TaskWriterFactory<T> taskWriterFactory;

  private transient TaskWriter<T> writer;
  private transient int subTaskId;
  private transient int attemptId;
  private transient IcebergStreamWriterMetrics writerMetrics;

  private LogSinkFunction logSinkFunction;
  @Nullable private transient LogWriteCallback logCallback;
  private transient SimpleContext sinkContext;
  private long currentWatermark = Long.MIN_VALUE;

  IcebergStreamWriter(
      String fullTableName,
      TaskWriterFactory<T> taskWriterFactory,
      LogSinkFunction logSinkFunction) {
    this.fullTableName = fullTableName;
    this.taskWriterFactory = taskWriterFactory;
    this.logSinkFunction = logSinkFunction;
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  @Override
  public void setup(
      StreamTask<?, ?> containingTask,
      StreamConfig config,
      Output<StreamRecord<WriteResult>> output) {
    super.setup(containingTask, config, output);
    if (logSinkFunction != null) {
      FunctionUtils.setFunctionRuntimeContext(logSinkFunction, getRuntimeContext());
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    if (logSinkFunction != null) {
      StreamingFunctionUtils.restoreFunctionState(context, logSinkFunction);
    }
  }

  @Override
  public void open() throws Exception {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();
    this.writerMetrics = new IcebergStreamWriterMetrics(super.metrics, fullTableName);

    // Initialize the task writer factory.
    this.taskWriterFactory.initialize(subTaskId, attemptId);

    // Initialize the task writer.
    this.writer = taskWriterFactory.create();

    this.sinkContext = new SimpleContext(getProcessingTimeService());
    if (logSinkFunction != null) {
      FunctionUtils.openFunction(logSinkFunction, new Configuration());
      logCallback = new LogWriteCallback();
      logSinkFunction.setWriteCallback(logCallback);
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
    this.writer = taskWriterFactory.create();
  }

  @Override
  public void processElement(StreamRecord<T> element) throws Exception {
    writer.write(element.getValue());
    if (logSinkFunction != null) {
      logSinkFunction.invoke(element.getValue(), sinkContext);
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    if (logSinkFunction != null) {
      StreamingFunctionUtils.snapshotFunctionState(
          context, getOperatorStateBackend(), logSinkFunction);
    }
  }

  @Override
  public void finish() throws Exception {
    super.finish();
    if (logSinkFunction != null) {
      logSinkFunction.finish();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (writer != null) {
      writer.close();
      writer = null;
    }
    if (logSinkFunction != null) {
      FunctionUtils.closeFunction(logSinkFunction);
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    if (logSinkFunction instanceof CheckpointListener) {
      ((CheckpointListener) logSinkFunction).notifyCheckpointComplete(checkpointId);
    }
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    super.notifyCheckpointAborted(checkpointId);
    if (logSinkFunction instanceof CheckpointListener) {
      ((CheckpointListener) logSinkFunction).notifyCheckpointAborted(checkpointId);
    }
  }

  @Override
  public void endInput() throws IOException {
    // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the
    // remaining completed files to downstream before closing the writer so that we won't miss any
    // of them.
    // Note that if the task is not closed after calling endInput, checkpoint may be triggered again
    // causing files to be sent repeatedly, the writer is marked as null after the last file is sent
    // to guard against duplicated writes.
    flush();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table_name", fullTableName)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  /** close all open files and emit files to downstream committer operator */
  private void flush() throws IOException {
    if (writer == null) {
      return;
    }

    long startNano = System.nanoTime();
    WriteResult result = writer.complete();
    if (logCallback != null) {
      result.setLogStorePartitionOffsets(logCallback.offsets());
    }
    writerMetrics.updateFlushResult(result);
    output.collect(new StreamRecord<>(result));
    writerMetrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));

    // Set writer to null to prevent duplicate flushes in the corner case of
    // prepareSnapshotPreBarrier happening after endInput.
    writer = null;
  }

  private class SimpleContext implements SinkFunction.Context {

    @Nullable private Long timestamp;

    private final ProcessingTimeService processingTimeService;

    SimpleContext(ProcessingTimeService processingTimeService) {
      this.processingTimeService = processingTimeService;
    }

    @Override
    public long currentProcessingTime() {
      return processingTimeService.getCurrentProcessingTime();
    }

    @Override
    public long currentWatermark() {
      return currentWatermark;
    }

    @Override
    public Long timestamp() {
      return timestamp;
    }
  }
}
