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
package org.apache.iceberg.flink.kafka;

import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.log.LogSinkFunction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSinkFunction extends FlinkKafkaProducer<RowData>
    implements LogSinkFunction<RowData> {

  private LogSinkFunction.WriteCallback writeCallback;

  /**
   * Creates a {@link KafkaSinkFunction} for a given topic. The sink produces its input to the
   * topic. It accepts a {@link KafkaSerializationSchema} for serializing records to a {@link
   * ProducerRecord}, including partitioning information.
   *
   * @param defaultTopic The default topic to write data to
   * @param serializationSchema A serializable serialization schema for turning user objects into a
   *     kafka-consumable byte[] supporting key/value messages
   * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is
   *     the only required argument.
   */
  public KafkaSinkFunction(
      String defaultTopic,
      KafkaSerializationSchema<RowData> serializationSchema,
      Properties producerConfig,
      FlinkKafkaProducer.Semantic semantic) {
    super(defaultTopic, serializationSchema, producerConfig, semantic);
  }

  @Override
  public void setWriteCallback(LogSinkFunction.WriteCallback writeCallback) {
    this.writeCallback = writeCallback;
  }

  @Override
  public void open(Configuration configuration) throws Exception {
    super.open(configuration);
    Callback baseCallback = Preconditions.checkNotNull(callback, "Base callback can not be null");
    callback =
        (metadata, exception) -> {
          if (writeCallback != null) {
            writeCallback.onCompletion(metadata.partition(), metadata.offset());
          }
          baseCallback.onCompletion(metadata, exception);
        };
  }
}
