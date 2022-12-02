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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaLogDeserializationSchema implements KafkaDeserializationSchema<RowData> {

  private static final long serialVersionUID = 1L;
  private final Schema schema;
  private final List<Integer> equalityFieldIds;
  @Nullable private final DeserializationSchema<RowData> primaryKeyDeserializer;
  private final DeserializationSchema<RowData> valueDeserializer;
  @Nullable private RowDataWrapper keyWrapper;

  public KafkaLogDeserializationSchema(
      Schema schema,
      List<Integer> equalityFieldIds,
      @Nullable DeserializationSchema<RowData> primaryKeyDeserializer,
      DeserializationSchema<RowData> valueDeserializer) {
    this.schema = schema;
    this.equalityFieldIds = equalityFieldIds;
    this.primaryKeyDeserializer = primaryKeyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @Override
  public void open(DeserializationSchema.InitializationContext context) throws Exception {
    if (equalityFieldIds != null && !equalityFieldIds.isEmpty()) {
      Schema deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
      this.keyWrapper =
          new RowDataWrapper(FlinkSchemaUtil.convert(deleteSchema), deleteSchema.asStruct());
    }
    if (primaryKeyDeserializer != null) {
      primaryKeyDeserializer.open(context);
    }
    valueDeserializer.open(context);
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  @Override
  public RowData deserialize(ConsumerRecord<byte[], byte[]> record) {
    throw new RuntimeException(
        "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> underCollector)
      throws Exception {
    if (equalityFieldIds.size() > 0 && record.value() == null) {
      RowData key = primaryKeyDeserializer.deserialize(record.key());
      keyWrapper.wrap(key);
      GenericRowData result = new GenericRowData(RowKind.DELETE, schema.columns().size());
      for (int i = 0; i < equalityFieldIds.size(); i++) {
        result.setField(equalityFieldIds.get(i), keyWrapper.get(i, Object.class));
      }
      underCollector.collect(result);
    } else {
      valueDeserializer.deserialize(record.value(), underCollector);
    }
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return null;
  }
}
