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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaLogSerializationSchema implements KafkaSerializationSchema<RowData> {

  private static final long serialVersionUID = 1L;

  private final String topic;
  @Nullable private final SerializationSchema<RowData> primaryKeySerializer;
  private final SerializationSchema<RowData> valueSerializer;
  Schema schema;
  RowType flinkSchema;
  List<Integer> equalityFieldIds;
  @Nullable private Schema deleteSchema;
  @Nullable private RowDataProjection keyProjection;
  private final boolean upsert;

  public KafkaLogSerializationSchema(
      String topic,
      @Nullable SerializationSchema<RowData> keySerializer,
      SerializationSchema<RowData> valueSerializer,
      Schema schema,
      RowType flinkSchema,
      List<Integer> equalityFieldIds,
      boolean upsert) {
    this.topic = topic;
    this.primaryKeySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.equalityFieldIds = equalityFieldIds;
    this.upsert = upsert;
  }

  @Override
  public void open(SerializationSchema.InitializationContext context) throws Exception {
    if (equalityFieldIds != null && !equalityFieldIds.isEmpty()) {
      this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
      this.keyProjection = RowDataProjection.create(schema, deleteSchema);
    }
    if (primaryKeySerializer != null) {
      primaryKeySerializer.open(context);
    }
    valueSerializer.open(context);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(RowData element, @Nullable Long timestamp) {
    RowKind kind = element.getRowKind();

    byte[] primaryKeyBytes = null;
    byte[] valueBytes = null;
    if (primaryKeySerializer != null && upsert) {
      primaryKeyBytes = primaryKeySerializer.serialize(keyProjection.wrap(element));
      // TODO accord with File writer?
      if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
        valueBytes = valueSerializer.serialize(element);
      } else if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
        primaryKeyBytes = primaryKeySerializer.serialize(keyProjection.wrap(element));
        valueBytes = null;
      }
    } else {
      valueBytes = valueSerializer.serialize(element);
    }

    return new ProducerRecord<>(topic, null, primaryKeyBytes, valueBytes);
  }

  static RowData createProjectedRow(
      RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
    final int arity = fieldGetters.length;
    final GenericRowData genericRowData = new GenericRowData(kind, arity);
    for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
      genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
    }
    return genericRowData;
  }
}
