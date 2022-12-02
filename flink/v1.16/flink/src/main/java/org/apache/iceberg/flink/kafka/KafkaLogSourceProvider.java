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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.log.LogSourceProvider;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;

public class KafkaLogSourceProvider implements LogSourceProvider {

  private static final long serialVersionUID = 1L;

  private final String topic;

  private final Properties properties;

  @Nullable private final DeserializationSchema<RowData> primaryKeyDeserializer;

  private final DeserializationSchema<RowData> valueDeserializer;

  public KafkaLogSourceProvider(
      String topic,
      Properties properties,
      DataType physicalType,
      int[] primaryKey,
      @Nullable DeserializationSchema<RowData> primaryKeyDeserializer,
      DeserializationSchema<RowData> valueDeserializer) {
    this.topic = topic;
    this.properties = properties;
    this.primaryKeyDeserializer = primaryKeyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @Override
  public KafkaSource<RowData> createSource(
      @Nullable Map<Integer, Long> offsets, TableLoader tableLoader) {

    Table table;
    try (TableLoader loader = tableLoader) {
      loader.open();
      table = tableLoader.loadTable();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return KafkaSource.<RowData>builder()
        .setTopics(topic)
        .setStartingOffsets(toOffsetsInitializer(offsets))
        .setProperties(properties)
        .setDeserializer(createDeserializationSchema(table))
        .setGroupId(UUID.randomUUID().toString())
        .build();
  }

  @VisibleForTesting
  KafkaRecordDeserializationSchema<RowData> createDeserializationSchema(Table table) {
    Schema schema = table.schema();
    List<Integer> equalityFieldIds = Lists.newArrayList(schema.identifierFieldIds());
    return KafkaRecordDeserializationSchema.of(
        new KafkaLogDeserializationSchema(
            schema, equalityFieldIds, primaryKeyDeserializer, valueDeserializer));
  }

  private OffsetsInitializer toOffsetsInitializer(@Nullable Map<Integer, Long> partitionOffsets) {
    return partitionOffsets == null
        ? OffsetsInitializer.earliest()
        : OffsetsInitializer.offsets(toKafkaOffsets(partitionOffsets));
  }

  private Map<TopicPartition, Long> toKafkaOffsets(Map<Integer, Long> partitionOffsets) {
    Map<TopicPartition, Long> offsets = Maps.newHashMap();
    partitionOffsets.forEach(
        (bucket, offset) -> offsets.put(new TopicPartition(topic, bucket), offset));
    return offsets;
  }
}
