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
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.log.LogSinkFunction;
import org.apache.iceberg.flink.log.LogSinkProvider;
import org.apache.iceberg.flink.log.LogStoreTableFactory;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaLogSinkProvider implements LogSinkProvider {

  private static final long serialVersionUID = 1L;

  private final String topic;

  private final Properties properties;

  @Nullable private final SerializationSchema<RowData> keySerializer;

  private final SerializationSchema<RowData> valueSerializer;

  private final LogStoreTableFactory.LogStoreConsistency consistency;

  public KafkaLogSinkProvider(
      String topic,
      Properties properties,
      @Nullable SerializationSchema<RowData> keySerializer,
      SerializationSchema<RowData> valueSerializer,
      LogStoreTableFactory.LogStoreConsistency consistency) {
    this.topic = topic;
    this.properties = properties;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.consistency = consistency;
  }

  @Override
  public LogSinkFunction createSink(
      Table table, RowType flinkRowType, List<Integer> equalityFieldIds, boolean upsert) {
    FlinkKafkaProducer.Semantic semantic;
    switch (consistency) {
      case TRANSACTIONAL:
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "log-store-" + topic);
        semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
        break;
      case EVENTUAL:
        semantic = FlinkKafkaProducer.Semantic.AT_LEAST_ONCE;
        break;
      default:
        throw new IllegalArgumentException("Unsupported: " + consistency);
    }
    return new KafkaSinkFunction(
        topic,
        createSerializationSchema(table.schema(), flinkRowType, equalityFieldIds, upsert),
        properties,
        semantic);
  }

  @VisibleForTesting
  KafkaLogSerializationSchema createSerializationSchema(
      Schema schema, RowType flinkRowType, List<Integer> equalityFieldIds, boolean upsert) {
    return new KafkaLogSerializationSchema(
        topic, keySerializer, valueSerializer, schema, flinkRowType, equalityFieldIds, upsert);
  }
}
