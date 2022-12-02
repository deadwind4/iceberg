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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.iceberg.flink.log.LogStoreTableFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaLogStoreFactory implements LogStoreTableFactory {

  public static final String IDENTIFIER = "kafka";

  public static final String KAFKA_PREFIX = IDENTIFIER + ".";

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = Sets.newHashSet();
    options.add(KafkaLogOptions.BOOTSTRAP_SERVERS);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Sets.newHashSet();
  }

  private String topic(DynamicTableFactory.Context context) {
    return context.getCatalogTable().getOptions().get(KafkaLogOptions.TOPIC.key());
  }

  @Override
  public KafkaLogSourceProvider createSourceProvider(
      DynamicTableFactory.Context context, DynamicTableSource.Context sourceContext) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    DataType physicalType = schema.toPhysicalRowDataType();

    DeserializationSchema<RowData> primaryKeyDeserializer = null;
    int[] primaryKey = getPrimaryKeyIndexes(schema);
    if (primaryKey.length > 0) {
      DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
      primaryKeyDeserializer =
          LogStoreTableFactory.getKeyDecodingFormat(helper)
              .createRuntimeDecoder(sourceContext, keyType);
    }
    DeserializationSchema<RowData> valueDeserializer =
        LogStoreTableFactory.getValueDecodingFormat(helper)
            .createRuntimeDecoder(sourceContext, physicalType);

    return new KafkaLogSourceProvider(
        topic(context),
        toKafkaProperties(helper.getOptions()),
        physicalType,
        primaryKey,
        primaryKeyDeserializer,
        valueDeserializer);
  }

  @Override
  public KafkaLogSinkProvider createSinkProvider(
      DynamicTableFactory.Context context, DynamicTableSink.Context sinkContext) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    DataType physicalType = schema.toPhysicalRowDataType();

    SerializationSchema<RowData> keySerializer = null;
    int[] primaryKey = getPrimaryKeyIndexes(schema);
    if (primaryKey.length > 0) {
      DataType keyType = DataTypeUtils.projectRow(physicalType, primaryKey);
      keySerializer =
          LogStoreTableFactory.getKeyEncodingFormat(helper)
              .createRuntimeEncoder(sinkContext, keyType);
    }
    SerializationSchema<RowData> valueSerializer =
        LogStoreTableFactory.getValueEncodingFormat(helper)
            .createRuntimeEncoder(sinkContext, physicalType);

    return new KafkaLogSinkProvider(
        topic(context),
        toKafkaProperties(helper.getOptions()),
        keySerializer,
        valueSerializer,
        helper.getOptions().get(LOG_STORE_CONSISTENCY));
  }

  private int[] getPrimaryKeyIndexes(ResolvedSchema schema) {
    final List<String> columns = schema.getColumnNames();
    return schema
        .getPrimaryKey()
        .map(UniqueConstraint::getColumns)
        .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
        .orElseGet(() -> new int[] {});
  }

  public static Properties toKafkaProperties(ReadableConfig options) {
    Properties properties = new Properties();
    Map<String, String> optionMap = ((Configuration) options).toMap();
    optionMap.keySet().stream()
        .filter(key -> key.startsWith(KAFKA_PREFIX))
        .forEach(key -> properties.put(key.substring(KAFKA_PREFIX.length()), optionMap.get(key)));

    // Add read committed for transactional consistency mode.
    if (options.get(LOG_STORE_CONSISTENCY) == LogStoreConsistency.TRANSACTIONAL) {
      properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    }
    return properties;
  }
}
