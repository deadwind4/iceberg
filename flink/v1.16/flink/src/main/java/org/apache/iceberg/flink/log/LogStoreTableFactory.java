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
package org.apache.iceberg.flink.log;

import static org.apache.flink.configuration.description.TextElement.text;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

/**
 * Base interface for configuring a default log table connector. The log table is used by managed
 * table factory.
 *
 * <p>Log tables are for processing only unbounded data. Support streaming reading and streaming
 * writing.
 */
public interface LogStoreTableFactory extends DynamicTableFactory {

  String LOG_STORE_OFFSETS = "log-store.offsets";

  ConfigOption<LogStoreConsistency> LOG_STORE_CONSISTENCY =
      ConfigOptions.key("log-store.consistency")
          .enumType(LogStoreConsistency.class)
          .defaultValue(LogStoreConsistency.EVENTUAL)
          .withDescription("Specify the log consistency mode for table.");

  ConfigOption<String> LOG_STORE_KEY_FORMAT =
      ConfigOptions.key("log-store.key.format")
          .stringType()
          .defaultValue("json")
          .withDescription("Specify the key message format of log system with primary key.");

  ConfigOption<String> LOG_STORE_FORMAT =
      ConfigOptions.key("log-store.format")
          .stringType()
          .defaultValue("json")
          .withDescription("Specify the message format of log system.");

  /**
   * Creates a {@link LogSourceProvider} instance from a {@link CatalogTable} and additional context
   * information.
   */
  LogSourceProvider createSourceProvider(
      DynamicTableFactory.Context context, DynamicTableSource.Context sourceContext);

  /**
   * Creates a {@link LogSinkProvider} instance from a {@link CatalogTable} and additional context
   * information.
   */
  LogSinkProvider createSinkProvider(
      DynamicTableFactory.Context context, DynamicTableSink.Context sinkContext);

  // --------------------------------------------------------------------------------------------

  static DecodingFormat<DeserializationSchema<RowData>> getKeyDecodingFormat(
      FactoryUtil.TableFactoryHelper helper) {
    DecodingFormat<DeserializationSchema<RowData>> format =
        helper.discoverDecodingFormat(DeserializationFormatFactory.class, LOG_STORE_KEY_FORMAT);
    return format;
  }

  static EncodingFormat<SerializationSchema<RowData>> getKeyEncodingFormat(
      FactoryUtil.TableFactoryHelper helper) {
    EncodingFormat<SerializationSchema<RowData>> format =
        helper.discoverEncodingFormat(SerializationFormatFactory.class, LOG_STORE_KEY_FORMAT);
    return format;
  }

  static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
      FactoryUtil.TableFactoryHelper helper) {
    DecodingFormat<DeserializationSchema<RowData>> format =
        helper.discoverDecodingFormat(DeserializationFormatFactory.class, LOG_STORE_FORMAT);
    return format;
  }

  static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
      FactoryUtil.TableFactoryHelper helper) {
    EncodingFormat<SerializationSchema<RowData>> format =
        helper.discoverEncodingFormat(SerializationFormatFactory.class, LOG_STORE_FORMAT);
    return format;
  }

  /** Specifies the log consistency mode for table. */
  enum LogStoreConsistency implements DescribedEnum {
    TRANSACTIONAL(
        "transactional",
        "Only the data after the checkpoint can be seen by readers, the latency depends on checkpoint interval."),

    EVENTUAL(
        "eventual",
        "Immediate data visibility, you may see some intermediate states, "
            + "but eventually the right results will be produced, only works for table with primary key.");

    private final String value;
    private final String description;

    LogStoreConsistency(String value, String description) {
      this.value = value;
      this.description = description;
    }

    @Override
    public String toString() {
      return value;
    }

    @Override
    public InlineElement getDescription() {
      return text(description);
    }
  }
}
