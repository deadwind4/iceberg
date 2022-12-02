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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.List;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class TestFlinkLogStoreSourceSql extends TestSqlBase {

  @Override
  public void before() throws IOException {
    SqlHelpers.sql(
        getTableEnv(),
        "CREATE CATALOG iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        catalogResource.warehouse());
    SqlHelpers.sql(getTableEnv(), "use catalog iceberg_catalog");
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  public static final Schema SCHEMA =
      new Schema(
          required(1, "data", Types.StringType.get()),
          required(2, "id", Types.LongType.get()),
          required(3, "dt", Types.StringType.get()));

  public static final String TABLE = "log_store";

  public static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DATABASE, TABLE);

  @Test
  public void testR() throws Exception {
    Table table =
        catalogResource
            .catalog()
            .createTable(
                TABLE_IDENTIFIER, TestFixtures.SCHEMA, TestFixtures.SPEC, null, Maps.newHashMap());

    List<Record> writeRecords = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, TEMPORARY_FOLDER);

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.add(writeRecords.get(0));

    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    DataFile dataFile2 =
        helper.writeFile(
            TestHelpers.Row.of("2020-03-21", 0),
            RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    org.apache.iceberg.flink.TestHelpers.assertRecords(
        run(Maps.newHashMap(), "where dt='2020-03-20' and id=123", "*"),
        expectedRecords,
        TestFixtures.SCHEMA);
  }
}
