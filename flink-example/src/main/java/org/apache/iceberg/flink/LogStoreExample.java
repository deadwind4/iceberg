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
package org.apache.iceberg.flink;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class LogStoreExample {

  private LogStoreExample() {}

  public static void main(String[] args) throws Exception {

    Configuration configuration = new Configuration();
    configuration.setString("table.exec.iceberg.use-flip27-source", "true");
    configuration.setString("execution.checkpointing.interval", "5s");
    configuration.setString("state.checkpoint-storage", "jobmanager");
    configuration.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    TableEnvironment tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE CATALOG hive_catalog WITH (\n"
            + "  'type'='iceberg',\n"
            + "  'uri'='thrift://dev-node2:9083',\n"
            + "  'warehouse'='hdfs://ns1/dtInsight/hive/warehouse'\n"
            + ")");

    tEnv.executeSql("USE CATALOG hive_catalog");

    tEnv.executeSql("USE iceberg_w");

    //    tEnv.executeSql("CREATE TABLE default_catalog.default_database.f (\n" +
    //        "  id BIGINT,\n" +
    //        "  name STRING\n" +
    //        ") WITH (\n" +
    //        "  'connector' = 'filesystem',\n" +
    //        "  'path' = 'file:///Users/ada/tmp/log-store',\n" +
    //        "  'format' = 'csv'\n" +
    //        ")");

    //    tEnv.executeSql("CREATE TABLE default_catalog.default_database.word_log (\n" +
    //        "  `id` BIGINT,\n" +
    //        "  `word` STRING\n" +
    //        ") WITH (\n" +
    //        "  'connector' = 'kafka',\n" +
    //        "  'topic' = 'word_log',\n" +
    //        "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n" +
    //        "  'scan.startup.mode' = 'earliest-offset',\n" +
    //        "  'format' = 'json'\n" +
    //        ")");

    tEnv.executeSql(
        "CREATE TABLE default_catalog.default_database.source (\n"
            + "  `id` BIGINT,\n"
            + "  `name` STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = 'iceberg.source',\n"
            + "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n"
            + "  'scan.startup.mode' = 'earliest-offset',\n"
            + "  'format' = 'json'\n"
            + ")");

    //    tEnv.executeSql("CREATE TABLE default_catalog.default_database.sink (\n" +
    //        "  `id` BIGINT,\n" +
    //        "  `name` STRING\n" +
    //        ") WITH (\n" +
    //        "  'connector' = 'kafka',\n" +
    //        "  'topic' = 'flip27_log_2',\n" +
    //        "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n" +
    //        "  'scan.startup.mode' = 'earliest-offset'," +
    //        "  'properties.isolation.level' = 'read_uncommitted',\n" +
    //        "  'format' = 'json'\n" +
    //        ")");

    //    tEnv.executeSql("CREATE TABLE default_catalog.default_database.wc (\n" +
    //        "  `word` STRING,\n" +
    //        "  `word_count` BIGINT,\n" +
    //        "  PRIMARY KEY(`word`) NOT ENFORCED\n" +
    //        ") WITH (\n" +
    //        "  'connector' = 'upsert-kafka',\n" +
    //        "  'topic' = 'iceberg_w.wc',\n" +
    //        "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n" +
    //        "  'key.format' = 'json',\n" +
    //        "  'value.format' = 'json'\n" +
    //        ")");

    //    tEnv.executeSql("CREATE TABLE default_catalog.default_database.wc (\n" +
    //        "  `word` STRING,\n" +
    //        "  `word_count` BIGINT\n" +
    //        ") WITH (\n" +
    //        "  'connector' = 'kafka',\n" +
    //        "  'topic' = 'iceberg_w.wc',\n" +
    //        "  'scan.startup.mode' = 'earliest-offset'," +
    //        "  'properties.bootstrap.servers' = '172.16.100.109:9092',\n" +
    //        "  'format' = 'json'\n" +
    //        ")");

    //    tEnv.executeSql("CREATE TABLE wc (\n" +
    //        "  word STRING,\n" +
    //        "  word_count BIGINT,\n" +
    //        "  PRIMARY KEY(`word`) NOT ENFORCED\n" +
    //        ") WITH (\n" +
    //        "  'format-version' = '2'," +
    //        "  'log-store' = 'kafka'," +
    //        "  'kafka.bootstrap.servers' = '172.16.100.109:9092'," +
    //        "  'kafka.topic' = 'iceberg_w.wc'\n" +
    //        ")");

    //        tEnv.executeSql("CREATE TABLE simple_v2 (\n" +
    //        "  word STRING,\n" +
    //        "  word_count BIGINT,\n" +
    //        "  PRIMARY KEY(`word`) NOT ENFORCED\n" +
    //        ") WITH (\n" +
    //        "  'format-version' = '2'" +
    //        ")");

    //    tEnv.executeSql("ALTER TABLE flip27_log SET ('format-version'='2')");
    //    tEnv.executeSql("ALTER TABLE flip27_log SET
    // ('kafka.bootstrap.servers'='172.16.100.109:9092')");
    //        tEnv.executeSql("ALTER TABLE flip27_log_2 SET
    // ('kafka.transaction.timeout.ms'='300000')");

    //    tEnv.executeSql("INSERT INTO log_store_v2 VALUES (3, 'bar')");
    //    tEnv.executeSql(
    //            "SELECT * FROM log_store_v2 /*+ OPTIONS('streaming'='true',
    // 'monitor-interval'='1s')*/")
    //        .print();

    //    tEnv.executeSql(
    //            "INSERT INTO default_catalog.default_database.f SELECT * FROM log_store_v2 /*+
    // OPTIONS('streaming'='true', 'monitor-interval'='1s', 'log-store'='none') */")
    //        ;

    //    tEnv.executeSql("INSERT INTO flip27_log_2 VALUES(2, 'bar')");
    //    tEnv.executeSql("SELECT word, SUM(word_count) FROM default_catalog.default_database.wc
    // GROUP BY word").print();
    //    tEnv.executeSql("SELECT * FROM flip27_log_2 /*+ OPTIONS('streaming'='true',
    // 'log-store.consistency' = 'eventual') */").print();

    //    tEnv.executeSql("SELECT * FROM flip27_log_2 /*+ OPTIONS('streaming'='true',
    // 'log-store.consistency' = 'transactional') */").print();
    //    tEnv.executeSql("INSERT INTO wc /*+ OPTIONS('streaming'='true', 'upsert-enabled'='true')
    // */ SELECT word, COUNT(word) FROM default_catalog.default_database.word_log GROUP BY
    // word").print();
    //    tEnv.executeSql("INSERT INTO flip27_log_2 /*+ OPTIONS('streaming'='true') */ SELECT * FROM
    // default_catalog.default_database.source").print();
    //    tEnv.executeSql("INSERT INTO simple_v2 VALUES('bar', 2)");
    tEnv.executeSql("SELECT * FROM simple_v2");
  }
}
