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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class LogStoreOffsetsUtils {

  private LogStoreOffsetsUtils() {}

  public static String offsetsToString(Map<Integer, Long> offsets) {
    List<String> lists = Lists.newArrayList();
    offsets.forEach((k, v) -> lists.add(k + ":" + v));
    return String.join(",", lists);
  }

  public static Map<Integer, Long> stringToOffsets(String str) {
    if (str == null || str.length() == 0) {
      return Maps.newHashMap();
    }
    return Arrays.stream(str.split(","))
        .map(s -> s.split(":"))
        .collect(Collectors.toMap(s -> Integer.parseInt(s[0]), s -> Long.parseLong(s[1])));
  }
}
