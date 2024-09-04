/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.session.influxdb.example;

import org.apache.iotdb.session.influxdb.IoTDBInfluxDBProtoFactory;

import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxProtoExample {

  private static InfluxDB influxDB;

  public static void main(String[] args) {
    influxDB =
        IoTDBInfluxDBProtoFactory.connect(
            "http://127.0.0.1:5667/", "root", "root", new OkHttpClient.Builder());
    influxDB.deleteDatabase("db_test");
    influxDB.createDatabase("db_test");
    influxDB.setDatabase("db_test");

    insertData();
    queryData();

    influxDB.close();
  }

  private static void insertData() {
    Point.Builder builder = Point.measurement("student");
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    tags.put("type", "type_a");
    tags.put("country", "china");
    fields.put("score", 80.0);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(1, TimeUnit.MILLISECONDS);
    Point point = builder.build();
    influxDB.write(point);

    builder = Point.measurement("student");
    tags.clear();
    fields.clear();
    tags.put("type", "type_b");
    tags.put("country", "china");
    fields.put("score", 81.0);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(2, TimeUnit.MILLISECONDS);
    point = builder.build();
    influxDB.write(point);

    builder = Point.measurement("student");
    tags.clear();
    fields.clear();
    tags.put("type", "type_a");
    tags.put("country", "usa");
    fields.put("score", 82.0);
    builder.tag(tags);
    builder.fields(fields);
    builder.time(3, TimeUnit.MILLISECONDS);
    point = builder.build();
    influxDB.write(point);
  }

  private static void queryData() {
    QueryResult queryResult = influxDB.query(new Query("select * from student", "db_test"));
    for (QueryResult.Result result : queryResult.getResults()) {
      System.out.println(result);
    }
  }
}
