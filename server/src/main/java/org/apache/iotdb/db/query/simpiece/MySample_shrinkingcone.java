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

// Sim-Piece code forked from https://github.com/xkitsios/Sim-Piece.git

package org.apache.iotdb.db.query.simpiece;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

public class MySample_shrinkingcone {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    boolean series = true; // 从1开始编号列而不是时间戳列
    String[] datasetNameList =
        new String[] {
          //            "NISTPV-Ground-2015-Qloss_Ah",
          "NISTPV-Ground-2015-Pyra1_Wm2",
          //          "NISTPV-Ground-2015-RTD_C_3"
        };

    int[] noutList = new int[] {0};

    double[] r = new double[] {0.1, 0.5, 1.3};
    for (int y = 0; y < datasetNameList.length; y++) {
      String datasetName = datasetNameList[y];
      //      int start = (int) (10000000 / 2 - 2500000 * r[y]); // 从0开始计数
      //      int end = (int) (10000000 / 2 + 2500000 * (1 - r[y]));
      //      int N = end - start; // -1 for all

      int start = 0;
      int end = 100000;
      int N = end - start;

      for (int nout : noutList) {
        // apply Sim-Piece on the input file, outputting nout points saved in csvFile
        boolean hasHeader = false;
        try (FileInputStream inputStream = new FileInputStream(fileDir + datasetName + ".csv")) {
          String delimiter = ",";
          TimeSeries ts =
              TimeSeriesReader.getMyTimeSeries(
                  inputStream, delimiter, false, N, start, hasHeader, series);

          double epsilon = 10;
          List<Point> reducedPoints = ShrinkingCone.reducePoints(ts.data, epsilon);
          try (PrintWriter writer =
              new PrintWriter(
                  new FileWriter(datasetName + "-" + N + "-" + reducedPoints.size() + "-sc.csv"))) {
            for (Point p : reducedPoints) {
              writer.println(p.getTimestamp() + "," + p.getValue());
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
