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

package org.apache.iotdb.db.mpp.transformation.dag.udf.python;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;

public class PythonUDTF implements UDTF {
  @Override
  public void validate(UDFParameterValidator validator) throws Exception {
    UDTF.super.validate(validator);
  }

  @Override
  public void beforeDestroy() {
    UDTF.super.beforeDestroy();
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws Exception {}

  @Override
  public void transform(Row row, PointCollector collector) throws Exception {
    UDTF.super.transform(row, collector);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    UDTF.super.transform(rowWindow, collector);
  }

  @Override
  public Object transform(Row row) throws Exception {
    return UDTF.super.transform(row);
  }

  @Override
  public void terminate(PointCollector collector) throws Exception {
    UDTF.super.terminate(collector);
  }
}
