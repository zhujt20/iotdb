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

namespace java org.apache.iotdb.db.mpp.transformation.dag.udf.protocol.rpc
namespace py iotdb.udf.protocol.rpc

service UDFInvocationService {

  BeforeStartResponse beforeStart(1:BeforeStartRequest request);

  TransformResponse transformRows(1:TransformRowsRequest request);

  TransformResponse transformWindows(1:TransformWindowsRequest request);
}

struct BeforeStartRequest {
    1: required list<string> childExpressions
    2: required list<string> childExpressionDataTypes
    3: required map<string, string> attributes
}

struct BeforeStartResponse {
    1: required string accessStrategyType
    2: required map<string, string> accessStrategyAttributes
    3: required string outputDataType
}

struct TransformResponse {
    1: required binary timeColumn
    2: required binary valueColumn
}

struct TransformRowsRequest {
    1: required binary timeColumn
    2: required list<binary> valueColumns
    3: required list<binary> bitmaps
    4: required bool isLastRequest
}

struct TransformWindowsRequest {
    1: required binary timeColumn
    2: required list<binary> valueColumns
    3: required list<binary> bitmaps
    4: required list<WindowMeta> windows
    5: required bool isLastRequest
}

struct WindowMeta {
    1: required i32 beginIndex
    2: required i32 endIndex
    3: required i32 beginTime
    2: required i32 endTime
}
