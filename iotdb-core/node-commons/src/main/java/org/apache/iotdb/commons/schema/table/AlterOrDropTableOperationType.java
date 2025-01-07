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

package org.apache.iotdb.commons.schema.table;

public enum AlterOrDropTableOperationType {
  ADD_COLUMN((byte) 0),
  SET_PROPERTIES((byte) 1),
  RENAME_COLUMN((byte) 2),
  DROP_COLUMN((byte) 3),
  RENAME_TABLE((byte) 4),
  DROP_TABLE((byte) 5),
  ALTER_COLUMN_DATA_TYPE((byte) 6),
  ;

  private final byte type;

  AlterOrDropTableOperationType(final byte type) {
    this.type = type;
  }

  public byte getTypeValue() {
    return type;
  }

  public static AlterOrDropTableOperationType getType(final byte value) {
    switch (value) {
      case 0:
        return ADD_COLUMN;
      case 1:
        return SET_PROPERTIES;
      case 2:
        return RENAME_COLUMN;
      case 3:
        return DROP_COLUMN;
      case 4:
        return RENAME_TABLE;
      case 5:
        return DROP_TABLE;
      case 6:
        return ALTER_COLUMN_DATA_TYPE;
      default:
        throw new IllegalArgumentException();
    }
  }
}
