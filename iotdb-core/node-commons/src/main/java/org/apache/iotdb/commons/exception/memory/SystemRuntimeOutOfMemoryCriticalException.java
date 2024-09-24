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

package org.apache.iotdb.commons.exception.memory;

import java.util.Objects;

public class SystemRuntimeOutOfMemoryCriticalException extends RuntimeException {
  private final long timeStamp;

  public SystemRuntimeOutOfMemoryCriticalException(String message) {
    super(message);
    this.timeStamp = System.currentTimeMillis();
  }

  public SystemRuntimeOutOfMemoryCriticalException(String message, long timeStamp) {
    super(message);
    this.timeStamp = timeStamp;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof SystemRuntimeOutOfMemoryCriticalException
        && Objects.equals(
            getMessage(), ((SystemRuntimeOutOfMemoryCriticalException) obj).getMessage())
        && Objects.equals(
            getTimeStamp(), ((SystemRuntimeOutOfMemoryCriticalException) obj).getTimeStamp());
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  @Override
  public String toString() {
    return "SystemRuntimeOutOfMemoryException{"
        + "message='"
        + getMessage()
        + "', timeStamp="
        + getTimeStamp()
        + "}";
  }
}
