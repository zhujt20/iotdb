#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


# set path
KEYSTORE=temp-keystore.jks
CERT=public.cer
ALIAS=myiotdb
STOREPASS=temp123
LIB_DIR="../lib"

# -------- Check Certificate --------
if [ ! -f "$CERT" ]; then
  echo "❌ Certificate file '$CERT' not found!"
  exit 1
fi

# -------- Create Temp Keystore --------
rm -f "$KEYSTORE"
keytool -importcert -keystore "$KEYSTORE" -storepass "$STOREPASS" -alias "$ALIAS" -file "$CERT" -noprompt

# -------- Verify Matching JARs --------
for jar in "$LIB_DIR"/iotdb*.jar; do
  if [ -f "$jar" ]; then
    echo "🔍 Verifying $jar ..."
    jarsigner -verify -keystore "$KEYSTORE" -storepass "$STOREPASS" "$jar"
  fi
done

# -------- Cleanup --------
rm -f "$KEYSTORE"
echo "✅ Verification complete."