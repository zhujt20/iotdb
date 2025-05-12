@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@echo off
REM Set the path to the certificate and JAR file directory
REM Set the certificate file path
set CERT_PATH=.\public.cer
REM Set the directory path for the JAR files
set JAR_DIR=..\..\lib
set TRUSTSTORE=truststore.jks
set STOREPASS=changeit
set CERT_ALIAS=myiotdb

REM Check if the certificate file exists
if not exist "%CERT_PATH%" (
    echo Error: Certificate file %CERT_PATH% not found
    exit /b 1
)

REM Check if the JAR directory exists
if not exist "%JAR_DIR%" (
    echo Error: JAR directory %JAR_DIR% not found
    exit /b 1
)

REM === Step 1: Create truststore if not exist ===
if not exist "%TRUSTSTORE%" (
    echo Truststore not found. Creating and importing certificate...
    keytool -importcert -alias %CERT_ALIAS% -file %CERT_PATH% -keystore %TRUSTSTORE% -storepass %STOREPASS% -noprompt
) else (
    echo Truststore already exists. Skipping import...
)

REM === Step 2: Verify all matching JARs ===
for %%f in (%JAR_DIR%\iotdb*.jar) do (
    echo Verifying %%f ...
    jarsigner -verify -keystore %TRUSTSTORE% -storepass %STOREPASS% -verbose -certs "%%f"
)

REM -------- Clean up temporary keystore --------
if exist "%TRUSTSTORE%" (
    del /f /q "%TRUSTSTORE%"
)

REM All JAR files have been successfully verified
echo All JAR files verified successfully!
exit /b 0