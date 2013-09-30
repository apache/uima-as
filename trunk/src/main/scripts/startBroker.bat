@echo off

REM   Licensed to the Apache Software Foundation (ASF) under one
REM   or more contributor license agreements.  See the NOTICE file
REM   distributed with this work for additional information
REM   regarding copyright ownership.  The ASF licenses this file
REM   to you under the Apache License, Version 2.0 (the
REM   "License"); you may not use this file except in compliance
REM   with the License.  You may obtain a copy of the License at
REM
REM    http://www.apache.org/licenses/LICENSE-2.0
REM
REM   Unless required by applicable law or agreed to in writing,
REM   software distributed under the License is distributed on an
REM   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
REM   KIND, either express or implied.  See the License for the
REM   specific language governing permissions and limitations
REM   under the License.

@if not defined UIMA_HOME goto USAGE_UIMA
@goto RUN

:USAGE_UIMA
@echo UIMA_HOME environment variable is not set 
@goto EXIT

:RUN

@echo on

@REM  ActiveMQ needs a HOME
@setlocal
@if "%ACTIVEMQ_HOME%" == "" (
  set "ACTIVEMQ_HOME=%UIMA_HOME%\apache-activemq"
)

@REM  ActiveMQ needs a writable directory for the log files and derbydb
@REM  watchout! it appears that ACTIVEMQ_BASE cannot contain backslashes!
@if "%ACTIVEMQ_BASE%" == "" (
  set ACTIVEMQ_BASE=amq
)

@REM If directory missing create it
@if not exist "%ACTIVEMQ_BASE%" (
  mkdir "%ACTIVEMQ_BASE%"
  mkdir "%ACTIVEMQ_BASE%\conf"
)

@REM If config file not there, copy it
@if not exist "%ACTIVEMQ_BASE%\conf\activemq-nojournal.xml" (
  copy "%UIMA_HOME%\as_config\log4j.properties" "%ACTIVEMQ_BASE%\conf"
  copy "%UIMA_HOME%\as_config\activemq-nojournal.xml" "%ACTIVEMQ_BASE%\conf"
  copy "%ACTIVEMQ_HOME%\conf\*" "%ACTIVEMQ_BASE%\conf"
)

call "%ACTIVEMQ_HOME%\bin\activemq.bat" "xbean:file:%ACTIVEMQ_BASE%/conf/activemq-nojournal.xml"
:EXIT
