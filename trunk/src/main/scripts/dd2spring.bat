@echo off
@REM   Licensed to the Apache Software Foundation (ASF) under one
@REM   or more contributor license agreements.  See the NOTICE file
@REM   distributed with this work for additional information
@REM   regarding copyright ownership.  The ASF licenses this file
@REM   to you under the Apache License, Version 2.0 (the
@REM   "License"); you may not use this file except in compliance
@REM   with the License.  You may obtain a copy of the License at
@REM
@REM    http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM   Unless required by applicable law or agreed to in writing,
@REM   software distributed under the License is distributed on an
@REM   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM   KIND, either express or implied.  See the License for the
@REM   specific language governing permissions and limitations
@REM   under the License.

@setlocal

if not defined UIMA_HOME goto USAGE_UIMA
if "%1" == "" goto usage
goto RUN

:USAGE_UIMA
echo UIMA_HOME environment variable is not set 
goto EXIT

:usage
echo USAGE: dd2spring source-deployment-descriptor output-file-path
echo   for instance:  %%UIMA_HOME%%\bin\dd2spring %%UIMA_HOME%%\examples\deploy\as\Deploy_MeetingDetectorTAE.xml c:\temp\dd2so.xml
goto EXIT

:RUN
set UIMA_CLASSPATH=%UIMA_CLASSPATH%;%UIMA_HOME%\saxon\saxon8.jar
set UIMA_JVM_OPTS=%UIMA_JVM_OPTS% -Xmx256M 
@call "%UIMA_HOME%\bin\runUimaClass.bat" net.sf.saxon.Transform -l -s "%1" -o "%2" "%UIMA_HOME%\bin\dd2spring.xsl" %3 %4 %5 %6 %7 %8 %9

:EXIT