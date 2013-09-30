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

REM Run with -notest to skip the unit tests

@echo on

@if "%~1"=="" goto usage
@if not "%4"=="" goto usage
@if "%~1"=="trunk" goto trunk
@set leveldir=uima-as-%~1-%~2
@set svnloc=tags/uima-as-%~1/%leveldir%
@goto checkargs

@:trunk
@set leveldir=trunk
@set svnloc=trunk
@goto checkargs

@:checkargs
@set jvmarg=
@set mvnCommand=clean install

@if "%~3"=="" goto execute
@if "%~3"=="-notest" goto notest
@if "%~3"=="-deploy" goto deploy
@goto usage

@:usage
@echo off
echo Run this command in the same directory where all the base uima projects live
echo Running this command in a directory produces an extract as a subdirectory of that directory;
echo   whose contents are then copied into this directory (required for the build)
echo Usage: extractAndBuild.bat level release-candidate [-notest] [-deploy]
echo            (-notest and -deploy cannot be used together)
echo  Examples of the 1st 2 arguments, level release-candidate, are  trunk trunk   or  2.2.2  01
echo  If trunk, use the word "trunk" for the 2nd argument, e.g. extractAndBuild.bat trunk trunk 
@echo on
@goto exit

@:notest
@set jvmarg="-Dmaven.test.skip=true"
@goto execute

@:deploy
@set jvmarg="-DsignArtifacts=true"
@set mvnCommand=clean deploy
@goto execute

@:execute
svn export -r HEAD http://svn.apache.org/repos/asf/incubator/uima/uima-as/%svnloc%
xcopy %leveldir%\* . /E
rmdir /S /Q %leveldir%
cd uimaj-as
call mvn %jvmarg%  %mvnCommand%
REM keep these next 2 "cd"s as two separate lines - got strange behavior when combining 2009
REM cd ..
REM cd uima-as-distr
call mvn clean install
@:exit
