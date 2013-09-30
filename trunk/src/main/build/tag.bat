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
REM  The form %~1 (as opposed to %1) removes surrounding quotes

@echo on

@if "%~1"=="" goto usage
@if "%~4"=="" goto usage
@if not "%5"=="" goto usage
@goto execute

@:usage
@echo off
echo Run this command in any directory to tag a uima-as release
echo This will copy the HEAD in the repository for several projects
echo This command doesn't modify any local directories or files

REM the period following echo inserts a blank line
echo.  
echo Usage: tag level release-candidate  core-uima-tag-base core-uima-tag-release-candidate
echo.
echo  example tag 2.2.2 01 2.2.2 05
@echo on
@goto exit


@:execute
@setlocal
@set asLastDir=uima-as-%~1-%~2
@set asSvnloc=tags/uima-as-%~1/%asLastDir%

@set baseURL=https://svn.apache.org/repos/asf/incubator/uima/uimaj/tags/uimaj-%3/uimaj-%3-%4
@set asURL=https://svn.apache.org/repos/asf/incubator/uima/sandbox/trunk
@set tagURL=https://svn.apache.org/repos/asf/incubator/uima/sandbox/%asSvnloc%
@set commitMsg=Create Tag for uima-as-%1-%2 release candidate based on uimaj-%3-%4

svn copy %asURL%/uima-as             %tagURL%                    -m "%commitMsg%"
svn copy %baseURL%/uimaj             %tagURL%/uimaj              -m "%commitMsg%"
svn copy %baseURL%/uimaj-distr       %tagURL%/uimaj-distr        -m "%commitMsg%"
svn copy %baseURL%/uimaj-examples    %tagURL%/uimaj-examples     -m "%commitMsg%"
svn copy %baseURL%/uima-docbooks     %tagURL%/uima-docbooks      -m "%commitMsg%"
svn copy %baseURL%/uima-docbook-tool %tagURL%/uima-docbook-tool  -m "%commitMsg%"

@:exit
