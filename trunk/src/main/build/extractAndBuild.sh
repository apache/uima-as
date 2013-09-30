#!/bin/sh

#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.

# Bourne shell syntax, this should hopefully run on pretty much anything.

usage() {
  echo "Run this command in the same directory where all the base uima projects live"
  echo "Running this command in a directory produces an extract as a subdirectory of that directory;"
  echo "  whose contents are then copied into this directory (required for the build)"
  echo "Usage: extractAndBuild.sh <level> <release candidate> [-notest] [-deploy]"
  echo "           (-notest and -deploy cannot be used together)"
  echo " the 1st 2 arguments, level release-candidate: trunk trunk or 2.2.2 01"
  echo " If trunk, use the word \"trunk\" for the 2nd argument, e.g. extractAndBuild.bat trunk trunk"
}

jvmargs=""
mvnCommand="clean install"

# Check arguments
if [ $# -eq 0 -o $# -gt 3 ]
then
  usage
  exit 1
fi

if [ "$1" = "trunk" ]
then
  svnloc=trunk
  leveldir=trunk
else
  leveldir=uima-as-$1-$2
  svnloc=tags/uima-as-$1/$leveldir
fi

if [ -n "$3" ]
then
# Check for -notest switch.  If present, add the no-test define to the mvn command line.
  if [ "$3" = "-notest" ]
  then
    jvmargs="-Dmaven.test.skip=true"
# Check for -deploy switch.  If present, change maven command to deploy artifacts to remote Maven repo
  elif [ "$3" = "-deploy" ]
  then
    jvmargs="-DsignArtifacts=true"
    mvnCommand="source:jar deploy"
  else
    usage
    exit 1
  fi
fi

svn export -r HEAD http://svn.apache.org/repos/asf/incubator/uima/uima-as/$svnloc
cp -r $leveldir/* . 
cd uimaj-as
mvn ${jvmargs} $mvnCommand
# cd ../uima-as-distr
mvn clean install

