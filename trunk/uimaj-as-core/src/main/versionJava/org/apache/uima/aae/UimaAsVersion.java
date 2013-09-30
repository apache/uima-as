/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.aae;

/**
 * This class is used to define current UIMA AS version
 * 
 */
public class UimaAsVersion {

  /**
   * Gets the major version number of the UIMA AS implementation.
   * 
   * @return the major version number
   */
  public static short getMajorVersion() {
    return ${parsedVersion.majorVersion}; // major version
  }

  /**
   * Gets the minor version number of the UIMA AS implementation.
   * 
   * @return the minor version number
   */
  public static short getMinorVersion() {
    return ${parsedVersion.minorVersion}; // minor version
  }

  /**
   * Gets the build revision number of the UIMA AS implementation.
   * 
   * @return the build revision number
   */
  public static short getBuildRevision() {
    return ${parsedVersion.incrementalVersion}; // build revision
  }

  public static String getVersionString() {
    return "" + getMajorVersion() + "." + getMinorVersion() + "." + getBuildRevision();
  }
  
  /**
   * @return the build version, including any suffixes, as a String
   */
  public static String getFullVersionString() {
    return "${project.version}";
  }
  
  /**
   * @return the build version of uimaj that this build depends on, as a String
   */
  public static String getUimajFullVersionString() {
    return "${uimajDependencyVersion}"; // e.g. 2.3.1-SNAPSHOT
  }
}
