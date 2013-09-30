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

package org.apache.uima.aae.jmx;

import java.util.ArrayList;
import java.util.List;

public class ServiceErrors implements ServiceErrorsMBean {
  private static final long serialVersionUID = 1L;

  private static final String label = "Service Errors";

  private static final int MAX_ERROR_COUNT = 5;

  private long processErrors = 0;

  private long metadataErrrors = 0;

  private long cpcErrors = 0;

  private String[] errors = new String[] { " ", " ", " ", " ", " " };

  private List throwables = new ArrayList();

  public String getLabel() {
    return label;
  }

  public synchronized void addError(Throwable aThrowable) {

  }

  public String[] getErrors() {
    return errors;
  }

  public synchronized long getProcessErrors() {
    return processErrors;
  }

  public synchronized void incrementProcessErrors() {
    processErrors++;
  }

  public synchronized long getMetadataErrors() {
    return metadataErrrors;
  }

  public synchronized void incrementMetadataErrors() {
    metadataErrrors++;
  }

  public synchronized long getCpcErrors() {
    return cpcErrors;
  }

  public synchronized void incrementCpcErrors() {
    cpcErrors++;
  }

  public synchronized void reset() {
    cpcErrors = 0;
    metadataErrrors = 0;
    processErrors = 0;
  }

}
