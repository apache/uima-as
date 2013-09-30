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
package org.apache.uima.aae.client;

import java.util.List;

import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;

/**
 * Contains information about the processing of a CAS by a UIMA-AS Analysis Engine.
 * 
 */
public interface UimaASProcessStatus extends EntityProcessStatus {

  /**
   * Gets the unique ID of the returned CAS
   * 
   * @return the CAS ID
   */
  public String getCasReferenceId();

  /**
   * If the Analysis Engine has returned a new CAS this will get the unique ID 
   * of the input CAS that caused its creation.
   * Will be null if the returned CAS is not new.
   * 
   * @return the parent CAS ID or null if an input CAS has been returned
   */
  public String getParentCasReferenceId();
  
  /**
   * Gets the returned CAS
   * 
   * @return the CAS
   */
  public CAS getCAS();
  
  /**
   * Gets a list of performance metrics containing, for each component in the Analysis Engine,
   * the performance breakdown reported by the AE
   * 
   * @return a list of {@link AnalysisEnginePerformanceMetrics}
   */
  public List<AnalysisEnginePerformanceMetrics> getPerformanceMetricsList();
}
