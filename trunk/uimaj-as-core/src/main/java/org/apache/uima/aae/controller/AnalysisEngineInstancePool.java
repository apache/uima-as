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

package org.apache.uima.aae.controller;

import java.util.List;

import org.apache.uima.analysis_engine.AnalysisEngine;

public interface AnalysisEngineInstancePool {
  /**
   * Adds an instance of AnalysisEngine to the pool
   * 
   * @param anAnalysisEngine
   *          - AnalysisEngine instance to be added to the pool
   * @throws Exception
   */
  public void checkin(AnalysisEngine anAnalysisEngine) throws Exception;

  /**
   * Borrows an instance of AnalysisEngine from the pool
   * 
   * @return AnalysisEngine instance
   * @throws Exception
   */
  public AnalysisEngine checkout() throws Exception;

  /**
   * Destroys Analysis Engine instance pool.
   * 
   * @throws Exception
   */
  public void destroy() throws Exception;

  /**
   * Checks if the current Thread is assigned to an AE instance
   * 
   * @return
   */
  public boolean exists();

  public int size();
}
