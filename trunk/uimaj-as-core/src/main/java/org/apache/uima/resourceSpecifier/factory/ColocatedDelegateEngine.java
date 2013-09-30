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
package org.apache.uima.resourceSpecifier.factory;

// TODO: Auto-generated Javadoc
/**
 * The Interface ColocatedDelegateEngine.
 */
public interface ColocatedDelegateEngine extends DelegateAnalysisEngine {
  
  /**
   * Checks if is async.
   *
   * @return true, if is async
   */
  public boolean isAsync();
  
  /**
   * Sets the async.
   */
  public void setAsync();
  
  /**
   * Gets the input queue scaleout.
   *
   * @return the input queue scaleout
   */
  public int getInputQueueScaleout();
  
  /**
   * Sets the input queue scaleout.
   *
   * @param scaleup the new input queue scaleout
   */
  public void setInputQueueScaleout(int scaleup);
  
}
