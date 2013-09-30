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
 * The Interface DelegateAnalysisEngine.
 */
public interface DelegateAnalysisEngine {
  
  /**
   * Gets the key.
   *
   * @return the key
   */
  public String getKey();
  
  /**
   * Sets the key.
   *
   * @param key the new key
   */
  public void setKey(String key);
  
  /**
   * Gets the reply queue scaleup.
   *
   * @return the reply queue scaleup
   */
  public int getReplyQueueScaleup();
  
  /**
   * Sets the reply queue scaleup.
   *
   * @param scaleup the new reply queue scaleup
   */
  public void setReplyQueueScaleup(int scaleup);
  
  /**
   * Gets the cas multiplier.
   *
   * @return the cas multiplier
   */
  public CasMultiplier getCasMultiplier();
  
  /**
   * Sets the cas multiplier.
   *
   * @param cm the new cas multiplier
   */
  public void setCasMultiplier(CasMultiplier cm);
  
  /**
   * Gets the configuration.
   *
   * @return the configuration
   */
  public DelegateConfiguration getConfiguration();
  
  /**
   * Checks if is remote.
   *
   * @return true, if is remote
   */
  public boolean isRemote();
  
  /**
   * Checks if is aggregate.
   *
   * @return true, if is aggregate
   */
  public boolean isAggregate();
  
}
