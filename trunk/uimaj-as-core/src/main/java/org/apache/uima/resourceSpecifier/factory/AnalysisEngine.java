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
 * The Interface AnalysisEngine.
 */
public interface AnalysisEngine {
  
  /**
   * Gets the input queue scaleout.
   *
   * @return the input queue scaleout
   */
  public Scaleout getInputQueueScaleout();
  
  /**
   * Sets the input queue scaleout.
   *
   * @param scaleout the new input queue scaleout
   */
  public void setInputQueueScaleout(Scaleout scaleout);
  
  /**
   * Gets the reply queue scaleout.
   *
   * @return the reply queue scaleout
   */
  public Scaleout getReplyQueueScaleout();
  
  /**
   * Sets the reply queue scaleout.
   *
   * @param scaleout the new reply queue scaleout
   */
  public void setReplyQueueScaleout(Scaleout scaleout);

  /**
   * Gets the delegates.
   *
   * @return the delegates
   */
  public Delegates getDelegates();
  
  /**
   * Sets the delegates.
   *
   * @param delegates the new delegates
   */
  public void setDelegates(Delegates delegates);
  
  /**
   * Gets the cas multiplier.
   *
   * @return the cas multiplier
   */
  public CasMultiplier getCasMultiplier();

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
   * Checks if is async.
   *
   * @return true, if is async
   */
  public boolean isAsync();
  
  /**
   * Sets the is async.
   */
  public void setIsAsync();

}
