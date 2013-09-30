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
 * The Interface ErrorHandlingSettings.
 */
public interface ErrorHandlingSettings {
  
  /**
   * Sets the max retries.
   *
   * @param retryCount the new max retries
   */
  public void setMaxRetries(int retryCount);
  
  /**
   * Gets the max retries.
   *
   * @return the max retries
   */
  public int getMaxRetries();
  
  /**
   * Sets the timeout.
   *
   * @param timeout the new timeout
   */
  public void setTimeout(int timeout);
  
  /**
   * Gets the timeout.
   *
   * @return the timeout
   */
  public int getTimeout();
  
  /**
   * Sets the action.
   *
   * @param action the new action
   */
  public void setAction(Action action);
  
  /**
   * Gets the action.
   *
   * @return the action
   */
  public Action getAction();
}
