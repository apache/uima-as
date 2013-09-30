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
 * The Interface ProcessCasErrors.
 */
public interface ProcessCasErrors extends ErrorHandlingSettings {

  /**
   * Gets the threshold count.
   *
   * @return the threshold count
   */
  public int getThresholdCount();
  
  /**
   * Sets the threshold count.
   *
   * @param threshold the new threshold count
   */
  public void setThresholdCount(int threshold);
  
  /**
   * Gets the threshold window.
   *
   * @return the threshold window
   */
  public int getThresholdWindow();
  
  /**
   * Sets the threshold window.
   *
   * @param window the new threshold window
   */
  public void setThresholdWindow(int window);
  
  /**
   * Gets the threshold action.
   *
   * @return the threshold action
   */
  public Action getThresholdAction();
  
  /**
   * Sets the threshold action.
   *
   * @param action the new threshold action
   */
  public void setThresholdAction(Action action);
}
