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

import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Interface ColocatedDelegateConfiguration.
 */
public interface ColocatedDelegateConfiguration extends DelegateConfiguration {
  
  /**
   * Adds the delegate.
   *
   * @param dc the dc
   */
  public void addDelegate(DelegateConfiguration dc);
  
  /**
   * Gets the delegates.
   *
   * @return the delegates
   */
  public List<DelegateConfiguration> getDelegates();
  
  /**
   * Checks if is aggregate.
   *
   * @return true, if is aggregate
   */
  public boolean isAggregate();
  
  /**
   * Sets the number of instances.
   *
   * @param numberOfInstances the new number of instances
   */
  public void setNumberOfInstances(int numberOfInstances);
  
  /**
   * Gets the number of instances.
   *
   * @return the number of instances
   */
  public int getNumberOfInstances();
  
  /**
   * Sets the input queue scaleout.
   *
   * @param inputQueueScaleout the new input queue scaleout
   */
  public void setInputQueueScaleout(int inputQueueScaleout);
  
  /**
   * Gets the input queue scaleout.
   *
   * @return the input queue scaleout
   */
  public int getInputQueueScaleout();
  
  /**
   * Sets the internal reply queue scaleout.
   *
   * @param internalReplyQueueScaleout the new internal reply queue scaleout
   */
  public void setInternalReplyQueueScaleout(int internalReplyQueueScaleout);
  
  /**
   * Gets the internal reply queue scaleout.
   *
   * @return the internal reply queue scaleout
   */
  public int getInternalReplyQueueScaleout();
}
