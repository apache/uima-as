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
package org.apache.uima.resourceSpecifier.factory.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class ColocatedDelegateConfigurationImpl.
 */
public class ColocatedDelegateConfigurationImpl extends DelegateConfigurationImpl implements
        ColocatedDelegateConfiguration {
  
  /** The delegates. */
  private List<DelegateConfiguration> delegates = new ArrayList<DelegateConfiguration>();
  
  /** The number of instances. */
  private int numberOfInstances=1;
  
  /** The internal reply queue scaleout. */
  private int internalReplyQueueScaleout=1;
  
  /** The input queue scaleout. */
  private int inputQueueScaleout=1;
  
//  public ColocatedDelegateConfigurationImpl() {
//  }
  
  /**
 * Instantiates a new colocated delegate configuration impl.
 *
 * @param key the key
 * @param nestedDelegateConfiguration the nested delegate configuration
 * @param errorHandlingSettings the error handling settings
 */
public ColocatedDelegateConfigurationImpl(String key, DelegateConfiguration[] nestedDelegateConfiguration,ErrorHandlingSettings ...errorHandlingSettings) {
    super( key, errorHandlingSettings);
    super.setRemote(false);
    for( DelegateConfiguration cdc : nestedDelegateConfiguration ) {
      addDelegate(cdc);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#addDelegate(org.apache.uima.resourceSpecifier.factory.DelegateConfiguration)
   */
  public void addDelegate(DelegateConfiguration dc) {
    Assert.notNull(dc);
    delegates.add(dc);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#getDelegates()
   */
  public List<DelegateConfiguration> getDelegates() {
    return delegates;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#isAggregate()
   */
  public boolean isAggregate() {
    return delegates.size() > 0;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#setNumberOfInstances(int)
   */
  public void setNumberOfInstances(int numberOfInstances) {
    this.numberOfInstances = numberOfInstances;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#getNumberOfInstances()
   */
  public int getNumberOfInstances() {
    return numberOfInstances;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#setInputQueueScaleout(int)
   */
  public void setInputQueueScaleout(int inputQueueScaleout) {
    this.inputQueueScaleout = inputQueueScaleout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#getInputQueueScaleout()
   */
  public int getInputQueueScaleout() {
    return inputQueueScaleout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#setInternalReplyQueueScaleout(int)
   */
  public void setInternalReplyQueueScaleout(int internalReplyQueueScaleout) {
    this.internalReplyQueueScaleout = internalReplyQueueScaleout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration#getInternalReplyQueueScaleout()
   */
  public int getInternalReplyQueueScaleout() {
    return internalReplyQueueScaleout;
  }

}
