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

import java.util.Properties;

import org.apache.uima.resourceSpecifier.AsyncPrimitiveErrorConfigurationType;
import org.apache.uima.resourceSpecifier.factory.Action;
import org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrors;
import org.apache.uima.resourceSpecifier.factory.ProcessCasErrors;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class AsyncPrimitiveErrorConfigurationImpl.
 */
public class AsyncPrimitiveErrorConfigurationImpl implements AsyncPrimitiveErrorConfiguration {

  
  /** The aect. */
  private AsyncPrimitiveErrorConfigurationType aect;
  
  /** The pce. */
  private ProcessCasErrors pce;
  
  /** The cpce. */
  private CollectionProcessCompleteErrors cpce;
  
  /**
   * Instantiates a new async primitive error configuration impl.
   *
   * @param aect the aect
   * @param context the context
   * @param topLevel the top level
   */
  protected AsyncPrimitiveErrorConfigurationImpl(AsyncPrimitiveErrorConfigurationType aect, ServiceContext context, boolean topLevel) {
    this.aect = aect;
    pce = new ProcessCasErrorsImpl(aect.addNewProcessCasErrors(), context, topLevel);
    cpce = new CollectionProcessCompleteErrorsImpl(aect.addNewCollectionProcessCompleteErrors(), context, topLevel);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#getProcessCasErrors()
   */
  public ProcessCasErrors getProcessCasErrors() {
    Assert.notNull(pce);
    return pce;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#setProcessCasErrors(org.apache.uima.resourceSpecifier.factory.ProcessCasErrors)
   */
  public void setProcessCasErrors(ProcessCasErrors pce) {
    Assert.notNull(aect);
    getProcessCasErrors(); // instantiate if necessary
    this.pce = pce;
//    aect.getProcessCasErrors().setContinueOnRetryFailure(pce.continueOnRetryFailure);
//    aect.getProcessCasErrors().setMaxRetries(maxRetries)
//    aect.getProcessCasErrors().setThresholdAction(thresholdAction);
//    aect.getProcessCasErrors().setThresholdCount(thresholdCount);
//    aect.getProcessCasErrors().setThresholdWindow(thresholdWindow);
//    aect.getProcessCasErrors().setTimeout(timeout);
    
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#getTimeout()
   */
  public long getTimeout() {
    Assert.notNull(aect);
    return 0;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#setTimeout(long)
   */
  public void setTimeout(long timeout) {
    Assert.notNull(aect);
    
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#getThresholdCount()
   */
  public int getThresholdCount() {
    Assert.notNull(aect);
    return 0;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#setThresholdCount(int)
   */
  public void setThresholdCount(int threshold) {
    Assert.notNull(aect);
    
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#getContinueOnRetryFailure()
   */
  public boolean getContinueOnRetryFailure() {
    Assert.notNull(aect);
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#setContinueOnRetryFailure()
   */
  public void setContinueOnRetryFailure() {
    Assert.notNull(aect);
    
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#getThresholdWindow()
   */
  public int getThresholdWindow() {
    Assert.notNull(aect);
    return 0;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#setThresholdWindow()
   */
  public void setThresholdWindow() {
    Assert.notNull(aect);
    
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#getThresholdAction()
   */
  public Action getThresholdAction() {
    Assert.notNull(aect);
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration#setThresholdAction(org.apache.uima.resourceSpecifier.factory.Action)
   */
  public void setThresholdAction(Action pa) {
    Assert.notNull(aect);
    
  }

}
