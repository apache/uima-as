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


import org.apache.uima.resourceSpecifier.ProcessCasErrorsType;
import org.apache.uima.resourceSpecifier.factory.AggregateProcessCasErrors;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class AggregateProcessCasErrorsImpl.
 */
public class AggregateProcessCasErrorsImpl extends ProcessCasErrorsImpl 
implements AggregateProcessCasErrors {
  
  /**
   * Instantiates a new aggregate process cas errors impl.
   *
   * @param pcet the pcet
   * @param context the context
   * @param topLevel the top level
   */
  protected AggregateProcessCasErrorsImpl(ProcessCasErrorsType pcet,ServiceContext context, boolean topLevel) {
    super(pcet,context,topLevel);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateProcessCasErrors#getMaxRetries()
   */
  public int getMaxRetries(){
    Assert.notNull(super.pcet);
    return pcet.getMaxRetries();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateProcessCasErrors#setMaxRetris(int)
   */
  public void setMaxRetris(int retries) {
    Assert.notNull(super.pcet);
    pcet.setMaxRetries(retries);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateProcessCasErrors#getTimeout()
   */
  public int getTimeout() {
    Assert.notNull(super.pcet);
    return pcet.getTimeout();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateProcessCasErrors#setTimeout(int)
   */
  public void setTimeout(int timeout) {
    Assert.notNull(super.pcet);
    pcet.setTimeout(timeout);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateProcessCasErrors#continueOnRetryFailure()
   */
  public boolean continueOnRetryFailure() {
    Assert.notNull(super.pcet);
    return Boolean.parseBoolean(pcet.getContinueOnRetryFailure());
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateProcessCasErrors#setContinueOnRetryFailure()
   */
  public void setContinueOnRetryFailure() {
    Assert.notNull(super.pcet);
    pcet.setContinueOnRetryFailure("true");
  }
}
