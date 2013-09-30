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
import org.apache.uima.resourceSpecifier.factory.Action;
import org.apache.uima.resourceSpecifier.factory.ProcessCasErrors;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class ProcessCasErrorsImpl.
 */
public class ProcessCasErrorsImpl extends ErrorHandlingSettingsImpl 
implements ProcessCasErrors {
  
  /** The pcet. */
  protected ProcessCasErrorsType pcet;
  
  /**
   * Instantiates a new process cas errors impl.
   *
   * @param pcet the pcet
   * @param context the context
   * @param topLevel the top level
   */
  protected ProcessCasErrorsImpl(ProcessCasErrorsType pcet, ServiceContext context, boolean topLevel ) {
    this.pcet = pcet;
    setThresholdCount(context.getProcessErrorThresholdCount());
    setThresholdWindow(context.getProcessErrorThresholdWindow());
    setThresholdAction(context.getProcessErrorThresholdAction());
  }
  

  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.ProcessCasErrors#getThresholdCount()
 */
public int getThresholdCount() {
    Assert.notNull(pcet);
    return pcet.getThresholdCount();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessCasErrors#setThresholdCount(int)
   */
  public void setThresholdCount(int threshold) {
    Assert.notNull(pcet);
    pcet.setThresholdCount(threshold);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessCasErrors#getThresholdWindow()
   */
  public int getThresholdWindow() {
    Assert.notNull(pcet);
    return pcet.getThresholdWindow();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessCasErrors#setThresholdWindow(int)
   */
  public void setThresholdWindow(int window) {
    Assert.notNull(pcet);
    pcet.setThresholdWindow(window);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessCasErrors#getThresholdAction()
   */
  public Action getThresholdAction() {
    Assert.notNull(pcet);
    return Action.valueOf(pcet.getThresholdAction());
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessCasErrors#setThresholdAction(org.apache.uima.resourceSpecifier.factory.Action)
   */
  public void setThresholdAction(Action action) {
    Assert.notNull(pcet);
    pcet.setThresholdAction(action.name().toLowerCase());
  }

}
