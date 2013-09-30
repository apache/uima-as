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

import org.apache.uima.resourceSpecifier.factory.Action;
import org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings;

// TODO: Auto-generated Javadoc
/**
 * The Class ProcessErrorHandlingSettingsImpl.
 */
public class ProcessErrorHandlingSettingsImpl extends ErrorHandlingSettingsImpl implements
        ProcessErrorHandlingSettings {
  
  /** The continue on retry failure. */
  private boolean continueOnRetryFailure;
  
  /** The threshold count. */
  private int thresholdCount;
  
  /** The threshold window. */
  private int thresholdWindow;
  
  /**
   * Instantiates a new process error handling settings impl.
   */
  public ProcessErrorHandlingSettingsImpl() {
    
  }
  
  /**
   * Instantiates a new process error handling settings impl.
   *
   * @param retryCount the retry count
   * @param timeout the timeout
   * @param action the action
   * @param continueOnRetruFailure the continue on retru failure
   * @param thresholdCount the threshold count
   * @param thresholdWindow the threshold window
   */
  public ProcessErrorHandlingSettingsImpl(int retryCount, int timeout, Action action, boolean continueOnRetruFailure, int thresholdCount, int thresholdWindow) {
    super(retryCount,timeout,action);
    setContinueOnRetryFailure(continueOnRetruFailure);
    setThresholdCount(thresholdCount);
    setThresholdWindow(thresholdWindow);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings#setContinueOnRetryFailure(boolean)
   */
  public void setContinueOnRetryFailure(boolean continueOnRetryFailure) {
    this.continueOnRetryFailure = continueOnRetryFailure;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings#continueOnRetryFailure()
   */
  public boolean continueOnRetryFailure() {
    return continueOnRetryFailure;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings#setThresholdCount(int)
   */
  public void setThresholdCount(int thresholdCount) {
    this.thresholdCount = thresholdCount;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings#getThresholdCount()
   */
  public int getThresholdCount() {
    return thresholdCount;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings#setThresholdWindow(int)
   */
  public void setThresholdWindow(int thresholdWindow) {
    this.thresholdWindow = thresholdWindow;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings#getThresholdWindow()
   */
  public int getThresholdWindow() {
    return thresholdWindow;
  }

}
