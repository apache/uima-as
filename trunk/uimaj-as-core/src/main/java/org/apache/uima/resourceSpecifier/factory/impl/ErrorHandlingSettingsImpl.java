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
import org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings;

// TODO: Auto-generated Javadoc
/**
 * The Class ErrorHandlingSettingsImpl.
 */
public abstract class ErrorHandlingSettingsImpl implements ErrorHandlingSettings {
  
  /** The max retries. */
  private int maxRetries;
  
  /** The timeout. */
  private int timeout;
  
  /** The action. */
  private Action action;

  /**
   * Instantiates a new error handling settings impl.
   */
  public ErrorHandlingSettingsImpl() {
    this(0,0,Action.Terminate);
  }
  
  /**
   * Instantiates a new error handling settings impl.
   *
   * @param retryCount the retry count
   * @param timeout the timeout
   * @param action the action
   */
  public ErrorHandlingSettingsImpl(int retryCount, int timeout, Action action) {
    setMaxRetries(retryCount);
    setTimeout(timeout);
    setAction(action);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings#setMaxRetries(int)
   */
  public void setMaxRetries(int retryCount) {
    maxRetries = retryCount;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings#getMaxRetries()
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings#setTimeout(int)
   */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings#getTimeout()
   */
  public int getTimeout() {
    return timeout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings#setAction(org.apache.uima.resourceSpecifier.factory.Action)
   */
  public void setAction(Action action) {
    this.action = action;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings#getAction()
   */
  public Action getAction() {
    return action;
  }
}
