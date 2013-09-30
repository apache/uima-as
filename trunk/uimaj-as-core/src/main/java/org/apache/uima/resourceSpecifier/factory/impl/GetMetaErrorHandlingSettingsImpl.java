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
import org.apache.uima.resourceSpecifier.factory.GetMetaErrorHandlingSettings;
import org.springframework.util.Assert;

/**
 * The Class GetMetaErrorHandlingSettingsImpl.
 */
public class GetMetaErrorHandlingSettingsImpl extends ErrorHandlingSettingsImpl
implements GetMetaErrorHandlingSettings {
  
  /**
   * Instantiates a new gets the meta error handling settings impl.
   */
  public GetMetaErrorHandlingSettingsImpl() {
    this(0,0,Action.Terminate);
  }
  
  /**
   * Instantiates a new gets the meta error handling settings impl.
   *
   * @param retryCount the retry count
   * @param timeout the timeout
   * @param action the action
   */
  public GetMetaErrorHandlingSettingsImpl(int retryCount, int timeout, Action action) {
    super.setMaxRetries(retryCount);
    super.setTimeout(timeout);
    Assert.notNull(action);
    Assert.isTrue(action.equals(Action.Terminate) || action.equals(Action.Disable), "GetMeta Error Handler Action Must Either Be: terminate or disable. Provided action:"+action+" is not valid.");
    super.setAction(action);
  }
}
