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

package org.apache.uima.aae.delegate;

import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.message.AsynchAEMessage;

public class ControllerDelegate extends Delegate {
  // Controller that owns this delegate
  private AnalysisEngineController controller;

  /**
   * Initializes this instance with a unique delegate key
   * 
   * @param aDelegateKey
   */
  public ControllerDelegate(String aDelegateKey, AnalysisEngineController aController) {
    super.delegateKey = aDelegateKey;
    controller = aController;
  }

  public String getComponentName() {
    return controller.getComponentName();
  }
  public String enrichProcessCASTimeoutMessage(int aCommand, String casReferenceId, long timeToWait, String timeoutMessage) {
    StringBuffer sb = new StringBuffer(timeoutMessage);
    try {
      if ( aCommand == AsynchAEMessage.Process  && controller != null && casReferenceId != null ) {
        CasStateEntry cse = 
          controller.getLocalCache().lookupEntry(casReferenceId);
        if ( cse != null && cse.getHostIpProcessingCAS() != null) {
          sb.append(". Process CAS on host: "+cse.getHostIpProcessingCAS()+" exceeded configured timeout threshold of "+timeToWait+" ms");
        }
      } 
    } catch( Exception e) {
    }
    return sb.toString();
  }
  public void handleError(Exception e, ErrorContext errorContext) {
    if (controller != null && controller.getErrorHandlerChain() != null) {
      // Handle Timeout
      controller.getErrorHandlerChain().handle(e, errorContext, controller);
    }
  }
}
