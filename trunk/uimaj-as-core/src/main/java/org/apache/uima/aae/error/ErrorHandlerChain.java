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

package org.apache.uima.aae.error;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.uima.aae.controller.AnalysisEngineController;

public class ErrorHandlerChain extends LinkedList {
  public ErrorHandlerChain(List aChainofHandlers) {
    this.addAll(aChainofHandlers);
  }

  public Map getThresholds() {
    Map thresholds = new HashMap();
    Iterator iterator = this.iterator();
    while (iterator.hasNext()) {
      ErrorHandler handler = ((ErrorHandler) iterator.next());
      Map thresholdMap = handler.getEndpointThresholdMap();
      // merge
      thresholds.putAll(thresholdMap);
    }
    return thresholds;
  }

  public void handle(Throwable t, ErrorContext anErrorContext, AnalysisEngineController aController) {
    boolean errorHandled = false;
    if (size() > 0) {
      Throwable cause = t;
      if (t instanceof AsynchAEException && t.getCause() != null) {
        cause = t.getCause();
      }
      Iterator iterator = this.iterator();
      while (errorHandled == false && iterator.hasNext()) {
        ErrorHandler handler = ((ErrorHandler) iterator.next());
        errorHandled = handler.handleError(cause, anErrorContext, aController);
      }
    }
  }

}
