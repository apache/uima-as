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

package org.apache.uima.aae.controller;

import java.util.concurrent.CountDownLatch;

import org.apache.uima.aae.error.AsynchAEException;

public class ControllerLatch {
  private final CountDownLatch latch = new CountDownLatch(1);

  private final AnalysisEngineController controller;

  public ControllerLatch(AnalysisEngineController aController) {
    controller = aController;
  }

  public void waitUntilInitialized() throws AsynchAEException {
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new AsynchAEException(e);
    }
  }

  public void release() {
    latch.countDown();

  }

  public void openLatch(String aName, boolean isTopLevelAggregate, boolean showMsg) {
    release();
    if (!showMsg) {
      return;
    }
    System.out.println("Service:" + controller.getComponentName()
            + " Initialized. Ready To Process Messages From Queue:" + aName);
  }

}
