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

package org.apache.uima.aae.spi.transport.vm;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController_impl;
import org.apache.uima.aae.spi.transport.UimaMessage;
import org.apache.uima.aae.spi.transport.UimaMessageDispatcher;
import org.apache.uima.aae.spi.transport.UimaMessageListener;
import org.apache.uima.util.Level;

/**
 * 
 * Uima message implementation of {@link UimaMessageDispatcher}. It uses a Java's Executor framework
 * to pass Uima messages to a collocated Uima AS service. Each message is processed in a seperate
 * thread provided by the Executor.
 * 
 * */
public class UimaVmMessageDispatcher implements UimaMessageDispatcher {
  private static final Class<?> CLASS_NAME = UimaVmMessageDispatcher.class;

  private ThreadPoolExecutor executor = null;

  // Message listener which will receive a new message
  private final UimaMessageListener targetListener;

  private String delegateKey;

  public UimaVmMessageDispatcher(ThreadPoolExecutor anExecutor, UimaMessageListener aListener,
          String aKey) {
    executor = anExecutor;
    delegateKey = aKey;
    targetListener = aListener;
  }

  /**
   * This method is responsible for adding a Uima message to a queue which is shared with a
   * collocated service. Each message is processed by the receiving service in a thread provided by
   * the Executor.
   */
  public void dispatch(final UimaMessage message) {
    if (executor.isShutdown() || executor.isTerminating() || executor.isShutdown()) {
      return;
    }
    executor.execute(new Runnable() {
      public void run() {

        try {
          if (targetListener instanceof UimaVmMessageListener) {
            ((UimaVmMessageListener) targetListener).onMessage(message);
          }
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", e);
          }
        }
      }
    });
  }

  public void stop() {
    if (executor != null) {
      executor.purge();
      executor.shutdownNow();
    }
  }
}
