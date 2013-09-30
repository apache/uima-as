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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaAsContext;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.PrimitiveAnalysisEngineController;
import org.apache.uima.aae.handler.Handler;
import org.apache.uima.aae.handler.input.MetadataRequestHandler_impl;
import org.apache.uima.aae.handler.input.MetadataResponseHandler_impl;
import org.apache.uima.aae.handler.input.ProcessRequestHandler_impl;
import org.apache.uima.aae.handler.input.ProcessResponseHandler;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.message.UIMAMessage;
import org.apache.uima.aae.message.UimaMessageValidator;
import org.apache.uima.aae.spi.transport.UimaMessage;
import org.apache.uima.aae.spi.transport.UimaMessageListener;
import org.apache.uima.util.Level;

public class UimaVmMessageListener implements UimaMessageListener {
  private static final Class CLASS_NAME = UimaVmMessageListener.class;

  private AnalysisEngineController controller;

  // Reference to the first message handler. Handlers are created and linked together
  // in the initialize() method. Each handler specializes in handling specific message
  // types. If a handler receives a message which is not meant for it, the handler passes
  // it on to the next handler in the chain.
  private transient Handler handler;

  // Prevents a message from being processed until initialization is done. Once the initialization
  // is completed the latch is lowered and messages are allowed to be processed.
  private CountDownLatch latch = new CountDownLatch(1);

  // This is used to identify threads. Thread names are modified by a listener for easier debugging.
  // A default thread name is overriden using the controller's name.
  private ConcurrentHashMap<Long, String> concurrentThreads = new ConcurrentHashMap<Long, String>();

  public UimaVmMessageListener(AnalysisEngineController aController) {
    controller = aController;
  }

  /**
   * Called when a new message is sent from a collocated client. Checks if the structure of the
   * message is valid (contains required attributes) and passes it on to the message handler for
   * processing.
   */
  public void onMessage(UimaMessage aMessage) {
    boolean doCheckpoint = false;
    int requestType = 0;
    try {
      latch.await();
      if (controller != null && controller.isStopped()) {
        return; // throw away the message, we are stopping
      }
      if (UimaMessageValidator.isValidMessage(aMessage, controller)) {
        MessageContext msgContext = aMessage.toMessageContext(controller.getName());
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "onMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_new_msg_recvd__FINEST",
                  new Object[] { controller.getComponentName(), aMessage.toString() });
        }
        if (!concurrentThreads.containsKey(Thread.currentThread().getId())) {
          Thread.currentThread().setName(
                  Thread.currentThread().getName() + "::" + controller.getComponentName() + "::"
                          + Thread.currentThread().getId());
          // Store the thread identifier in the map. The value stored is not important. All
          // we want is to save the fact that the thread name has been changed. And we only
          // want to change it once
          concurrentThreads.put(Thread.currentThread().getId(), Thread.currentThread().getName());
        }
        requestType = aMessage.getIntProperty(AsynchAEMessage.Command);
        if (requestType == AsynchAEMessage.Stop) {
          return;
        }
        // Determine if this message is a request and either GetMeta, CPC, or Process
        doCheckpoint = isCheckpointWorthy(aMessage);
        // Checkpoint
        if (doCheckpoint) {
          controller.beginProcess(requestType);
        }
        // Process the message.
        handler.handle(msgContext);
      }
    } catch (InterruptedException e) {
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( controller != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "onMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", controller.getComponentName());
          
        }
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "onMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    } finally {
      // Call the end checkpoint for non-aggregates. For primitives the CAS has been fully processed
      // if we are here
      if (doCheckpoint && controller instanceof PrimitiveAnalysisEngineController) {
        controller.endProcess(requestType);
      }
    }
  }

  /**
   * Initializes this listener. Instantiates and links message handlers. O
   */
  public void initialize(UimaAsContext context) throws Exception {
    MetadataRequestHandler_impl metaRequestHandler = new MetadataRequestHandler_impl(
            "MetadataRequestHandler");
    ProcessRequestHandler_impl processRequestHandler = new ProcessRequestHandler_impl(
            "ProcessRequestHandler");
    metaRequestHandler.setController(controller);
    processRequestHandler.setController(controller);
    handler = processRequestHandler;
    if (controller instanceof AggregateAnalysisEngineController) {
      ProcessResponseHandler processResponseHandler = new ProcessResponseHandler(
              "ProcessResponseHandler");
      processResponseHandler.setController(controller);
      processResponseHandler.setDelegate(metaRequestHandler);
      processRequestHandler.setDelegate(processResponseHandler);
      MetadataResponseHandler_impl metadataResponseHandler = new MetadataResponseHandler_impl(
              "MetadataResponseHandler");
      metadataResponseHandler.setController(controller);
      metaRequestHandler.setDelegate(metadataResponseHandler);
    } else {
      processRequestHandler.setDelegate(metaRequestHandler);
    }
    latch.countDown();
  }

  private boolean isCheckpointWorthy(UimaMessage aMessage) throws Exception {
    // Dont do checkpoints if a message was sent from a Cas Multiplier
    if (aMessage.containsProperty(AsynchAEMessage.CasSequence)) {
      return false;
    }

    if (aMessage.containsProperty(AsynchAEMessage.MessageType)
            && aMessage.containsProperty(AsynchAEMessage.Command)
            && aMessage.containsProperty(UIMAMessage.ServerURI)) {
      int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      if (msgType == AsynchAEMessage.Request
              && (command == AsynchAEMessage.Process || command == AsynchAEMessage.CollectionProcessComplete)) {
        return true;
      }
    }
    return false;

  }

  public void startIt() {
    // TODO Auto-generated method stub

  }

  public void stopIt() {
    // TODO Auto-generated method stub

  }

}
