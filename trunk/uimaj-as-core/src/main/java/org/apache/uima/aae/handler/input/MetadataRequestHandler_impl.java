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

package org.apache.uima.aae.handler.input;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.handler.HandlerBase;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.util.Level;

public class MetadataRequestHandler_impl extends HandlerBase {
  private static final Class CLASS_NAME = MetadataRequestHandler_impl.class;

  public MetadataRequestHandler_impl(String aName) {
    super(aName);
  }

  public void handle(Object anObjectToHandle)// throws AsynchAEException
  {

    if (anObjectToHandle instanceof MessageContext) {
      try {
        int messageType = ((MessageContext) anObjectToHandle)
                .getMessageIntProperty(AsynchAEMessage.MessageType);
        int command = ((MessageContext) anObjectToHandle)
                .getMessageIntProperty(AsynchAEMessage.Command);

        // This handler handles request for metadata. Check if this is the request for metadata. If
        // not, pass it on
        // to the next handler in the chain.
        if (AsynchAEMessage.Request == messageType && AsynchAEMessage.GetMeta == command) {

          Endpoint endpoint = ((MessageContext) anObjectToHandle).getEndpoint();
          if (getController().isTopLevelComponent()) {
            endpoint.setCommand(AsynchAEMessage.GetMeta);
            getController().cacheClientEndpoint(endpoint);
          }
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "handle",
                    UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_handling_metadata_request__FINEST",
                    new Object[] { endpoint.getEndpoint() });
          }
          getController().getControllerLatch().waitUntilInitialized();
          // Check to see if the controller hasnt been aborted while we were waiting on the latch
          if (!getController().isStopped()) {
            getController().sendMetadata(endpoint);
          }
        } else {
          if (super.hasDelegateHandler()) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                      "handle", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_msg_for_next_handler__FINEST", new Object[] { messageType });
            }
            super.getDelegate().handle(anObjectToHandle);
          } else {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "handle",
                      UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_no_handler_for_message__FINE", new Object[] { messageType });
            }
          }
        }
      } catch (Exception e) {
        getController().getErrorHandlerChain().handle(e,
                HandlerBase.populateErrorContext((MessageContext) anObjectToHandle),
                getController());
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "handle",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_invalid_context_object__INFO",
                new Object[] { getController().getName(), anObjectToHandle.getClass().getName() });
      }
    }
  }

}
