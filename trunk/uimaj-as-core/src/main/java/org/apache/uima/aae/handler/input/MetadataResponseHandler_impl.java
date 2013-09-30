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
import org.apache.uima.aae.controller.BaseAnalysisEngineController.ServiceState;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.handler.HandlerBase;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.util.Level;

public class MetadataResponseHandler_impl extends HandlerBase {
  private static final Class CLASS_NAME = MetadataResponseHandler_impl.class;

  public MetadataResponseHandler_impl(String aName) {
    super(aName);
  }

  /**
   * This method handles both incoming request for metadata and outgoing response containing
   * metadata.
   * 
   */
  public void handle(Object anObjectToHandle) {

    if (anObjectToHandle instanceof MessageContext) {
      try {
        int messageType = ((MessageContext) anObjectToHandle)
                .getMessageIntProperty(AsynchAEMessage.MessageType);
        int command = ((MessageContext) anObjectToHandle)
                .getMessageIntProperty(AsynchAEMessage.Command);
        int payload = ((MessageContext) anObjectToHandle)
                .getMessageIntProperty(AsynchAEMessage.Payload);

        if (AsynchAEMessage.Response == messageType && AsynchAEMessage.GetMeta == command) {
          // Metadata Response is only applicable to the Aggregate Controller
          if (getController() instanceof AggregateAnalysisEngineController) {
            String fromEndpoint = ((MessageContext) anObjectToHandle)
                    .getMessageStringProperty(AsynchAEMessage.MessageFrom);

            String delegateKey = ((AggregateAnalysisEngineController) getController())
                    .lookUpDelegateKey(fromEndpoint);

            // Some delegates may not include supported serialization. If thats the case
            // assume XMI as a default serialization for such delegate. Also, check
            // delegate configuration (provided in the deployment descriptor) and
            // make sure that it matches "xmi". If the configuration says "binary" there
            // is a mis-configuration which we handle by overriding the endpoint setting using
            // "xmi" as a value for serialization strategy.
            int serializationSupportedByRemote;
            if (!((MessageContext) anObjectToHandle).propertyExists(AsynchAEMessage.SERIALIZATION)) {
              serializationSupportedByRemote = AsynchAEMessage.XMI_SERIALIZATION;
              Endpoint masterEndpoint = ((AggregateAnalysisEngineController) getController())
                      .lookUpEndpoint(delegateKey, false);
              if (masterEndpoint.getSerialFormat() != SerialFormat.XMI) {
                // Override configured serialization
                masterEndpoint.setSerialFormat(SerialFormat.XMI);
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                        "handle", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_override_serialization__WARNING",
                        new Object[] { getController().getComponentName(), delegateKey });
              }
            } else {
              // record the version of compressed binary serialization supported (if any)
              serializationSupportedByRemote = ((MessageContext) anObjectToHandle).getMessageIntProperty(AsynchAEMessage.SERIALIZATION);
            }
            Delegate delegate = ((AggregateAnalysisEngineController) getController())
                    .lookupDelegate(delegateKey);
            if (delegate.getEndpoint().isRemote()) {
              delegate.cancelDelegateGetMetaTimer();
              delegate.setState(Delegate.OK_STATE);
              delegate.setNotificationEndpoint(((MessageContext) anObjectToHandle).getEndpoint());

              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, this.getClass().getName(),
                        "handle", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_cancelled_timer_FINE",
                        new Object[] { getController().getComponentName(), delegateKey });
              }
              String casReferenceId = null;
              // Check if the GetMeta reply was actually a PING message to check
              // delegate's availability. This would be the case if the delegate
              // has previously timed out waiting for Process CAS reply.
              if (delegate.isAwaitingPingReply() && delegate.getState() == Delegate.OK_STATE) {
                // Since this is a reply to a ping we may have delayed some
                // CASes waiting for the ping to come back. Drain the list
                // of delayed CASes and send each CAS to the delegate.
                while ((casReferenceId = delegate.removeOldestFromPendingDispatchList()) != null) {
                  ((AggregateAnalysisEngineController) getController()).retryLastCommand(
                          AsynchAEMessage.Process, delegate.getEndpoint(), casReferenceId);
                }

                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, this.getClass().getName(),
                          "handle", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_aggregate_rcvd_ping_reply__FINE",
                          new Object[] { getController().getComponentName(), delegateKey });
                }
                // Reset delegate flag to indicate that the ping reply was received
                delegate.resetAwaitingPingReply();
                // No need to merge typesystem. We've received a reply to a ping
                return;
              }
            }
            if (AsynchAEMessage.Exception == payload) {
              return;
            }

            String analysisEngineMetadata = ((MessageContext) anObjectToHandle).getStringMessage();
            String fromServer = null;
            if (((MessageContext) anObjectToHandle).propertyExists(AsynchAEMessage.EndpointServer)) {
              fromServer = ((MessageContext) anObjectToHandle)
                      .getMessageStringProperty(AsynchAEMessage.EndpointServer);
            }
            ((AggregateAnalysisEngineController)getController()).changeCollocatedDelegateState(delegateKey, ServiceState.RUNNING);
            // If old service does not echo back the external broker name then the queue name must
            // be unique.
            // The ServerURI set by the service may be its local name for the broker, e.g.
            // tcp://localhost:61616
            ((AggregateAnalysisEngineController) getController()).mergeTypeSystem(
                    analysisEngineMetadata, fromEndpoint, fromServer);
            ((AggregateAnalysisEngineController) getController()).setRemoteSerializationSupported(serializationSupportedByRemote, fromEndpoint, fromServer);
          }
        } else {

          if (super.hasDelegateHandler()) {
            super.getDelegate().handle(anObjectToHandle);
          }
        }
      } catch (Exception e) {
        getController().notifyListenersWithInitializationStatus(e);
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
