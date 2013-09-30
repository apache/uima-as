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
package org.apache.uima.adapter.jms.client;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.Destination;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.error.MessageTimeoutException;
import org.apache.uima.aae.error.UimaASPingTimeout;
import org.apache.uima.aae.error.UimaASProcessCasTimeout;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngineCommon_impl.ClientRequest;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngineCommon_impl.ClientState;
import org.apache.uima.cas.CAS;
import org.apache.uima.util.Level;

public class ClientServiceDelegate extends Delegate {
  private static final Class CLASS_NAME = ClientServiceDelegate.class;

  private BaseUIMAAsynchronousEngineCommon_impl clientUimaAsEngine;

  private String applicationName = "UimaAsClient";

  private volatile boolean usesSynchronousAPI;

  private Destination freeCasDestination = null;
  
  private Object errorMux = new Object();

  private volatile boolean pingTimeout = false;
  
  public ClientServiceDelegate(String serviceName, String anApplicationName,
          BaseUIMAAsynchronousEngineCommon_impl engine) {
    super.delegateKey = serviceName;
    clientUimaAsEngine = engine;
    if (anApplicationName != null && anApplicationName.trim().length() > 0) {
      applicationName = anApplicationName;
    }
  }

  public boolean isSynchronousAPI() {
    return usesSynchronousAPI;
  }
  public boolean isPingTimeout() {
    return pingTimeout;
  }
  public void resetPingTimeout() {
    pingTimeout = false;
  }
  public void setSynchronousAPI() {
    this.usesSynchronousAPI = true;
  }

  public Destination getFreeCasDestination() {
    return freeCasDestination;
  }

  public void setFreeCasDestination(Destination freeCasDestination) {
    this.freeCasDestination = freeCasDestination;
  }

  public String getComponentName() {
    return applicationName;
  }

  public void handleError(Exception e, ErrorContext errorContext) {
    String casReferenceId = null;
    CAS cas = null;
    ClientRequest cachedRequest = null;
    casReferenceId = (String) errorContext.get(AsynchAEMessage.CasReference);

    synchronized(errorMux) {
      if (!clientUimaAsEngine.running) {
        cancelDelegateTimer();
        return;
      }
      
      int command = ((Integer) errorContext.get(AsynchAEMessage.Command)).intValue();
      try {
        if (e instanceof MessageTimeoutException) {
          switch (command) {
            case AsynchAEMessage.Process:
              //casReferenceId = (String) errorContext.get(AsynchAEMessage.CasReference);
              if (casReferenceId != null) {
                cachedRequest = (ClientRequest) clientUimaAsEngine.clientCache.get(casReferenceId);
                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)
                        && getEndpoint() != null) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                          "handleError", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAJMS_process_timeout_WARNING",
                          new Object[] { getEndpoint().getEndpoint(), clientUimaAsEngine.getBrokerURI(), cachedRequest.getHostIpProcessingCAS() });
                }
                if (cachedRequest != null && cachedRequest.isRemote()) {
                  cas = cachedRequest.getCAS();
                }
                boolean isPingTimeout = false;
                if (errorContext.containsKey(AsynchAEMessage.ErrorCause)) {
                  isPingTimeout = AsynchAEMessage.PingTimeout == (Integer) errorContext
                          .get(AsynchAEMessage.ErrorCause);
                }
                if (isPingTimeout && isAwaitingPingReply()) {
                  //  reset only if the connection is valid
                  if ( clientUimaAsEngine.state != ClientState.RECONNECTING) {
                    resetAwaitingPingReply();
                  }
                  pingTimeout = true;
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                          "handleError", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAJMS_client_ping_timed_out__WARNING", new Object[] { getKey() });
                  clientUimaAsEngine.notifyOnTimout(cas, clientUimaAsEngine.getEndPointName(),
                          BaseUIMAAsynchronousEngineCommon_impl.ProcessTimeout, casReferenceId);
                } else {
                  if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                    UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                            "handleError", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                            "UIMAJMS_client_process_timeout__WARNING", new Object[] { super.getCasProcessTimeout() });
                  }
                  clientUimaAsEngine.notifyOnTimout(cas, clientUimaAsEngine.getEndPointName(),
                          BaseUIMAAsynchronousEngineCommon_impl.ProcessTimeout, casReferenceId);
                }
              }
              clientUimaAsEngine.clientSideJmxStats.incrementProcessTimeoutErrorCount();
              break;

            case AsynchAEMessage.GetMeta:
              if (isAwaitingPingReply()) {
            	  
                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                        "handleError", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_client_ping_timed_out__WARNING", new Object[] { clientUimaAsEngine.getEndPointName(),getCasPendingDispatchListSize(),getCasPendingReplyListSize()});
                }
                super.resetAwaitingPingReply();
                
                synchronized( super.pendingDispatchList ) {
                  //  Fail all CASes in the PendingDispatch list
                  Iterator<Delegate.DelegateEntry> it = getDelegateCasesPendingDispatch().iterator();
                  while( clientUimaAsEngine.running && it.hasNext() ) {
                    DelegateEntry de = it.next();
                    cachedRequest = (ClientRequest) (clientUimaAsEngine.getCache()).get(de.getCasReferenceId());
                    if ( cachedRequest != null ) {
                      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                                "handleError", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                                "UIMAJMS_client_reject_by_forced_timeout__WARNING", new Object[] { de.getCasReferenceId(), String.valueOf(cachedRequest.getCAS().hashCode())});
                      }
                      //dumpDelayedList();
                      try {
                        clientUimaAsEngine.handleException(new UimaASProcessCasTimeout("Service Not Responding to Ping - CAS:"+de.getCasReferenceId(), new UimaASPingTimeout("Forced Timeout on CAS in PendingDispatch list. The CAS Has Not Been Dispatched since the Service Appears to be Unavailable")), de.getCasReferenceId(), null,cachedRequest, !cachedRequest.isSynchronousInvocation(), false);
                      } catch( Exception ex) {
                        ex.printStackTrace();
                      }
                    }
                    if ( clientUimaAsEngine.running ) {
                      it.remove();
                    }
                  }
                }
              } else {
                  if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                              "handleError", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                              "UIMAJMS_meta_timeout_WARNING", new Object[] { getKey() });
                    }
                // Notifies Listeners and removes ClientRequest instance from the client cache
                clientUimaAsEngine.notifyOnTimout(cas, clientUimaAsEngine.getEndPointName(),
                        BaseUIMAAsynchronousEngineCommon_impl.MetadataTimeout, casReferenceId);
                clientUimaAsEngine.clientSideJmxStats.incrementMetaTimeoutErrorCount();
              }
              break;

            case AsynchAEMessage.CollectionProcessComplete:

              break;
          }
        }
      } catch (Exception ex) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                  "handleError", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", ex);
        }
      }
      // Dont release the CAS if synchronous API was used
      if (cas != null && !cachedRequest.isSynchronousInvocation()) {
        cas.release();
      }

    }
  }
  public String enrichProcessCASTimeoutMessage(int aCommand, String casReferenceId, long timeToWait, String timeoutMessage) {
    StringBuffer sb = new StringBuffer(timeoutMessage);
    try {
      if ( casReferenceId != null && clientUimaAsEngine.getCache().containsKey(casReferenceId) ) {
        ClientRequest cr = 
          (ClientRequest)clientUimaAsEngine.getCache().get(casReferenceId);
        if ( cr != null ) {
          sb.append(". Process CAS on host: "+cr.getHostIpProcessingCAS()+" exceeded configured timeout threshold of "+timeToWait+" ms");
        }
      }
    } catch( Exception e) {
      e.printStackTrace();
    }
    return sb.toString();
  }

}
