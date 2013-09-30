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

import java.util.Timer;

import org.apache.uima.aae.controller.BaseAnalysisEngineController.ServiceState;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.jmx.ServiceInfo;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.TypeSystemImpl;

public class Endpoint_impl implements Endpoint, Cloneable {
  private static final Class CLASS_NAME = Endpoint_impl.class;

  private volatile Object destination = null;

  private String endpoint;  // is the queue name (only)

  private String serverURI;

  private volatile boolean initialized;

  private Timer timer;

  private String replyTo;

  private volatile boolean waitingForResponse;

  private int metadataRequestTimeout;

  private int processRequestTimeout;

  private int collectionProcessCompleteTimeout;

  private volatile boolean isRemote;

  private String descriptor;

  private SerialFormat serialFormat = null;
  
  private String serializer = "xmi";  // spring bean interface

  private volatile boolean finalEndpoint;

  private final long timeIn = System.nanoTime();

  private long checkpointTimer;

  private AnalysisEngineController controller;

  private Endpoint selfRef = this;

  private volatile boolean retryEnabled;

  private Object monitor = new Object();

  private String highWaterMark = null;

  private volatile boolean completedProcessingCollection;

  private volatile boolean noConsumers = false;

  private volatile boolean remove = false;

  private volatile boolean isCasMultiplier = false;

  private int shadowCasPoolSize = 0;

  private volatile boolean isReplyEndpointFlag;

  private ServiceInfo serviceInfo = null;

  private int command;

  private volatile boolean registeredWithParent;

  private volatile boolean tempReplyDestination;

  private int initialHeapSize;

  private volatile boolean replyDestinationFailed;

  private long idleTime = 0;

  private int concurrentRequestConsumers = 1;

  private int concurrentReplyConsumers = 1;

  // This is supplied by the remote client. It needs to be
  // echoed back to the client.
  private String endpointServer = null;

  private int status;

  private String delegateKey;

  private volatile boolean processParentLast = false;

  private volatile boolean freeCasEndpoint = false;
  
  private volatile TypeSystemImpl typeSystemImpl;
  
  private volatile boolean disableJCasCache;
  
  public boolean isDisableJCasCache() {
    return disableJCasCache;
  }

  public void setDisableJCasCache(boolean disableJCasCache) {
    this.disableJCasCache = disableJCasCache;
  }

  public Endpoint_impl() {
    status = Endpoint.OK;
  }
  
  public void setTypeSystemImpl(TypeSystemImpl typeSystemImpl) {
    this.typeSystemImpl = typeSystemImpl;
  }
  
  public TypeSystemImpl getTypeSystemImpl() {
    return typeSystemImpl;
  }
  
  public void setFreeCasEndpoint(boolean trueOrFalse) {
    freeCasEndpoint = trueOrFalse;
  }
  public boolean isFreeCasEndpoint() {
    return freeCasEndpoint;
  }
  public void setProcessParentLast(boolean parentLast) {
    processParentLast = parentLast;
  }

  public boolean processParentLast() {
    return processParentLast;
  }

  public int getCommand() {
    return command;
  }

  public void setCommand(int command) {
    this.command = command;
  }

  public void setNoConsumers(boolean trueOrFalse) {
    noConsumers = trueOrFalse;
  }

  public void setReplyEndpoint(boolean tORf) {
    isReplyEndpointFlag = tORf;
  }

  public boolean isReplyEndpoint() {
    return isReplyEndpointFlag;
  }

  public boolean remove() {
    return remove;

  }

  public void setRemove(boolean rm) {
    remove = rm;
  }

  public boolean hasNoConsumers() {
    return noConsumers;
  }

  public String getReplyToEndpoint() {
    return replyTo;
  }

  public void setReplyToEndpoint(String anEndpointName) {
    replyTo = anEndpointName;
  }

  public boolean completedProcessingCollection() {
    return completedProcessingCollection;
  }

  public void setCompletedProcessingCollection(boolean completed) {
    completedProcessingCollection = completed;
  }

  public void setHighWaterMark(String aHighWaterMark) {
    highWaterMark = aHighWaterMark;
  }

  public String getHighWaterMark() {
    return highWaterMark;
  }

  public boolean isRetryEnabled() {
    return retryEnabled;
  }

  public void setRetryEnabled(boolean retryEnabled) {
    this.retryEnabled = retryEnabled;
  }

  public void setController(AnalysisEngineController aController) {
    controller = aController;
  }

  public void startCheckpointTimer() {
    checkpointTimer = System.nanoTime();
  }

  public long getCheckpointTimer() {
    return checkpointTimer;
  }

  public long getEntryTime() {
    return timeIn;
  }

  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new InternalError(e.toString());
    }
  }

  public String getSerializer() {
    return serializer;
  }

  public boolean isFinal() {
    return finalEndpoint;
  }

  public void setFinal(boolean isFinal) {
    finalEndpoint = isFinal;
  }

  public void setSerializer(String serializer) {
    this.serializer = serializer;
  }
  
  public SerialFormat getSerialFormat() {
    if (serialFormat == null) {
      serialFormat = (serializer.equalsIgnoreCase("xmi")) ? SerialFormat.XMI : SerialFormat.BINARY;
    }
    return serialFormat;
  }
  
  public void setSerialFormat(SerialFormat serialFormat) {
    this.serialFormat = serialFormat;
    this.serializer = (serialFormat == SerialFormat.XMI) ? "xmi" : "binary";  // for error messages
  }
 

  public int getMetadataRequestTimeout() {
    return metadataRequestTimeout;
  }

  public void setMetadataRequestTimeout(int metadataRequestTimeout) {
    this.metadataRequestTimeout = metadataRequestTimeout;
  }

  public int getProcessRequestTimeout() {
    return processRequestTimeout;
  }

  public void setProcessRequestTimeout(int processRequestTimeout) {
    this.processRequestTimeout = processRequestTimeout;
  }

  public void setCollectionProcessCompleteTimeout(int cpcTimeout) {
    this.collectionProcessCompleteTimeout = cpcTimeout;
  }

  public int getCollectionProcessCompleteTimeout() {
    return collectionProcessCompleteTimeout;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getServerURI() {
    return serverURI;
  }

  public void setServerURI(String aServerURI) {
    this.serverURI = aServerURI;
    if (aServerURI != null && aServerURI.startsWith("vm:") == true) {
      setRemote(false);
    } else {
      setRemote(true);
    }
  }

  public void setWaitingForResponse(boolean isWaiting) {
    waitingForResponse = isWaiting;

  }

  private void startTimer(final int aTimeToWait, String aCasReferenceId, int command) {
    /*
     * synchronized( monitor ) { final String casReferenceId = aCasReferenceId; final int cmd =
     * command; Date timeToRun = new Date(System.currentTimeMillis() + aTimeToWait);
     * 
     * 
     * setWaitingForResponse(true); // timer = new Timer();
     * 
     * 
     * if ( controller != null ) { timer = new
     * Timer("Controller:"+controller.getComponentName()+":TimerThread-Endpoint_impl:"
     * +endpoint+":"+System.nanoTime()+":Cmd:"+cmd); } else { timer = new
     * Timer("TimerThread-Endpoint_impl:"+endpoint+":"+System.nanoTime()+":Cmd:"+cmd); }
     * 
     * 
     * timer.schedule(new TimerTask() { public void run() { if ( AsynchAEMessage.Process == cmd ) {
     * if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
     * UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, this.getClass().getName(),
     * "TimerTask.run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
     * "UIMAEE_cas_timeout_no_reply__INFO", new Object[] { endpoint, aTimeToWait, casReferenceId });
     * } } else if ( AsynchAEMessage.GetMeta == cmd ) { if
     * (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
     * UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, this.getClass().getName(),
     * "TimerTask.run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
     * "UIMAEE_meta_timeout_no_reply__INFO", new Object[] { endpoint, aTimeToWait }); } } else { if
     * (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
     * UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, this.getClass().getName(),
     * "TimerTask.run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
     * "UIMAEE_cpc_timeout_no_reply__INFO", new Object[] { endpoint, aTimeToWait }); }
     * 
     * }
     * 
     * waitingForResponse = false; if ( timer != null ) { timer.cancel(); timer.purge(); }
     * 
     * if ( controller != null ) {
     * 
     * ErrorContext errorContext = new ErrorContext(); if ( casReferenceId != null ) {
     * errorContext.add(AsynchAEMessage.CasReference, String.valueOf(casReferenceId)); }
     * errorContext.add(AsynchAEMessage.Command, cmd); errorContext.add(AsynchAEMessage.Endpoint,
     * selfRef); if ( controller != null && controller.getErrorHandlerChain() != null ) { // Handle
     * Timeout controller.getErrorHandlerChain().handle(new MessageTimeoutException(), errorContext,
     * controller); } } } }, timeToRun); }
     */
  }

  public ServiceInfo getServiceInfo() {
    if (serviceInfo == null) {
      serviceInfo = new ServiceInfo(isCasMultiplier, null);
      serviceInfo.setBrokerURL(serverURI);
      serviceInfo.setInputQueueName(endpoint);
      if ( controller == null ) {
        serviceInfo.setState(ServiceState.INITIALIZING.name());
      } else {
        serviceInfo.setState(controller.getState().name());
      }
    }
    return serviceInfo;
  }

  public void startProcessRequestTimer(String aCasReferenceId) {
    if (getProcessRequestTimeout() > 0) {
      startTimer(processRequestTimeout, aCasReferenceId, AsynchAEMessage.Process);
    } else {
      setWaitingForResponse(true);
    }

  }

  public void startMetadataRequestTimer() {
    if (getMetadataRequestTimeout() > 0) {
      startTimer(metadataRequestTimeout, null, AsynchAEMessage.GetMeta);
    } else {
      setWaitingForResponse(true);
    }
  }

  public void startCollectionProcessCompleteTimer() {
    if (getCollectionProcessCompleteTimeout() > 0) {
      startTimer(collectionProcessCompleteTimeout, null, AsynchAEMessage.CollectionProcessComplete);
    } else {
      setWaitingForResponse(true);
    }

  }

  public void cancelTimer() {
    /*
     * synchronized( monitor ) { if (timer != null) { waitingForResponse = false; timer.cancel();
     * timer = null; } }
     */
  }

  public boolean isWaitingForResponse() {

    return waitingForResponse;
  }

  public boolean isRemote() {
    return isRemote;
  }

  public void setRemote(boolean aRemote) {
    isRemote = aRemote;

  }

  public String getDescriptor() {
    return descriptor;
  }

  public void setDescriptor(String aDescriptor) {
    descriptor = aDescriptor;
  }

  public void initialize() throws AsynchAEException {
    // TODO Auto-generated method stub

  }

  public boolean isOpen() {
    return true;
  }

  public void close() {

  }

  public boolean isCasMultiplier() {
    return isCasMultiplier;
  }

  public void setIsCasMultiplier(boolean trueORfalse) {
    isCasMultiplier = trueORfalse;
    if (isCasMultiplier) {
      getServiceInfo().setCASMultiplier();
    }
  }

  public void setShadowCasPoolSize(int aPoolSize) {
    shadowCasPoolSize = aPoolSize;
  }

  public int getShadowPoolSize() {
    return shadowCasPoolSize;
  }

  public Object getDestination() {
    return destination;
  }

  public void setDestination(Object aDestination) {
    destination = aDestination;
  }

  public void setRegisteredWithParent() {
    registeredWithParent = true;
  }

  public boolean isRegisteredWithParent() {
    return registeredWithParent;
  }

  public void setInitialFsHeapSize(int aHeapSize) {
    initialHeapSize = aHeapSize;
  }

  public void setTempReplyDestination(boolean isTempReplyDestination) {
    tempReplyDestination = isTempReplyDestination;
  }

  public boolean isTempReplyDestination() {
    return tempReplyDestination;
  }

  public void setReplyDestinationFailed() {
    replyDestinationFailed = true;
  }

  public boolean replyDestinationFailed() {
    return replyDestinationFailed;
  }

  public long getIdleTime() {
    return idleTime;
  }

  public void setIdleTime(long idleTime) {
    this.idleTime = idleTime;
  }

  /*
   * Print name of the endpoint rather than class hash code
   */
  public String toString() {
    return endpoint;
  }

  public void setEndpointServer(String anEndpointServer) {
    endpointServer = anEndpointServer;
  }

  public String getEndpointServer() {
    return endpointServer;
  }

  public void setConcurrentRequestConsumers(int aConsumerCount) {
    concurrentRequestConsumers = aConsumerCount;
  }

  public int getConcurrentRequestConsumers() {
    return concurrentRequestConsumers;
  }

  public void setConcurrentReplyConsumers(int aConsumerCount) {
    concurrentReplyConsumers = aConsumerCount;

  }

  public int getConcurrentReplyConsumers() {
    return concurrentReplyConsumers;
  }

  public void setStatus(int aStatus) {
    status = aStatus;
  }

  public int getStatus() {
    return status;
  }

  public void setDelegateKey(String aDelegateKey) {
    delegateKey = aDelegateKey;
  }

  public String getDelegateKey() {
    return delegateKey;
  }
}