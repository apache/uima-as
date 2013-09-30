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

import java.util.Iterator;
import java.util.Map;

import org.apache.uima.UimaContext;
import org.apache.uima.aae.AsynchAECasManager;
import org.apache.uima.aae.InProcessCache;
import org.apache.uima.aae.InputChannel;
import org.apache.uima.aae.OutputChannel;
import org.apache.uima.aae.UimaAsContext;
import org.apache.uima.aae.UimaEEAdminContext;
import org.apache.uima.aae.controller.BaseAnalysisEngineController.ServiceState;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.error.ErrorHandlerChain;
import org.apache.uima.aae.jmx.JmxManagement;
import org.apache.uima.aae.jmx.ServiceErrors;
import org.apache.uima.aae.jmx.ServiceInfo;
import org.apache.uima.aae.jmx.ServicePerformance;
import org.apache.uima.aae.monitor.Monitor;
import org.apache.uima.aae.spi.transport.UimaMessageListener;
import org.apache.uima.aae.spi.transport.UimaTransport;
import org.apache.uima.cas.CAS;

/**
 * Abstract class meant to provide default implementations for AnalysisEngineController instances.
 * 
 * 
 */
public abstract class AnalysisEngineControllerAdapter implements AnalysisEngineController {

  public void addAbortedCasReferenceId(String casReferenceId) {

  }

  public void addInputChannel(InputChannel anInputChannel) throws Exception {

  }

  public void addServiceInfo(ServiceInfo serviceInfo) {

  }

  public void addTimeSnapshot(long snapshot, String key) {

  }

  public void beginProcess(int msgType) {

  }

  public void cacheClientEndpoint(Endpoint anEndpoint) {

  }

  public void collectionProcessComplete(Endpoint anEndpoint) throws AsynchAEException {

  }

  public void dropCAS(String casReferenceId, boolean dropCacheEntry) {

  }

  public void dropCAS(CAS acas) {

  }

  public void dropStats(String casReferenceId, String anEndpointName) {

  }

  public void endProcess(int msgType) {

  }

  public void forceTimeoutOnPendingCases(String key) {

  }

  public long getAnalysisTime() {

    return 0;
  }

  public AsynchAECasManager getCasManagerWrapper() {

    return null;
  }

  public ServicePerformance getCasStatistics(String casReferenceId) {

    return null;
  }

  public UimaContext getChildUimaContext(String delegateEndpointName) throws Exception {

    return null;
  }

  public Endpoint getClientEndpoint() {

    return null;
  }

  public String getComponentName() {

    return null;
  }
  public Map<String,String> getDeadClientMap() {
	  return null;
  }

  public ControllerLatch getControllerLatch() {

    return null;
  }

  public long getCpuTime() {

    return 0;
  }

  public ErrorHandlerChain getErrorHandlerChain() {

    return null;
  }

  public EventSubscriber getEventListener() {

    return null;
  }

  public long getIdleTime() {

    return 0;
  }

  public long getIdleTimeBetweenProcessCalls(int msgType) {

    return 0;
  }

  public InProcessCache getInProcessCache() {

    return null;
  }

  public int getIndex() {

    return 0;
  }

  public InputChannel getInputChannel() {

    return null;
  }

  public InputChannel getInputChannel(String queueName) {

    return null;
  }

  public String getJMXDomain() {

    return null;
  }

  public String getJmxContext() {

    return null;
  }

  public LocalCache getLocalCache() {

    return null;
  }

  public JmxManagement getManagementInterface() {

    return null;
  }

  public Monitor getMonitor() {

    return null;
  }

  public String getName() {

    return null;
  }

  public OutputChannel getOutputChannel() {

    return null;
  }

  public AnalysisEngineController getParentController() {

    return null;
  }

  public InputChannel getReplyInputChannel(String delegateKey) {

    return null;
  }

  public long getReplyTime() {

    return 0;
  }

  public String getServiceEndpointName() {

    return null;
  }

  public ServiceErrors getServiceErrors() {

    return null;
  }

  public ServiceInfo getServiceInfo() {

    return null;
  }

  public ServicePerformance getServicePerformance() {

    return null;
  }

  public Map getStats() {

    return null;
  }

  public long getTime(String casReferenceId, String anEndpointName) {

    return 0;
  }

  public long getTimeSnapshot(String key) {

    return 0;
  }

  public UimaTransport getTransport(UimaAsContext context, String key) throws Exception {

    return null;
  }

  public UimaTransport getTransport(String key) throws Exception {

    return null;
  }

  public UimaEEAdminContext getUimaEEAdminContext() {

    return null;
  }

  public UimaMessageListener getUimaMessageListener(String delegateKey) {

    return null;
  }

  public void handleDelegateLifeCycleEvent(String anEndpoint, int delegateCount) {

  }

  public void incrementDeserializationTime(long cpuTime) {

  }

  public void incrementSerializationTime(long cpuTime) {

  }

  public void initialize() throws AsynchAEException {

  }

  public void initializeVMTransport(int parentControllerReplyConsumerCount) throws Exception {

  }

  public boolean isAwaitingCacheCallbackNotification() {

    return false;
  }

  public boolean isCasMultiplier() {

    return false;
  }

  public boolean isPrimitive() {

    return false;
  }

  public boolean isStopped() {

    return false;
  }

  public boolean isTopLevelComponent() {

    return false;
  }

  public void notifyListenersWithInitializationStatus(Exception e) {

  }

  public void onInitialize() {

  }

  public void process(CAS cas, String casId) {

  }

  public void process(CAS acas, String anInputCasReferenceId, String newCasReferenceId,
          String newCASProducedBy) {

  }

  public void process(CAS acas, String casReferenceId, Endpoint anEndpoint) {

  }

  public abstract void quiesceAndStop();

  public void registerVmQueueWithJMX(Object o, String name) throws Exception {

  }

  public void releaseNextCas(String casReferenceId) {

  }

  public void saveReplyTime(long snapshot, String key) {

  }

  public void saveTime(long anArrivalTime, String casReferenceId, String anEndpointName) {

  }

  public void sendMetadata(Endpoint anEndpoint) throws AsynchAEException {

  }

  public void setCasManager(AsynchAECasManager casManager) {

  }

  public void setDeployDescriptor(String deployDescriptor) {

  }

  public void setInputChannel(InputChannel anInputChannel) throws Exception {

  }

  public void setOutputChannel(OutputChannel anOutputChannel) throws Exception {

  }

  public void setStopped() {

  }

  public void setUimaEEAdminContext(UimaEEAdminContext anAdminContext) {

  }

  public void stop() {

  }

  public void takeAction(String anAction, String anEndpointName, ErrorContext anErrorContext) {

  }

  public void addControllerCallbackListener(ControllerCallbackListener listener) {

  }

  public void removeControllerCallbackListener(ControllerCallbackListener listener) {

  }

  public void terminate() {

  }

  public void addEndpointToDoNotProcessList(String anEndpointName) {
  }

  public boolean isEndpointOnDontProcessList(String anEndpointName) {
    return false;
  }

  public void evictExpiredEntries() {

  }
  public void changeState(ServiceState state) {
  }
  
  public ServiceState getState() {
    return null;
  }
}
