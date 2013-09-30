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
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;

public interface AnalysisEngineController extends ControllerLifecycle {
  public static final String CasPoolSize = "CasPoolSize";

  public static final String AEInstanceCount = "AEInstanceCount";

  public void sendMetadata(Endpoint anEndpoint) throws AsynchAEException;

  public ControllerLatch getControllerLatch();

  public void setInputChannel(InputChannel anInputChannel) throws Exception;

  public void addInputChannel(InputChannel anInputChannel) throws Exception;

  public String getServiceEndpointName();

  public void handleDelegateLifeCycleEvent(String anEndpoint, int aDelegateCount);

  public void takeAction(String anAction, String anEndpointName, ErrorContext anErrorContext);

  public InputChannel getInputChannel();

  public InputChannel getInputChannel(String aQueueName);

  public void saveReplyTime(long snapshot, String aKey);

  public long getReplyTime();

  public Map getStats();

  public UimaContext getChildUimaContext(String aDelegateEndpointName) throws Exception;

  public void dropCAS(String aCasReferenceId, boolean dropCacheEntry);

  public void dropCAS(CAS aCAS);

  public InProcessCache getInProcessCache();

  public boolean isPrimitive();

  public Monitor getMonitor();

  public String getName();

  public String getComponentName();

  public void collectionProcessComplete(Endpoint anEndpoint) throws AsynchAEException;

  public boolean isTopLevelComponent();

  public void initialize() throws AsynchAEException;

  public void process(CAS aCas, String aCasId);// throws AnalysisEngineProcessException,
                                               // AsynchAEException;

  public void process(CAS aCAS, String anInputCasReferenceId, String aNewCasReferenceId,
          String newCASProducedBy); // throws AnalysisEngineProcessException, AsynchAEException;

  public void process(CAS aCAS, String aCasReferenceId, Endpoint anEndpoint);// throws
                                                                             // AnalysisEngineProcessException,
                                                                             // AsynchAEException;

  public void saveTime(long anArrivalTime, String aCasReferenceId, String anEndpointName);

  public long getTime(String aCasReferenceId, String anEndpointName);

  public ErrorHandlerChain getErrorHandlerChain();

  public void setOutputChannel(OutputChannel anOutputChannel) throws Exception;

  public OutputChannel getOutputChannel();

  public void setCasManager(AsynchAECasManager aCasManager);

  public AsynchAECasManager getCasManagerWrapper();

  public void stop();

  /**
   * Returns true if the AnalysisEngineController has been (or is in the process of) shutdown.
   * 
   * @return - true if stopped
   */
  public boolean isStopped();

  /**
   * Called to set the state of the AnalysisEngineController to STOPPED. This method does not stop
   * input or output channels.
   */
  public void setStopped();

  public void dropStats(String aCasReferenceId, String anEndpointName);

  public void setUimaEEAdminContext(UimaEEAdminContext anAdminContext);

  public UimaEEAdminContext getUimaEEAdminContext();

  public String getJMXDomain();

  public int getIndex();

  public String getJmxContext();

  public ServicePerformance getServicePerformance();

  public ServiceInfo getServiceInfo();

  public void addServiceInfo(ServiceInfo aServiceInfo);

  public ServiceErrors getServiceErrors();

  public void setDeployDescriptor(String aDeployDescriptor);

  public void cacheClientEndpoint(Endpoint anEndpoint);

  public Endpoint getClientEndpoint();

  public EventSubscriber getEventListener();

  public JmxManagement getManagementInterface();

  public void notifyListenersWithInitializationStatus(Exception e);

  public ServicePerformance getCasStatistics(String aCasReferenceId);

  public boolean isCasMultiplier();

  public void releaseNextCas(String aCasReferenceId);

  // public long getTotalIdleTime();
  public long getIdleTime();

  // This is called every time a request comes
  public void beginProcess(int msgType);

  // This is called every time a request is completed
  public void endProcess(int msgType);

  // Returns the idle time between process CAS calls
  public long getIdleTimeBetweenProcessCalls(int msgType);

  public long getCpuTime();

  public long getAnalysisTime();

  public void incrementSerializationTime(long cpuTime);

  public void incrementDeserializationTime(long cpuTime);

  public void onInitialize();

  public UimaMessageListener getUimaMessageListener(String aDelegateKey);

  public UimaTransport getTransport(UimaAsContext aContext, String aKey) throws Exception;

  public UimaTransport getTransport(String aKey) throws Exception;

  public void initializeVMTransport(int parentControllerReplyConsumerCount) throws Exception;

  public InputChannel getReplyInputChannel(String aDelegateKey);

  public LocalCache getLocalCache();

  public void registerVmQueueWithJMX(Object o, String aName) throws Exception;

  public AnalysisEngineController getParentController();

  public void addAbortedCasReferenceId(String aCasReferenceId);

  public boolean isAwaitingCacheCallbackNotification();

  public void quiesceAndStop();

  public void forceTimeoutOnPendingCases(String key);

  public void changeState(ServiceState state);
  
  public ServiceState getState();

  public Map<String,String> getDeadClientMap();

  public String getKey();
  
  public void dumpState(StringBuffer buffer, String lbl1);
  /**
   * Return UIMA AS process PID
   * @return
   */
  public String getPID();
}
