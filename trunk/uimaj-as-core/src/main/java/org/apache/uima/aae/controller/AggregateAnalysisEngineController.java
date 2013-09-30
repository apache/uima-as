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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.UimaContext;
import org.apache.uima.aae.InProcessCache;
import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.controller.BaseAnalysisEngineController.ServiceState;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.jmx.AggregateServiceInfo;
import org.apache.uima.aae.jmx.PrimitiveServiceInfo;
import org.apache.uima.aae.jmx.ServiceErrors;
import org.apache.uima.aae.jmx.ServiceInfo;
import org.apache.uima.aae.jmx.ServicePerformance;
import org.apache.uima.flow.FinalStep;

public interface AggregateAnalysisEngineController extends AnalysisEngineController {
  public void mergeTypeSystem(String aTypeSystem, String fromDestination) throws AsynchAEException;

  public void mergeTypeSystem(String aTypeSystem, String fromDestination, String fromServer)
          throws AsynchAEException;

  public void setRemoteSerializationSupported(int code, String fromDestination, String fromServer);
  
  public void sendRequestForMetadataToRemoteDelegates() throws AsynchAEException;

  public void addMessageOrigin(String aCasReferenceId, Endpoint anEndpoint);

  public Endpoint getMessageOrigin(String aCasReferenceId);

  public void removeMessageOrigin(String aCasReferenceId);

  public void processCollectionCompleteReplyFromDelegate(String aDelegateKey, boolean sendReply)
          throws AsynchAEException;

  public boolean isDelegateKeyValid(String anEndpointName);

  public void mapEndpointsToKeys(ConcurrentHashMap aDestinationMap);

  public Endpoint lookUpEndpoint(String anAnalysisEngineKey, boolean clone)
          throws AsynchAEException;

  public void dispatchMetadataRequest(Endpoint anEndpoint) throws AsynchAEException;

  public void retryLastCommand(int aCommand, Endpoint anEndpoint, String aCasReferenceId);

  public void retryMetadataRequest(Endpoint anEndpoint) throws AsynchAEException;

  public String lookUpDelegateKey(String aDelegateEndpointName);

  public String lookUpDelegateKey(String aDelegateEndpointName, String server);

  public UimaContext getChildUimaContext(String aDelegateEndpointName) throws Exception;

  // public void retryCAS( String aCasReferenceId, Endpoint anEndpoint )throws AsynchAEException;

  public void retryProcessCASRequest(String aCasReferenceId, Endpoint anEndpoint,
          boolean addEndpointToCache) throws AsynchAEException;

  public void enableDelegates(List aDelegateList) throws AsynchAEException;

  public void disableDelegates(List aDelegateList) throws AsynchAEException;

  public boolean continueOnError(String aCasReferenceId, String aDelegateKey, Exception anException)
          throws AsynchAEException;

  public void dropFlow(String aCasReferenceId, boolean dropFlow);

  public boolean isDelegateDisabled(String aDelegateKey);

  public String getLastDelegateKeyFromFlow(String anInputCasReferenceId);

  public void registerChildController(AnalysisEngineController aChildController, String aDelegateKey)
          throws Exception;

  public void saveStatsFromService(String aServiceEndpointName, Map aServiceStats);

  public Map getDelegateStats();

  public AggregateServiceInfo getServiceInfo();

  public ServicePerformance getDelegateServicePerformance(String aDelegateKey);

  public PrimitiveServiceInfo getDelegateServiceInfo(String aDelegateKey);

  public ServiceErrors getDelegateServiceErrors(String aDelegateKey);

  public void stopTimers();

  public boolean requestForMetaSentToRemotes();

  public void setRequestForMetaSentToRemotes();

  public Map getDestinations();

  public ServicePerformance getServicePerformance(String aDelegateKey);

  public void finalStep(FinalStep aStep, String aCasReferenceId);

  public Delegate lookupDelegate(String aDelegateKey);

  public boolean delayCasIfDelegateInTimedOutState(String aCasReferenceId, String aDelegateKey, long casHashcode)
          throws AsynchAEException;

  public List getChildControllerList();

  public void stopCasMultiplier(Delegate casMultiplier, String aCasReferenceId);

  public void setCasLoggingDirectory(String key, String directoryName);
  
  public void changeCollocatedDelegateState( String delegateKey, ServiceState state ) throws Exception;

  public int getServiceCasPoolSize();
}
