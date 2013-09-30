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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UIMARuntimeException;
import org.apache.uima.aae.AsynchAECasManager;
import org.apache.uima.aae.InProcessCache;
import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.AsynchAECasManager_impl;
import org.apache.uima.aae.InputChannel;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaClassFactory;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.delegate.ControllerDelegate;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.error.ErrorHandler;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.error.UimaEEServiceException;
import org.apache.uima.aae.error.UnknownDestinationException;
import org.apache.uima.aae.jmx.AggregateServiceInfo;
import org.apache.uima.aae.jmx.JmxManagement;
import org.apache.uima.aae.jmx.PrimitiveServiceInfo;
import org.apache.uima.aae.jmx.ServiceErrors;
import org.apache.uima.aae.jmx.ServiceInfo;
import org.apache.uima.aae.jmx.ServicePerformance;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.monitor.Monitor;
import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.aae.monitor.statistics.Statistic;
import org.apache.uima.aae.spi.transport.UimaMessage;
import org.apache.uima.aae.spi.transport.UimaTransport;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.asb.impl.FlowContainer;
import org.apache.uima.analysis_engine.asb.impl.FlowControllerContainer;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.admin.CASFactory;
import org.apache.uima.cas.admin.CASMgr;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.TypeSystemImpl;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.flow.FinalStep;
import org.apache.uima.flow.ParallelStep;
import org.apache.uima.flow.SimpleStep;
import org.apache.uima.flow.Step;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.Level;
import org.apache.uima.util.TypeSystemUtil;
import org.apache.uima.util.XMLInputSource;

public class AggregateAnalysisEngineController_impl extends BaseAnalysisEngineController implements
        AggregateAnalysisEngineController, AggregateAnalysisEngineController_implMBean {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private static final Class CLASS_NAME = AggregateAnalysisEngineController_impl.class;

  private static final int SERVICE_INFO_INDX = 0;

  private static final int SERVICE_PERF_INDX = 1;

  private static final int SERVICE_ERROR_INDX = 2;

  private ConcurrentHashMap<String, FlowContainer> flowMap = new ConcurrentHashMap<String, FlowContainer>();

  private ConcurrentHashMap<String, Endpoint> destinationMap;

  private Map<String, String> destinationToKeyMap;

  private volatile boolean typeSystemsMerged = false;

  private AnalysisEngineMetaData aggregateMetadata;

  private HashMap<String, ProcessingResourceMetaData> analysisEngineMetaDataMap = new HashMap<String, ProcessingResourceMetaData>();
  
  private List disabledDelegateList = new ArrayList();

  private List remoteCasMultiplierList = new ArrayList();

  private String descriptor;

  private transient FlowControllerContainer flowControllerContainer;

  private String flowControllerDescriptor;

  private ConcurrentHashMap<String, Endpoint> originMap = new ConcurrentHashMap<String, Endpoint>();

  private String controllerBeanName = null;

  private String serviceEndpointName = null;

  protected volatile boolean initialized = false;

  protected List<AnalysisEngineController> childControllerList = new ArrayList<AnalysisEngineController>();

  private ConcurrentHashMap delegateStats = new ConcurrentHashMap();

  private AggregateServiceInfo serviceInfo = null;

  private int remoteIndex = 1;

  private volatile boolean requestForMetaSentToRemotes = false;

  private ConcurrentHashMap<String, Object[]> delegateStatMap = new ConcurrentHashMap<String, Object[]>();

  public final Object parallelStepMux = new Object();

  // prevents more than one thread to call collectionProcessComplete on the FC
  private volatile boolean doSendCpcReply = false;
  //	Guards FC's next() method. Single thread access is allowed at any given time
  private Semaphore flowSemaphore = new Semaphore(1);

  // Map of components registered for Cas logging. Value indicates that typesystem.xml has been created.
  private HashMap<String, Boolean> enableCasLogMap; // = new HashMap<String, Boolean>();
  // Map of log directory names per component
  private HashMap<String, String> casLogDirMap; // = new HashMap<String, String>();
  // Base time used for default XmiCas file names.
  private Long initializationTime = System.nanoTime();
  
  // Used by async aggregate to throttle CAS ingestion from an input queue. The 
  // semaphore is lazily created on the first Process request and is assigned
  // the number of permits equal to the size of CasPool configured for this
  // service. A permit is acquired right after process() is called which means
  // that the CAS has been handed off to another async delegate. The permit is
  // released when the CAS is fully processed and reply sent to a client. This
  // allows the async aggregate to concurrently process as many CASes as there 
  // are CASes in a CasPool. It also ensures that this aggregate doesnt try
  // to ingest more CASes as it is currently capable of processing without waiting
  // for a free CAS from the CasPool.
  public Semaphore semaphore = null;
  
  /**
   * 
   * @param anEndpointName
   * @param aDescriptor
   * @param aCasManager
   * @param anInProcessCache
   * @param aDestinationMap
   * @throws Exception
   */
  public AggregateAnalysisEngineController_impl(String anEndpointName, String aDescriptor,
          AsynchAECasManager aCasManager, InProcessCache anInProcessCache, Map aDestinationMap)
          throws Exception {
    this(null, anEndpointName, aDescriptor, aCasManager, anInProcessCache, aDestinationMap);
  }

  /**
   * 
   * 
   * @param aParentController
   * @param anEndpointName
   * @param aDescriptor
   * @param aCasManager
   * @param anInProcessCache
   * @param aDestinationMap
   * @throws Exception
   */
  public AggregateAnalysisEngineController_impl(AnalysisEngineController aParentController,
          String anEndpointName, String aDescriptor, AsynchAECasManager aCasManager,
          InProcessCache anInProcessCache, Map aDestinationMap) throws Exception {
    this(aParentController, anEndpointName, aDescriptor, aCasManager, anInProcessCache,
            aDestinationMap, null);
  }

  public AggregateAnalysisEngineController_impl(AnalysisEngineController aParentController,
          String anEndpointName, String aDescriptor, AsynchAECasManager aCasManager,
          InProcessCache anInProcessCache, Map aDestinationMap, JmxManagement aJmxManagement)
          throws Exception {
    super(aParentController, 0, anEndpointName, aDescriptor, aCasManager, anInProcessCache,
            aDestinationMap, aJmxManagement);
    this.initialize();
  }

  public void registerChildController(AnalysisEngineController aChildController, String aDelegateKey)
          throws Exception {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
              "registerChildController", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_register_controller__FINE",
              new Object[] { getComponentName(), aChildController.getComponentName() });
    }
    synchronized(childControllerList) {
      childControllerList.add(aChildController);
    }
  }

  public void saveStatsFromService(String aServiceEndpointName, Map aServiceStats) {
    String delegateKey = lookUpDelegateKey(aServiceEndpointName);
    delegateStats.put(delegateKey, aServiceStats);
  }

  /**
	 * 
	 */
  public void addMessageOrigin(String aCasReferenceId, Endpoint anEndpoint) {
    if (anEndpoint == null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, getClass().getName(), "addMessageOrigin",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_endpoint_null__INFO",
                  new Object[] { getComponentName(), aCasReferenceId});
      }
      return;
    }
    originMap.put(aCasReferenceId, anEndpoint);
    if (UIMAFramework.getLogger().isLoggable(Level.FINE)) {
      Iterator it = originMap.keySet().iterator();
      StringBuffer sb = new StringBuffer();
      while (it.hasNext()) {
        String key = (String) it.next();
        Endpoint e = (Endpoint) originMap.get(key);
        if (e != null) {
          sb.append("\t\nCAS:" + key + " Origin:" + e.getEndpoint());
        }
      }
      /*
       * UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
       * "addMessageOrigin", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
       * "UIMAEE_dump_msg_origin__FINE", new Object[] {getComponentName(), sb.toString()});
       */
    }
  }

  public boolean isDelegateDisabled(String aDelegateKey) {
    if (aDelegateKey == null) {
      return false;
    }
    Iterator it = disabledDelegateList.iterator();
    while (it.hasNext()) {
      if (aDelegateKey.equalsIgnoreCase((String) it.next())) {
        return true;
      }
    }
    return false;
  }

  /**
   * 
   * @param anEndpointName
   */
  public void setServiceEndpointName(String anEndpointName) {
    serviceEndpointName = anEndpointName;
    if (this.isTopLevelComponent()) {
      // This is done so that the collocated client application can determine where to send messages
      System.setProperty("ServiceInputQueueName", serviceEndpointName);
    }
  }

  /**
	 * 
	 */
  public String getServiceEndpointName() {
    return serviceEndpointName;
  }

  /**
   * 
   * @param aBeanName
   */
  public void setControllerBeanName(String aBeanName) {
    controllerBeanName = aBeanName;
    if (this.isTopLevelComponent()) {
      System.setProperty("Controller", controllerBeanName);
    }
  }

  /**
	 * 
	 */
  public Endpoint getMessageOrigin(String aCasReferenceId) {
      if (originMap.containsKey(aCasReferenceId)) {
        return (Endpoint) originMap.get(aCasReferenceId);
    }
    return null;
  }

  public void removeMessageOrigin(String aCasReferenceId) {
      if (originMap.containsKey(aCasReferenceId)) {
        originMap.remove(aCasReferenceId);
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "removeMessageOrigin", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_remove_msg_origin_entry__FINEST",
                  new Object[] { getComponentName(), aCasReferenceId });
        }
      }
  }

  /**
	 * 
	 */
  public void dropCAS(String aCasReferenceId, boolean dropCacheEntry) {

    FlowContainer flow = lookupFlow(aCasReferenceId);
    if (flow != null) {
        flowMap.remove(aCasReferenceId);
    }
    super.dropCAS(aCasReferenceId, dropCacheEntry);
  }

  public void dropFlow(String aCasReferenceId, boolean abortFlow) {
    FlowContainer flow = lookupFlow(aCasReferenceId);
    if (flow != null) {
      if (abortFlow) {
        synchronized (flowControllerContainer) {
          flow.aborted();
        }
      }

    flowMap.remove(aCasReferenceId);
    }

  }

  /**
	 * 
	 */
  public void mapEndpointsToKeys(ConcurrentHashMap aDestinationMap) {
    destinationMap = aDestinationMap;
    Set set = destinationMap.entrySet();
    for (Iterator it = set.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      Endpoint endpoint = (Endpoint) entry.getValue();
      if (endpoint != null) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                  "mapEndpointsToKeys", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_endpoint_to_key_map__FINE",
                  new Object[] { getName(), (String) entry.getKey(), endpoint.getEndpoint() });
        }
        if (destinationToKeyMap == null) {
          destinationToKeyMap = new HashMap();
        }
        // Create and initialize a Delegate object for the endpoint
        Delegate delegate = new ControllerDelegate((String) entry.getKey(), this);
        delegate.setCasProcessTimeout(endpoint.getProcessRequestTimeout());
        delegate.setGetMetaTimeout(endpoint.getMetadataRequestTimeout());
        delegate.setEndpoint(endpoint);
        // Add new delegate to the global Delegate list
        delegates.add(delegate);
        endpoint.setDelegateKey((String) entry.getKey());
        destinationToKeyMap.put(endpoint.getEndpoint(), (String) entry.getKey());
      }
    }

  }

  /**
   * Change the CPC status for each delegate in the destination Map.
   */
  private void resetEndpointsCpcStatus() {
    Set set = destinationMap.entrySet();
    for (Iterator it = set.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      Endpoint endpoint = (Endpoint) entry.getValue();
      if (endpoint != null && endpoint.getStatus() == Endpoint.OK) {
        endpoint.setCompletedProcessingCollection(false);
      }
    }
  }

  /**
   * 
   * @return
   */
  private synchronized boolean allDelegatesCompletedCollection() {
    Set set = destinationMap.entrySet();
    for (Iterator it = set.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      Endpoint endpoint = (Endpoint) entry.getValue();
      if (endpoint != null && endpoint.getStatus() == Endpoint.OK
              && endpoint.completedProcessingCollection() == false) {
        return false;
      }
    }
    // All delegates replied to CPC, change the status of each delegate
    // to handle next CPC request.
    resetEndpointsCpcStatus();
    return true;
  }

  public Map getDelegateStats() {
    return delegateStats;
  }

  /**
	 * 
	 */
  public void processCollectionCompleteReplyFromDelegate(String aDelegateKey, boolean sendReply)
          throws AsynchAEException {

    try {
      Endpoint endpoint = (Endpoint) destinationMap.get(aDelegateKey);
      String key = lookUpDelegateKey(aDelegateKey);
      if (endpoint == null) {
        endpoint = (Endpoint) destinationMap.get(key);
        if (endpoint == null) {
          throw new AsynchAEException("Unable to find Endpoint Object Using:" + aDelegateKey);
        }
      }
      endpoint.cancelTimer();
      endpoint.setCompletedProcessingCollection(true);

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                "processCollectionCompleteReplyFromDelegate",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_recvd_cpc_reply__FINE",
                new Object[] { key });
      }
      Endpoint cEndpoint = null;
      // synchronized to prevent more than one thread to call collectionProcessComplete() on
      // the Flow Controller.
      if (flowControllerContainer != null) {
        synchronized (flowControllerContainer) {
          if (doSendCpcReply == false && sendReply && allDelegatesCompletedCollection()
                && ((cEndpoint = getClientEndpoint()) != null)) {
            doSendCpcReply = true;
            flowControllerContainer.collectionProcessComplete();
          }
        }
      }
      // Reply to a client once for each CPC request. doSendCpcReply is volatile thus
      // no need to synchronize it
      if (doSendCpcReply) {
        sendCpcReply(cEndpoint);
        doSendCpcReply = false; // reset for the next CPC
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  private void sendCpcReply(Endpoint aClientEndpoint) throws Exception {
    Iterator destIterator = destinationMap.keySet().iterator();
    while (destIterator.hasNext()) {

      String key = (String) destIterator.next();
      Endpoint endpoint = (Endpoint) destinationMap.get(key);
      if (endpoint != null) {
        endpoint.setCompletedProcessingCollection(false); // reset for the next run
      }
      logStats(key, getDelegateServicePerformance(key));

    }
    // Log this controller's stats
    logStats(getComponentName(), servicePerformance);

    endProcess(AsynchAEMessage.Process);
    if (aClientEndpoint == null) {
      aClientEndpoint = getClientEndpoint();
    }
    if (!aClientEndpoint.isRemote()) {
      UimaTransport transport = getTransport(aClientEndpoint.getEndpoint());
      UimaMessage message = transport.produceMessage(AsynchAEMessage.CollectionProcessComplete,
              AsynchAEMessage.Response, getName());
      // Send reply back to the client. Use internal (non-jms) transport
      transport.getUimaMessageDispatcher(aClientEndpoint.getEndpoint()).dispatch(message);
    } else {
      getOutputChannel().sendReply(AsynchAEMessage.CollectionProcessComplete, aClientEndpoint, null, false);
    }

    clearStats();
  }

  /**
   * 
   * @param aFlowControllerDescriptor
   */
  public synchronized void setFlowControllerDescriptor(String aFlowControllerDescriptor) {
    flowControllerDescriptor = aFlowControllerDescriptor;
  }

  /**
   * 
   * @param anEndpoint
   * @throws AsynchAEException
   */
  private void waitUntilAllCasesAreProcessed(Endpoint anEndpoint) throws AsynchAEException {
    try {
      boolean cacheNotEmpty = true;
      boolean shownOnce = false;
      final Object localMux = new Object();
      while (cacheNotEmpty) {
        InProcessCache cache = getInProcessCache();
        if (!shownOnce) {
          shownOnce = true;
          cache.dumpContents(getComponentName());
        }

        if (cache.isEmpty()) {
          cacheNotEmpty = false;
        } else {
          synchronized (localMux) {
            localMux.wait(10);
          }
        }
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  /**
	 * 
	 */
  public void takeAction(String anAction, String anEndpointName, ErrorContext anErrorContext) {
    try {
      handleAction(anAction, anEndpointName, anErrorContext);
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "takeAction", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getComponentName());
        
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "takeAction", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }
  }

  public void collectionProcessComplete(Endpoint anEndpoint) throws AsynchAEException {

    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
              "collectionProcessComplete", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_cpc__FINEST", new Object[] { getName() });
    }
    getInProcessCache().dumpContents(getComponentName());
    localCache.dumpContents();

    cacheClientEndpoint(anEndpoint);

    // Wait until the entire cache is empty. The cache stores in-process CASes.
    // When a CAS is processed completly it is removed from the cache.
    waitUntilAllCasesAreProcessed(anEndpoint);

    anEndpoint.setCommand(AsynchAEMessage.CollectionProcessComplete);

    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
              "collectionProcessComplete", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_cpc_all_cases_processed__FINEST", new Object[] { getName() });
    }
    // Special case. Check if ALL delegates have been disabled. If so, destinationMap
    // will be empty.
    if (destinationMap.size() == 0) {
      try {
        sendCpcReply(null);
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "collectionProcessComplete", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getComponentName());

          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                  "collectionProcessComplete", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", e);
        }
      }
    } else {
      Set set = destinationMap.entrySet();
      for (Iterator it = set.iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        Endpoint endpoint = (Endpoint) entry.getValue();
        if (endpoint != null && endpoint.getStatus() == Endpoint.OK) {

          if (!endpoint.isRemote()) {
            try {
              UimaTransport transport = getTransport(endpoint.getEndpoint());
              UimaMessage message = transport
                      .produceMessage(AsynchAEMessage.CollectionProcessComplete,
                              AsynchAEMessage.Request, getName());
              // Send reply back to the client. Use internal (non-jms) transport
              transport.getUimaMessageDispatcher(endpoint.getEndpoint()).dispatch(message);
            } catch (Exception e) {
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                        "collectionProcessComplete", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_service_exception_WARNING", getComponentName());

                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                        "collectionProcessComplete", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_exception__WARNING", e);
              }
            }
          } else {
            getOutputChannel().sendRequest(AsynchAEMessage.CollectionProcessComplete, null, endpoint);
            endpoint.startCollectionProcessCompleteTimer();
          }
        }
      }
    }
  }

  public String getDescriptor() {
    return descriptor;
  }

  public void setDescriptor(String descriptor) {
    this.descriptor = descriptor;
  }

  public boolean isPrimitive() {
    return false;
  }

  public Map getDestinations() {
    return destinationMap;
  }

  public void enableDelegates(List aDelegateList) throws AsynchAEException {
    try {
      // flowControllerContainer.addAnalysisEngines(aDelegateList);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }

  }

  public void handleInitializationError(Exception ex) {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
              "handleInitializationError", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_service_exception_WARNING", getComponentName());

      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
              "handleInitializationError", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_exception__WARNING", ex);
    }
    // Any problems in completeInitialization() is a reason to stop
    notifyListenersWithInitializationStatus(ex);
    super.stop(true);   // shutdown now
  }

  private void stopListener(String key, Endpoint endpoint) throws Exception {
    // Stop the Listener on endpoint that has been disabled
    InputChannel iC = null;
    String destName = null;
    if (endpoint.getDestination() != null) {
      destName = endpoint.getDestination().toString();
      iC = getInputChannel(destName);
    } else {
      destName = endpoint.getReplyToEndpoint();
      iC = getInputChannel(destName);
    }
    if (iC != null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, getClass().getName(), "stopListener",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_stopping_listener__INFO",
                new Object[] { getComponentName(), destName, key });
      }
      iC.destroyListener(destName, key);
    }
  }

  public void disableDelegates(List aDelegateList) throws AsynchAEException {
    disableDelegates(aDelegateList, null);
  }

  protected void disableDelegates(List aDelegateList, String aCasReferenceId)
          throws AsynchAEException {
    if (aDelegateList == null) {
      throw new AsynchAEException("Controller:" + getComponentName()
              + " Unable To Disable a Delegate. The Delegate List Provided Is Invalid (Null)");
    }
    try {
      Iterator it = aDelegateList.iterator();
      while (it.hasNext()) {
        String key = (String) it.next();
        Endpoint endpoint = lookUpEndpoint(key, false);
        // if the the current delegate is remote, destroy its listener
        if (endpoint != null && endpoint.isRemote()) {
          Delegate delegate = lookupDelegate(key);
          if (delegate != null) {
            delegate.cancelDelegateTimer();
          }
          stopListener(key, endpoint);
          endpoint.setStatus(Endpoint.DISABLED);
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, getClass().getName(),
                  "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_disable_endpoint__INFO", new Object[] { getComponentName(), key });
        }
        // Change state of the delegate
        ServiceInfo sf = getDelegateServiceInfo(key);
        if (sf != null) {
          sf.setState(ServiceState.DISABLED.name());
        }
        synchronized (disabledDelegateList) {
          disabledDelegateList.add(key);
        }

      }
      if (flowControllerContainer != null) {
        try {
          synchronized (flowControllerContainer) {
            flowControllerContainer.removeAnalysisEngines(aDelegateList);
          }
        } catch (Exception ex) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_service_exception_WARNING", getComponentName());

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", ex);
          }
          if (aCasReferenceId != null) {
            CasStateEntry parentCasCacheEntry = getLocalCache().getTopCasAncestor(aCasReferenceId);
            if (parentCasCacheEntry != null && aDelegateList.size() > 0) {
              String delegateKey = (String) aDelegateList.get(0);
              
              
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                          "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_service_terminating_fc_failure__INFO",
                          new Object[] { getComponentName(), delegateKey, parentCasCacheEntry.getCasReferenceId() });
              }
              super.terminate(ex, parentCasCacheEntry.getCasReferenceId());
            } else {
              terminate();
            }
          } else {
            terminate();
          }
          return;
        }
      }
      if (!initialized && allTypeSystemsMerged()) {
        try {
          completeInitialization();
        } catch (ResourceInitializationException ex) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_service_exception_WARNING", getComponentName());

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", ex);
          }
          handleInitializationError(ex);
          return;
        }
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "disableDelegates", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
      throw new AsynchAEException(e);
    }

  }

  public boolean continueOnError(String aCasReferenceId, String aDelegateKey, Exception anException)
          throws AsynchAEException {
    if (aDelegateKey == null || aCasReferenceId == null) {
      return false;
    }
    try {
      FlowContainer flow = lookupFlow(aCasReferenceId);
      if (flow == null) {
        return false;
      }
      synchronized (flowControllerContainer) {
        return flow.continueOnFailure(aDelegateKey, anException);
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  private FlowContainer lookupFlow(String aCasReferenceId) {
    if (flowMap != null) {
        if (flowMap.containsKey(aCasReferenceId)) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                    "lookupFlow", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_retrieve_flow_object__FINEST",
                    new Object[] { getComponentName(), aCasReferenceId });
          }
          return (FlowContainer) flowMap.get(aCasReferenceId);
      }
    }
    return null;
  }

  public String getLastDelegateKeyFromFlow(String anInputCasReferenceId) {
    return null;
  }

  /**
   * This routine is called to handle CASes produced by a CAS Multiplier. A new CAS needs a flow
   * object which is produced here from the Flow associated with the input CAS. Once the subflow is
   * computed, it is cached for future use.
   * 
   * @param aCAS
   *          - CAS to process
   * @param anInputCasReferenceId
   *          - reference id of the input CAS
   * @param aNewCasReferenceId
   *          - reference id of the CAS created by the CAS multiplier
   * @param newCASProducedBy
   *          - name of the multiplier that created the CAS
   * @throws AnalysisEngineProcessException
   * @throws AsynchAEException
   */
  public void process(CAS aCAS, String anInputCasReferenceId, String aNewCasReferenceId,
          String newCASProducedBy) // throws AnalysisEngineProcessException, AsynchAEException
  {
    FlowContainer flow = null;

    try {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "process",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_lookup_flow__FINE",
                new Object[] { getComponentName(), anInputCasReferenceId });
      }
      try {
          // Lookup a Flow object associated with an input CAS.
          if (flowMap.containsKey(anInputCasReferenceId)) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                      "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_retrieve_flow_object__FINEST",
                      new Object[] { getComponentName(), anInputCasReferenceId });
            }
            // Retrieve an input CAS Flow object from the flow cache. This Flow object will be used
            // to compute
            // subordinate Flow for the new CAS.
            flow = (FlowContainer) flowMap.get(anInputCasReferenceId);
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                      "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_retrieved_flow_object_ok__FINEST",
                      new Object[] { getComponentName(), anInputCasReferenceId });
            }

          }
        if (flow != null) {

          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINE,
                    CLASS_NAME.getName(),
                    "process",
                    UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_lookup_flow_ok__FINE",
                    new Object[] { getComponentName(), aNewCasReferenceId, newCASProducedBy,
                        anInputCasReferenceId, });
          }
          // Compute subordinate Flow from the Flow associated with the
          // input CAS.
          synchronized (flowControllerContainer) {
            flow = flow.newCasProduced(aCAS, newCASProducedBy);
          }
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINE,
                    CLASS_NAME.getName(),
                    "process",
                    UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_new_flow_ok__FINE",
                    new Object[] { getComponentName(), aNewCasReferenceId, newCASProducedBy,
                        anInputCasReferenceId, });
          }
          // Check if the local cache already contains an entry for the Cas id.
          // A colocated Cas Multiplier may have already registered this CAS
          // in the parent's controller
          if (localCache.lookupEntry(aNewCasReferenceId) == null) {
            // Add this Cas Id to the local cache. Every input CAS goes through here
            CasStateEntry casStateEntry = localCache.createCasStateEntry(aNewCasReferenceId);
            casStateEntry.setInputCasReferenceId(anInputCasReferenceId);
          }

          // Save the subordinate Flow Object in a cache. Flow exists in the
          // cache until the CAS is fully processed or it is
          // explicitly deleted when processing of this CAS cannot continue
          flowMap.put(aNewCasReferenceId, flow);
          // Register the fact that this is a new CAS and the fact that is was produced
          // by this aggregate. It is important to register this to determine how to
          // handle the CAS in delegate Aggregate services. When the CAS is processed
          // in the Delegate Aggregate, the CAS produced in the parent Aggregate cannot
          // be dropped in the delegate. Check Final Step logic.
          getInProcessCache().getCacheEntryForCAS(aNewCasReferenceId).setNewCas(true,
                  getComponentName());
          getLocalCache().lookupEntry(anInputCasReferenceId).decrementOutstandingFlowCounter();
        } else {
          throw new AsynchAEException(
                  "Flow Object Not In Flow Cache. Expected Flow Object in FlowCache for Cas Reference Id:"
                          + anInputCasReferenceId);
        }

      } catch (Throwable t) {
        // Any error here is automatic termination
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getComponentName());

          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "process",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_exception__WARNING",
                  t);
        }
        sendReplyWithShutdownException(anInputCasReferenceId);
        handleAction(ErrorHandler.TERMINATE, null, null);
        return;

      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "process",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_executing_step__FINEST",
                new Object[] { getComponentName(), aNewCasReferenceId, newCASProducedBy,
                    anInputCasReferenceId, });
      }

      // Continue with Steps. The CAS has been produced by the CAS Multiplier
      executeFlowStep(flow, aNewCasReferenceId, true);

    } catch (Exception e) {
      HashMap map = new HashMap();
      map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
      handleError(map, e);
    }

  }

  private void sendReplyWithShutdownException(String aCasReferenceId) {
    try {
      CasStateEntry casStateEntry = localCache.createCasStateEntry(aCasReferenceId);
      CacheEntry cacheEntry = getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
      Endpoint replyEndpoint = getReplyEndpoint(cacheEntry, casStateEntry);
      if (replyEndpoint != null) {
        getOutputChannel().sendReply(new ServiceShutdownException(), aCasReferenceId, null,
                replyEndpoint, AsynchAEMessage.Process);
      }
    } catch (Exception ex) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "sendReply",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_exception__WARNING",
                ex);
      }
    }
  }

  private boolean abortProcessingCas(CasStateEntry casStateEntry, CacheEntry entry) {
    CasStateEntry parentCasStateEntry = null;
    try {
      // Check if this CAS has a parent
      if (casStateEntry.isSubordinate()) {
        // Fetch parent's cache entry
        parentCasStateEntry = getLocalCache().lookupEntry(casStateEntry.getInputCasReferenceId());
        // Check the state of the parent CAS. If it is marked as failed, it means that
        // one of its child CASes failed and error handling was configured to fail the
        // CAS. Such failure of a child CAS causes a failure of the parent CAS. All child
        // CASes will be dropped in finalStep() as they come back from delegates. When all are
        // accounted for and dropped, the parent CAS will be returned back to the client
        // with an exception.
        if (parentCasStateEntry.isFailed()) {
          // Fetch Delegate object for the CM that produced the CAS. The producer key
          // is associated with a cache entry in the ProcessRequestHandler. Each new CAS
          // must have a key of a CM that produced it.
          Delegate delegateCM = lookupDelegate(entry.getCasProducerKey());
          if (delegateCM != null && delegateCM.getEndpoint().isCasMultiplier()) {
            // If the delegate CM is a remote, send a Free CAS notification
            if (delegateCM.getEndpoint().isRemote()) {
              parentCasStateEntry.getFreeCasNotificationEndpoint().setCommand(AsynchAEMessage.Stop);
              Endpoint fcEndpoint = parentCasStateEntry.getFreeCasNotificationEndpoint();
              fcEndpoint.setReplyEndpoint(true);
              fcEndpoint.setIsCasMultiplier(true);
              fcEndpoint.setFreeCasEndpoint(true);
              getOutputChannel().sendRequest(AsynchAEMessage.ReleaseCAS, entry.getCasReferenceId(),
                      fcEndpoint);
            }
            // Check if a request to stop generation of new CASes from the parent of
            // this CAS has been sent to the CM. The Delegate object keeps track of
            // requests to STOP that are sent to the CM. Only one STOP is needed.
            if (delegateCM.isGeneratingChildrenFrom(parentCasStateEntry.getCasReferenceId())) {
              // Issue a request to the CM to stop producing new CASes from a given input
              // CAS
              stopCasMultiplier(delegateCM, parentCasStateEntry.getCasReferenceId());
            }
          }
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINE,
                    CLASS_NAME.getName(),
                    "abortProcessingCas",
                    UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_forcing_cas_to_finalstep__FINE",
                    new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                        casStateEntry.getSubordinateCasInPlayCount() });
          }
          casStateEntry.setReplyReceived();
          // Force the CAS to go to the Final Step where it will be dropped
          finalStep(new FinalStep(), casStateEntry.getCasReferenceId());
          return true; // Done here
        }
      } else if (casStateEntry.isFailed()) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "abortProcessingCas",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_forcing_cas_to_finalstep__FINE",
                  new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                      casStateEntry.getSubordinateCasInPlayCount() });
        }
        casStateEntry.setReplyReceived();
        // move this CAS to the final step
        finalStep(new FinalStep(), casStateEntry.getCasReferenceId());
        return true;
      }

    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "abortProcessingCas", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "abortProcessingCas", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }

    return false;
  }

  /**
   * This is a process method that is executed for CASes not created by a Multiplier in this
   * aggregate.
   * 
   */
  public void process(CAS aCAS, String aCasReferenceId) {
    boolean handlingDelayedStep = false;
    // First check if there are outstanding steps to be called before consulting the Flow
    // Controller.
    // This could be the case if a previous step was a parallel step and it contained collocated
    // delegates.
    if (!isStopped()) {
      if (abortGeneratingCASes(aCasReferenceId)) {
        // Force delegate Cas Multipliers to Stop generating new CASes
        super.stopCasMultipliers();
      }
      try {
        CacheEntry entry = getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
        CasStateEntry casStateEntry = getLocalCache().lookupEntry(aCasReferenceId);
        // Check if this CAS should be aborted due to previous error on this CAS or its
        // parent. If this is the case the method will move the CAS to the final state
        // where it will be dropped. If the CAS is an input CAS, it will be returned to
        // the client with an exception
        if (abortProcessingCas(casStateEntry, entry)) {
          // This CAS was aborted, we are done here
          return;
        }
        // Check if this is an input CAS from the client. If not, check if last
        // delegate handling this CAS was a Cas Multiplier configured to process
        // parent CAS last
        if (casStateEntry.getLastDelegate() != null) {
          // Fetch the endpoint corresponding to the last Delegate handling the CAS
          Endpoint lastDelegateEndpoint = casStateEntry.getLastDelegate().getEndpoint();
          // Check if this delegate is a Cas Multiplier and the parent CAS is to be processed last
          casStateEntry.setReplyReceived();
          if (lastDelegateEndpoint.isCasMultiplier()){
            //  The following blocks until all child CASes acquire their Flow objects from the Flow
            //  Controller. Release the semaphore immediately after acquiring it. This semaphore is 
            //  no longer needed. This synchronization is only necessary for blocking the parent
            //  CAS until all child CASes acquire their Flow objects.
            casStateEntry.acquireFlowSemaphore();
            casStateEntry.releaseFlowSemaphore();
            if ( lastDelegateEndpoint.processParentLast()) {
              synchronized (super.finalStepMux) {
                // Determine if the CAS should be held until all its children leave this aggregate.
                if (casStateEntry.getSubordinateCasInPlayCount() > 0) {
                  // This input CAS has child CASes still in play. It will remain in the cache
                  // until the last of the child CASes is released. Only than, the input CAS is
                  // is allowed to continue into the next step in the flow.
                  // The CAS has to be in final state
                  casStateEntry.setState(CacheEntry.FINAL_STATE);
                  // The input CAS will be interned until all children leave this aggregate
                  return;
                }
              }
              
            }
          }
        }
        // if we are here entry is not null. The above throws an exception if an entry is not
        // found in the cache. First check if there is a delayedSingleStepList in the cache.
        // If there is one, it means that a parallel step contained collocated delegate(s)
        // The parallel step may only contain remote delegates. All collocated delegates
        // were removed from the parallel step and added to the delayedSingleStepList in
        // parallelStep() method.
        List delayedSingleStepList = entry.getDelayedSingleStepList();
        if (delayedSingleStepList != null && delayedSingleStepList.size() > 0) {
          handlingDelayedStep = true;
          // Reset number of parallel delegates back to one. This is done only if the previous step
          // was a parallel step.
          synchronized (parallelStepMux) {
            if (casStateEntry.getNumberOfParallelDelegates() > 1) {
              casStateEntry.setNumberOfParallelDelegates(1);
            }
          }
          // Remove a delegate endpoint from the single step list cached in the CAS entry
          Endpoint endpoint = (Endpoint_impl) entry.getDelayedSingleStepList().remove(0);
          // send the CAS to a collocated delegate from the delayed single step list.
          dispatchProcessRequest(entry, endpoint, true);
        }
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getComponentName());

          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "process",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_exception__WARNING",
                  e);
        }
      } finally {
        // If just handled the delayed step, return as there is nothing else to do
        if (handlingDelayedStep) {
          return;
        }
      }
    }

    FlowContainer flow = null;
    try {
      if (aCasReferenceId != null) {
        try {
          // Check if a Flow object has been previously generated for the Cas.
          if (flowMap.containsKey(aCasReferenceId)) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                      "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_retrieve_flow_object__FINEST",
                      new Object[] { getComponentName(), aCasReferenceId });
            }
            flow = (FlowContainer) flowMap.get(aCasReferenceId);
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                      "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_retrieved_flow_object_ok__FINEST",
                      new Object[] { getComponentName(), aCasReferenceId });
            }
          } else {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                      "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_new_flow_object__FINEST", new Object[] { aCasReferenceId });
            }
            synchronized (flowControllerContainer) {
              flow = flowControllerContainer.computeFlow(aCAS);
            }
            // Save the Flow Object in a cache. Flow exists in the cache
            // until the CAS is fully processed or it is
            // explicitly deleted when processing of this CAS cannot
            // continue
            flowMap.put(aCasReferenceId, flow);
            // Check if the local cache already contains an entry for the Cas id.
            // A colocated Cas Multiplier may have already registered this CAS
            // in the parent's controller
            if (localCache.lookupEntry(aCasReferenceId) == null) {
              // Add this Cas Id to the local cache. Every input CAS goes through here
              localCache.createCasStateEntry(aCasReferenceId);
            }
          }
        } catch (Exception ex) {
          // Any error here is automatic termination
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_service_exception_WARNING", getComponentName());

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", ex);
          }
          sendReplyWithShutdownException(aCasReferenceId);

          handleAction(ErrorHandler.TERMINATE, null, null);
          return;
        }
        if (!isStopped()) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                    "process", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_executing_step_input_cas__FINEST",
                    new Object[] { getComponentName(), aCasReferenceId });
          }
          // Execute a step in the flow. false means that this CAS has not
          // been produced by CAS Multiplier
          executeFlowStep(flow, aCasReferenceId, false);
        } else {
          synchronized (flowControllerContainer) {
            flow.aborted();
          }
        }
      }
    } catch (Exception e) {
      HashMap map = new HashMap();
      map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
      map.put(AsynchAEMessage.CasReference, aCasReferenceId);
      handleError(map, e);
    }
  }

  private void simpleStep(SimpleStep aStep, String aCasReferenceId)// throws AsynchAEException
  {
    Endpoint endpoint = null;
    try {
      String analysisEngineKey = aStep.getAnalysisEngineKey();
      // Find the endpoint for the delegate
      endpoint = lookUpEndpoint(analysisEngineKey, true);
      CacheEntry cacheEntry = getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
      if (endpoint != null) {
        endpoint.setController(this);
        CasStateEntry casStateEntry = getLocalCache().lookupEntry(aCasReferenceId);
        casStateEntry.resetReplyReceived();
        if (enableCasLogMap!=null && enableCasLogMap.containsKey(analysisEngineKey)) {
          //  Get a CAS
          CAS cas = cacheEntry.getCas();
          logCasForEndpoint(analysisEngineKey, cas);
        }
        Delegate delegate = lookupDelegate(analysisEngineKey);
        casStateEntry.setLastDelegate(delegate);
        if (endpoint.isCasMultiplier()) {
          delegate.setGeneratingChildrenFrom(aCasReferenceId, true);
          // Record the outgoing CAS. CASes destined for remote CM are recorded
          // in JmsOutputchannel.
          if (!endpoint.isRemote()) {
            delegate.addNewCasToOutstandingList(aCasReferenceId, true,cacheEntry.getCas().hashCode(), false);
          }
        }

        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "simpleStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_next_step__FINEST", new Object[] { aCasReferenceId, analysisEngineKey  });
        }

        // Reset number of parallel delegates back to one. This is done only if the previous step
        // was a parallel step.
        synchronized (parallelStepMux) {
          if (casStateEntry.getNumberOfParallelDelegates() > 1) {
            casStateEntry.setNumberOfParallelDelegates(1);
          }
        }
        if (!isStopped()) {
          // Start a timer for this request. The amount of time to wait
          // for response is provided in configuration for this endpoint
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                    "simpleStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_next_step_dispatch__FINEST",
                    new Object[] { getComponentName(), analysisEngineKey, aCasReferenceId  });
          }
          dispatchProcessRequest(cacheEntry, endpoint, true);
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                    "simpleStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_next_step_dispatch_completed__FINEST",
                    new Object[] { getComponentName(),   analysisEngineKey, aCasReferenceId });
          }
        }
      }
    } catch (Throwable e) {
      HashMap map = new HashMap();
      map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
      map.put(AsynchAEMessage.CasReference, aCasReferenceId);
      if (endpoint != null) {
        map.put(AsynchAEMessage.Endpoint, endpoint);
      }
      handleError(map, e);
    }

  }

  /*
   * Cas logging controlled by the following Java properties:
   *
   * UIMA_CASLOG_BASE_DIRECTORY - optional; this is the directory under which other
   *   directories with XmiCas files will be created. If not specified, the processes current
   *   directory will be the base.
   * UIMA_CASLOG_COMPONENT_ARRAY - This is a space separated list of delegates keys. If a
   *   delegate is nested inside a co-located async aggregate, the name would include the key
   *   name of the aggregate, e.g. "someAggName/someDelName". The XmiCas files will then be
   *   written into $UIMA_CASLOG_BASE_DIRECTORY/someAggName/someDelName/
   * UIMA_CASLOG_TYPE_NAME - optional; this is the name of a FeatureStructure in the CAS
   *   containing a unique string to use the name each XmiCas file. If not specified, XmiCas
   *   file name will be NNN.xmi, where NNN is the time in microseconds since the component was
   *   initialized.
   * UIMA_CASLOG_FEATURE_NAME - optional unless if the TYPE_NAME is specified; this parameter
   *   gives the string feature to use. An example of type and feature names to use would be
   *   "org.apache.uima.examples.SourceDocumentInformation" and "uri".
   *   
   */
  private void logCasForEndpoint(String analysisEngineKey, CAS cas) throws Exception {
    if (!((Boolean)enableCasLogMap.get(analysisEngineKey))) {
      // create dir and serialize typesystem
      boolean status = new File((String)casLogDirMap.get(analysisEngineKey)).mkdir();
      TypeSystemDescription tsd = TypeSystemUtil.typeSystem2TypeSystemDescription(cas.getTypeSystem());
      File tsd2xml = new File(((String)casLogDirMap.get(analysisEngineKey))+"/typesystem.xml");
      FileOutputStream os = new FileOutputStream(tsd2xml);
      tsd.toXML(os);
      os.close();
      enableCasLogMap.put(analysisEngineKey, true);
    }
    // create XmiCas file name
    Long now = Long.valueOf((System.nanoTime()-initializationTime)/1000);
    String casName = (String)casLogDirMap.get(analysisEngineKey)+"/"+now.toString()+".xmi";
    if (null != System.getProperty("UIMA_CASLOG_TYPE_NAME") && null != System.getProperty("UIMA_CASLOG_FEATURE_NAME")) {
      String typeName = System.getProperty("UIMA_CASLOG_TYPE_NAME");
      String featureName = System.getProperty("UIMA_CASLOG_FEATURE_NAME");
      Type logType = cas.getTypeSystem().getType(typeName);
      Feature logFeature = logType.getFeatureByBaseName(featureName);
      CAS view = cas;
      if (null != System.getProperty("UIMA_CASLOG_VIEW_NAME")) {
        view = cas.getView(System.getProperty("UIMA_CASLOG_VIEW_NAME"));
      }
      Iterator<FeatureStructure> iter = view.getIndexRepository().getAllIndexedFS(logType);
      FeatureStructure fs = iter.next();
      if (!iter.hasNext() && null != fs.getStringValue(logFeature)) {
        String[] uri = fs.getStringValue(logFeature).split("/");
        if (null != uri[uri.length-1].trim()) {
          casName = (String)casLogDirMap.get(analysisEngineKey)+"/"+uri[uri.length-1].trim()+".xmi";
        }
      }
    }

    // serialize the Cas
    File xmiCas = new File(casName);
    FileOutputStream os = new FileOutputStream(xmiCas);
    XmiCasSerializer.serialize(cas, os);
    os.close();
  }

  private void parallelStep(ParallelStep aStep, String aCasReferenceId) throws AsynchAEException {
    try {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "parallelStep",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_parallel_step__FINE",
                new Object[] { getComponentName(), aCasReferenceId });
      }
      Collection keyList = aStep.getAnalysisEngineKeys();
      String[] analysisEngineKeys = new String[keyList.size()];
      keyList.toArray(analysisEngineKeys);
      List parallelDelegateList = new ArrayList();
      List singleStepDelegateList = null;
      // Only remote delegates can be in a parallel step. Iterate over the
      // delegates in parallel step and assign each to a different list based on location.
      // Remote delegates are assigned to parallelDelegateList, whereas co-located
      // delegates are assigned to singleStepDelegateList. Those delegates
      // assigned to the singleStepDelegateList will be executed sequentially
      // once all parallel delegates respond.
      for (int i = 0; i < analysisEngineKeys.length; i++) {
        // Fetch an endpoint corresponding to a given delegate key
        Endpoint endpoint = lookUpEndpoint(analysisEngineKeys[i], true);
        endpoint.setController(this);
        // Assign delegate to appropriate list
        if (endpoint.isRemote()) {
          parallelDelegateList.add(endpoint);
        } else {
          if (singleStepDelegateList == null) {
            singleStepDelegateList = new ArrayList();
          }
          singleStepDelegateList.add(endpoint);
          if (UIMAFramework.getLogger().isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                    "parallelStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_move_to_single_step_list__FINE",
                    new Object[] { getComponentName(), analysisEngineKeys[i], aCasReferenceId });
          }
        }
      }
      // Fetch cache entry for a given CAS id
      CacheEntry cacheEntry = getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
      CasStateEntry casStateEntry = getLocalCache().lookupEntry(aCasReferenceId);

      // Add all co-located delegates to the cache. These delegates will be called
      // sequentially once all parallel delegates respond
      if (singleStepDelegateList != null) {
        // Add a list containing single step delegates to the cache
        // These delegates will be called sequentially when all parallel
        // delegates respond.
        cacheEntry.setDelayedSingleStepList(singleStepDelegateList);
      }
      // Check if there are any delegates in the parallel step. It is possible that
      // all of the delegates were co-located and thus the parallel delegate list
      // is empty.
      if (parallelDelegateList.size() > 0) {
        // Create endpoint array to contain as many slots as there are parallel delegates
        Endpoint[] endpoints = new Endpoint_impl[parallelDelegateList.size()];
        // Copy parallel delegate endpoints to the array
        parallelDelegateList.toArray(endpoints);
        synchronized (parallelStepMux) {
          casStateEntry.resetDelegateResponded();
          // Set number of delegates in the parallel step
          casStateEntry.setNumberOfParallelDelegates(endpoints.length);
        }
        // Dispatch CAS to remote parallel delegates
        dispatchProcessRequest(cacheEntry, endpoints, true);
      } else {
        // All delegates in a parallel step are co-located. Send the CAS
        // to the first delegate in the single step list.
        process(null, aCasReferenceId);
      }
    } catch (Throwable e) {
      HashMap map = new HashMap();
      map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
      map.put(AsynchAEMessage.CasReference, aCasReferenceId);
      handleError(map, e);
    }
  }

  public void sendRequestForMetadataToRemoteDelegates() throws AsynchAEException {
    synchronized(childControllerList) {
      //  Add a delay of 100ms before sending requests for metadata to remote delegates.
      //  This is done to give the broker enough time to 'finalize' creation of
      //  temp reply queues for each remote delegate. It's been observed (on MAC OS only) that AMQ
      //  broker QueueSession.createTemporaryQueue() call is not synchronous. Meaning,
      //  return from createTemporaryQueue() does not guarantee immediate availability
      //  of the temp queue. It seems like this operation is asynchronous, causing: 
      //  "InvalidDestinationException: Cannot publish to a deleted Destination..."
      //  when a request is send quickly to a service before the broker 'finalizes'
      //  creation of the temp queue.
      try {
        childControllerList.wait(100);
      } catch( InterruptedException e) {}
      
      if ( childControllerList.size() > 0 ) {
        for( AnalysisEngineController childController : childControllerList ) {
          if (childController instanceof AggregateAnalysisEngineController) {
            ((AggregateAnalysisEngineController) childController)
              .sendRequestForMetadataToRemoteDelegates();
          }
        }
      }
    }
    Endpoint[] delegateEndpoints = new Endpoint[destinationMap.size()];

    // First copy endpoints to an array so that we dont get Concurrent access problems
    // in case an error handler needs to disable the endpoint.
    Set keySet = destinationMap.keySet();
    Iterator it = keySet.iterator();
    int indx = 0;
    while (it.hasNext()) {
      delegateEndpoints[indx++] = (Endpoint) destinationMap.get((String) it.next());
    }
    // Now send GetMeta request to all remote delegates
    for (int i = 0; !isStopped() && i < delegateEndpoints.length; i++) {
      if (delegateEndpoints[i].isRemote()) {
        delegateEndpoints[i].initialize();
        delegateEndpoints[i].setController(this);
        String key = lookUpDelegateKey(delegateEndpoints[i].getEndpoint());
        if (key != null && destinationMap.containsKey(key)) {
          Endpoint endpoint = ((Endpoint) destinationMap.get(key));
          if (key != null && endpoint != null) {
            ServiceInfo serviceInfo = endpoint.getServiceInfo();
            PrimitiveServiceInfo pServiceInfo = new PrimitiveServiceInfo(endpoint.isCasMultiplier(), null);
            pServiceInfo.setBrokerURL(serviceInfo.getBrokerURL());
            pServiceInfo.setInputQueueName(serviceInfo.getInputQueueName());
            if (endpoint.getDestination() != null) {
              pServiceInfo.setReplyQueueName(endpoint.getDestination().toString());
            }
            pServiceInfo.setServiceKey(key);
            pServiceInfo.setState(serviceInfo.getState());
            pServiceInfo.setAnalysisEngineInstanceCount(1);

            registerWithAgent(pServiceInfo, super.getManagementInterface().getJmxDomain()
                    + super.jmxContext + ",r" + remoteIndex + "=" + key
                    + " [Remote Uima EE Service],name=" + key + "_" + serviceInfo.getLabel());

            ServicePerformance servicePerformance = new ServicePerformance();
            // servicePerformance.setIdleTime(System.nanoTime());
            servicePerformance.setRemoteDelegate();
            servicePerformance.setReplyThreadCount(endpoint.getConcurrentReplyConsumers());
            registerWithAgent(servicePerformance, super.getManagementInterface().getJmxDomain()
                    + super.jmxContext + ",r" + remoteIndex + "=" + key
                    + " [Remote Uima EE Service],name=" + key + "_" + servicePerformance.getLabel());

            ServiceErrors serviceErrors = new ServiceErrors();

            registerWithAgent(serviceErrors, super.getManagementInterface().getJmxDomain()
                    + super.jmxContext + ",r" + remoteIndex + "=" + key
                    + " [Remote Uima EE Service],name=" + key + "_" + serviceErrors.getLabel());
            remoteIndex++;

            serviceErrorMap.put(key, serviceErrors);
            Object[] delegateStatsArray = new Object[] { pServiceInfo, servicePerformance,
                serviceErrors };

            delegateStatMap.put(key, delegateStatsArray);
          }
          // If the service has stopped dont bother doing anything else. The service
          // may have been stopped because listener connection could not be established.
          if (isStopped()) {
            return;
          }
          if (delegateEndpoints[i].getStatus() == Endpoint.OK ) {
            dispatchMetadataRequest(delegateEndpoints[i]);
          }
        }
      }

      else {
        // collocated delegate
        delegateEndpoints[i].initialize();
        delegateEndpoints[i].setController(this);

        delegateEndpoints[i].setWaitingForResponse(true);
        try {
          UimaMessage message = getTransport(delegateEndpoints[i].getEndpoint()).produceMessage(
                  AsynchAEMessage.GetMeta, AsynchAEMessage.Request, getName());
          UimaTransport transport = getTransport(delegateEndpoints[i].getEndpoint());
          transport.getUimaMessageDispatcher(delegateEndpoints[i].getEndpoint()).dispatch(message);
        } catch (Exception e) {
          throw new AsynchAEException(e);
        }
      }
    }
  }

  private CasStateEntry fetchParentCasFromLocalCache(CasStateEntry casStateEntry) throws Exception {
    // Lookup parent CAS in the local cache
    CasStateEntry parentCasStateEntry = localCache.lookupEntry(casStateEntry
            .getInputCasReferenceId());
    if (parentCasStateEntry == null) {

      if (UIMAFramework.getLogger().isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME)
                .logrb(
                        Level.INFO,
                        CLASS_NAME.getName(),
                        "fetchParentCasFromLocalCache",
                        UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_cas_not_found__INFO",
                        new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                            "Local Cache" });
      }
    }
    return parentCasStateEntry;
  }

  private CacheEntry fetchParentCasFromGlobalCache(CasStateEntry casStateEntry) throws Exception {
    CacheEntry parentCASCacheEntry = null;
    try {
      // Fetch the parent Cas cache entry
      parentCASCacheEntry = getInProcessCache().getCacheEntryForCAS(
              casStateEntry.getInputCasReferenceId());
    } catch (Exception ex) {

      if (UIMAFramework.getLogger().isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.INFO,
                CLASS_NAME.getName(),
                "fetchParentCasFromGlobalCache",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_cas_not_found__INFO",
                new Object[] { getComponentName(), casStateEntry.getInputCasReferenceId(),
                    "InProcessCache" });
      }
    }
    return parentCASCacheEntry;
  }

  private boolean casHasChildrenInPlay(CasStateEntry casStateEntry) throws Exception {
    if (casStateEntry.getSubordinateCasInPlayCount() > 0) {
      // This CAS has child CASes still in play. This CAS will remain in the cache
      // until all its children are fully processed.
      if (UIMAFramework.getLogger().isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "casHasChildrenInPlay",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_final_step_parent_cas_child_count__FINEST",
                new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                    casStateEntry.getSubordinateCasInPlayCount() });
      }
      // Leave input CAS in pending state. It will be returned to the client
      // *only* if the last subordinate CAS is fully processed.
      casStateEntry.setPendingReply(true);
      // Done here. There are subordinate CASes still being processed.
      return true;
    }
    return false;
  }

  public void finalStep(FinalStep aStep, String aCasReferenceId) {
    Endpoint endpoint = null;
    boolean replySentToClient = false;
    boolean isSubordinate = false;
    CacheEntry cacheEntry = null;
    CasStateEntry casStateEntry = null;
    CasStateEntry parentCasStateEntry = null;
    Endpoint freeCasEndpoint = null;
    CacheEntry parentCASCacheEntry = null;
    Endpoint cEndpoint = null;
    boolean casDropped = false;
    boolean doDecrementChildCount = false;
    localCache.dumpContents();

    // First locate entries in the global and local cache for a given CAS
    // If not found, log a message and return
    try {
      // Get entry from the cache for a given CAS Id. This throws an exception if
      // an entry doesnt exist in the cache
      cacheEntry = getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
      casStateEntry = localCache.lookupEntry(aCasReferenceId);
      if (casStateEntry.getState() != CacheEntry.FINAL_STATE) {
        // Mark the entry to indicate that the CAS reached a final step. This CAS
        // may still have children and will not be returned to the client until
        // all of them are fully processed. This state info will aid in the
        // internal bookkeeping when the final child is processed.
        casStateEntry.setFinalStep(aStep);
        casStateEntry.setState(CacheEntry.FINAL_STATE);
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "finalStep",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_cas_in_finalstep__FINE",
                  new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                      casStateEntry.getSubordinateCasInPlayCount() });
        }
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "finalStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "finalStep",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_exception__WARNING",
                e);
      }
      return;
    }

    // Found entries in caches for a given CAS id
    try {
      endpoint = getInProcessCache().getEndpoint(null, aCasReferenceId);

      synchronized (super.finalStepMux) {
        // Check if the global cache still contains the CAS. It may have been deleted by another
        // thread already
        if (!getInProcessCache().entryExists(aCasReferenceId)) {
          return;
        }
        
  	 // System.out.println(" ---- Controller::"+getComponentName()+" CAS:"+casStateEntry.getCasReferenceId()+ " Has Children:"+casHasChildrenInPlay(casStateEntry)+" Parent::"+casStateEntry.getInputCasReferenceId());

        // Check if this CAS has children that are still being processed in this aggregate
        if (casHasChildrenInPlay(casStateEntry)) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINE,
                    CLASS_NAME.getName(),
                    "finalStep",
                    UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_cas_has_children__FINE",
                    new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                        casStateEntry.getSubordinateCasInPlayCount() });
          }

          replySentToClient = false;
          casStateEntry.waitingForChildrenToFinish(true);
          return;
        }
        casStateEntry.waitingForChildrenToFinish(false);
        if (UIMAFramework.getLogger().isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "finalStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_final_step_parent_cas_no_children__FINEST",
                  new Object[] { getComponentName(), aCasReferenceId });
        }
        // Determine if this CAS is a child of some CAS
        isSubordinate = casStateEntry.getInputCasReferenceId() != null;

        if (isSubordinate) {
          // fetch the destination of a CM that produced this CAS, so that we know where to send
          // Free Cas Notification
          freeCasEndpoint = cacheEntry.getFreeCasEndpoint();
          parentCasStateEntry = fetchParentCasFromLocalCache(casStateEntry);
          parentCASCacheEntry = fetchParentCasFromGlobalCache(casStateEntry);
          doDecrementChildCount = true;
        }
        // If the CAS was generated by this component but the Flow Controller wants to drop it OR
        // this component
        // is not a Cas Multiplier
        
        
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                    "finalStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_drop_cas_debug_FINEST",
                    new Object[] { getComponentName(), aStep.getForceCasToBeDropped(), aCasReferenceId, casStateEntry.isReplyReceived() });
        
        if (forceToDropTheCas(parentCasStateEntry, cacheEntry, aStep)) {
        	
        	
            
   
        	
        	if (casStateEntry.isReplyReceived()) {
            if (isSubordinate) {
              // drop the flow since we no longer need it
              dropFlow(aCasReferenceId, true);
              // Drop the CAS and remove cache entry for it
              dropCAS(aCasReferenceId, true);
              casDropped = true;
              // If debug level=FINEST dump the entire cache
              localCache.dumpContents();
              // Set this state as if we sent the reply to the client. This triggers a cleanup of
              // origin map and stats
              // for the current cas
              if (isTopLevelComponent()) {
                replySentToClient = true;
              }
            }
          } else {
            doDecrementChildCount = false;
          }
        } else if (!casStateEntry.isDropped()) {
          casStateEntry.setWaitingForRelease(true);
          // Send a reply to the Client. If the CAS is an input CAS it will be dropped
          cEndpoint = replyToClient(cacheEntry, casStateEntry);
          replySentToClient = true;
          if (cEndpoint.isRemote()) {
            // if this service is a Cas Multiplier don't remove the CAS. It will be removed
            // when a remote client sends explicit Release CAS Request
            if (!isCasMultiplier()) {
              // Drop the CAS and remove cache entry for it
              dropCAS(aCasReferenceId, true);
            }
            casDropped = true;
          } else {
            // Remove entry from the local cache for this CAS. If the client
            // is remote the entry was removed in replyToClient()
            try {
              localCache.lookupEntry(aCasReferenceId).setDropped(true);
            } catch (Exception e) {
            }
            localCache.remove(aCasReferenceId);
          }
          // If debug level=FINEST dump the entire cache
          localCache.dumpContents();
        }

        if (parentCasStateEntry == null && isSubordinate) {
          parentCasStateEntry = localCache.lookupEntry(casStateEntry.getInputCasReferenceId());
        }
        if (doDecrementChildCount) {
          // Child CAS has been fully processed, decrement its parent count of active child CASes
          if (parentCasStateEntry != null) {
            parentCasStateEntry.decrementSubordinateCasInPlayCount();
            // If debug level=FINEST dump the entire cache
            localCache.dumpContents();
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(
                      Level.FINE,
                      CLASS_NAME.getName(),
                      "finalStep",
                      UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_cas_decremented_child_count__FINE",
                      new Object[] { getComponentName(), parentCasStateEntry.getCasReferenceId(),
                    	  parentCasStateEntry.getSubordinateCasInPlayCount() });
            }
          }
        }

        boolean clientIsCollocated = (cEndpoint == null || !cEndpoint.isRemote());

        if (parentCasStateEntry != null && parentCasStateEntry.getSubordinateCasInPlayCount() == 0
                && parentCasStateEntry.isFailed()) {
          parentCasStateEntry.setReplyReceived();
        }
        // For subordinate CAS, check if its parent needs to be put in play. This should happen if
        // this CAS was the last of the children in play
        if (isSubordinate && releaseParentCas(casDropped, clientIsCollocated, parentCasStateEntry)  ) {
//        	parentCasStateEntry.waitingForChildrenToFinish(false);
//        if (isSubordinate && releaseParentCas(casDropped, clientIsCollocated, parentCasStateEntry) ) {
          // Put the parent CAS in play. The parent CAS can be in one of two places now depending
          // on the configuration. The parent CAS could have been suspended in the final step, or it
          // could have
          // been suspended in the process method. If the configuration indicates that the parent
          // should follow only when the last of its children leaves this aggregate, call the
          // process method.
          // Otherwise, the CAS is in a final state and simply needs to resume there.
          Endpoint lastDelegateEndpoint = parentCasStateEntry.getLastDelegate().getEndpoint();
          if (lastDelegateEndpoint.processParentLast()) {
        	  
            // The parent was suspended in the process call. Resume processing the CAS
            process(parentCASCacheEntry.getCas(), parentCasStateEntry.getCasReferenceId());
          } else if ( parentCasStateEntry.waitingForChildrenToFinish()){
        	  
            // The parent was suspended in the final step. Resume processing the CAS
            finalStep(parentCasStateEntry.getFinalStep(), parentCasStateEntry.getCasReferenceId());
          }
        }
      } // synchronized
      if (endpoint != null) {
        // remove stats associated with this Cas and a given endpoint
        dropStats(aCasReferenceId, endpoint.getEndpoint());
      }
    } catch (Exception e) {
      HashMap map = new HashMap();
      map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
      map.put(AsynchAEMessage.CasReference, aCasReferenceId);
      if (endpoint != null) {
        map.put(AsynchAEMessage.Endpoint, endpoint);
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "finalStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "finalStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
      handleError(map, e);
    } finally {
      if (replySentToClient) {
        removeMessageOrigin(aCasReferenceId);
        dropStats(aCasReferenceId, super.getName());
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "finalStep",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_final_step_show_internal_stats__FINEST",
                new Object[] { getName(), flowMap.size(), getInProcessCache().getSize(),
                    originMap.size(), super.statsMap.size() });
      }
      // freeCasEndpoint is a special endpoint for sending Free CAS Notification.
      if (casDropped && freeCasEndpoint != null) {
        try {
          freeCasEndpoint.setReplyEndpoint(true);
          freeCasEndpoint.setIsCasMultiplier(true);
          freeCasEndpoint.setFreeCasEndpoint(true);
          // send Free CAS Notification to a Cas Multiplier
          getOutputChannel().sendRequest(AsynchAEMessage.ReleaseCAS, aCasReferenceId,
                  freeCasEndpoint);
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "finalStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_service_exception_WARNING", getComponentName());

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "finalStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", e);
          }
        }
      }
    }
  }

  public boolean releaseParentCas(boolean casDropped, boolean clientIsCollocated,
          CasStateEntry parentCasStateEntry) {
    if (parentCasStateEntry == null) {
      return false;
    }

    // To release the parent CAS, the following conditions must be true
    boolean retValue =
    // The child CAS was dropped OR this Aggregate Client is colocated
    (casDropped || clientIsCollocated)
    // The parent CAS has been received by this Aggregate
            && parentCasStateEntry.isReplyReceived()
            // The CAS has to be in final state
            && parentCasStateEntry.getState() == CacheEntry.FINAL_STATE
            // To release the CAS, it may not have any children (subordinate CASes)
            && parentCasStateEntry.getSubordinateCasInPlayCount() == 0;

    if (clientEndpoint != null && UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(
              Level.FINEST,
              CLASS_NAME.getName(),
              "finalStep",
              UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_show_why_not_releasing_parent__FINEST",
              new Object[] { getComponentName(), parentCasStateEntry.getCasReferenceId(), retValue,
                  casDropped, clientEndpoint.isRemote(), parentCasStateEntry.isReplyReceived(),
                  parentCasStateEntry.isPendingReply(),
                  parentCasStateEntry.getState() == CacheEntry.FINAL_STATE,
                  parentCasStateEntry.getSubordinateCasInPlayCount(), getComponentName() });
    }
    return retValue;
  }

  private boolean forceToDropTheCas(CasStateEntry entry, CacheEntry cacheEntry, FinalStep aStep) {
    // Get the key of the Cas Producer
    String casProducer = cacheEntry.getCasProducerAggregateName();
    // CAS is considered new from the point of view of this service IF it was produced by it
    boolean isNewCas = (cacheEntry.isNewCas() && casProducer != null && getComponentName().equals(
            casProducer));
    if (entry != null && entry.isFailed() && isNewCas) {
      return true; // no point to continue if the CAS was produced in this aggregate and its parent
                   // failed here
    }
    // If the CAS was generated by this component but the Flow Controller wants to drop the CAS OR
    // this component
    // is not a Cas Multiplier
    if (isNewCas && (aStep.getForceCasToBeDropped() || !isCasMultiplier())) {
      return true;
    }
    return false;
  }

  private boolean casHasExceptions(CasStateEntry casStateEntry) {
    return (casStateEntry.getErrors().size() > 0) ? true : false;
  }

  private void sendReplyWithException(CacheEntry acacheEntry, CasStateEntry casStateEntry,
          Endpoint replyEndpoint) throws Exception {
    // boolean casProducedInThisAggregate =
    // getComponentName().equals(cacheEntry.getCasProducerAggregateName());
    if (casStateEntry.isSubordinate()) {
      // We must reply with the input CAS
      // casStateEntry = getLocalCache().lookupEntry(casStateEntry.getInputCasReferenceId());
      casStateEntry = getLocalCache().getTopCasAncestor(casStateEntry.getCasReferenceId());
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(
              Level.FINE,
              CLASS_NAME.getName(),
              "sendReplyWithException",
              UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_returning_exception_to_client__FINE",
              new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                  replyEndpoint.getEndpoint() });
    }
    if (replyEndpoint.isRemote()) {
      // this is an input CAS that has been marked as failed. Return the input CAS
      // and an exception to the client.
      getOutputChannel().sendReply(casStateEntry.getErrors().get(0),
              casStateEntry.getCasReferenceId(), null, replyEndpoint, AsynchAEMessage.Process);
    } else {
      replyEndpoint.setReplyEndpoint(true);
      UimaTransport vmTransport = getTransport(replyEndpoint.getEndpoint());
      UimaMessage message = vmTransport.produceMessage(AsynchAEMessage.Process,
              AsynchAEMessage.Response, this.getName());
      message.addIntProperty(AsynchAEMessage.Payload, AsynchAEMessage.Exception);
      message.addStringProperty(AsynchAEMessage.CasReference, casStateEntry.getCasReferenceId());

      Throwable wrapper = null;
      Throwable cause = casStateEntry.getErrors().get(0);
      if (!(cause instanceof UimaEEServiceException)) {
        // Strip off AsyncAEException and replace with UimaEEServiceException
        if (cause instanceof AsynchAEException && cause.getCause() != null) {
          wrapper = new UimaEEServiceException(cause.getCause());
        } else {
          wrapper = new UimaEEServiceException(cause);
        }
      }
      if (wrapper == null) {
        message.addObjectProperty(AsynchAEMessage.Cargo, cause);
      } else {
        message.addObjectProperty(AsynchAEMessage.Cargo, wrapper);
      }
      vmTransport.getUimaMessageDispatcher(replyEndpoint.getEndpoint()).dispatch(message);
      dropStats(casStateEntry.getCasReferenceId(),getName());
    }
  }

  private boolean sendExceptionToClient(CacheEntry cacheEntry, CasStateEntry casStateEntry,
          Endpoint replyEndpoint) throws Exception {

	  // Dont send CASes to the client if the input CAS is in failed state. One
    // of the descendant CASes may have failed in one of the delegates. Any
    // exception on descendant CAS causes the input CAS to be returned to the
    // client with an exception but only when all its descendant CASes are
    // accounted for and released.
    if (casStateEntry.isSubordinate()) {

      // Fetch the top ancestor CAS of this CAS.
      CasStateEntry topAncestorCasStateEntry = getLocalCache().getTopCasAncestor(
              casStateEntry.getInputCasReferenceId());
      if ( topAncestorCasStateEntry != null ) {
    	// check the state
          if (topAncestorCasStateEntry.isFailed() && casHasExceptions(casStateEntry)
                  && topAncestorCasStateEntry.getSubordinateCasInPlayCount() == 0) {
            return true;
          } else {
            // Add the id of the generated CAS to the map holding outstanding CASes. This
            // map will be referenced when a client sends Free CAS Notification. The map
            // stores the id of the CAS both as a key and a value. Map is used to facilitate
            // quick lookup
            cmOutstandingCASes
                    .put(casStateEntry.getCasReferenceId(), casStateEntry.getCasReferenceId());
          }
        } else if (casStateEntry.isFailed() && casHasExceptions(casStateEntry)) {
          return true;
        }
      } else  if (casStateEntry.isFailed() && casHasExceptions(casStateEntry)) {
         return true;
      }
      
    return false;
  }  
  

  private void sendReplyToRemoteClient(CacheEntry cacheEntry, CasStateEntry casStateEntry,
          Endpoint replyEndpoint) throws Exception {
	//	Check if the client endpoint is still available. It may have previously been
	//  marked as dead and now exists in the Dead Client Map.
	if ( replyEndpoint.getDestination() != null &&
			isClientDead(replyEndpoint.getDestination().toString() )) {
	    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
           UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "sendReplyToRemoteClient",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_client_dead__FINE",
                new Object[] { getComponentName(), replyEndpoint.getDestination().toString(), casStateEntry.getCasReferenceId()});
        }


  	  dropCAS(casStateEntry.getCasReferenceId(), true);
	  // If the cache is empty change the state of the Aggregate to idle
	  if (getInProcessCache().isEmpty()) {
		  endProcess(AsynchAEMessage.Process);
	  }
	  return;
	}
	  
	  
    if (sendExceptionToClient(cacheEntry, casStateEntry, replyEndpoint)) {
      sendReplyWithException(cacheEntry, casStateEntry, replyEndpoint);
    } else {
      // Send response to a given endpoint
      getOutputChannel().sendReply(cacheEntry, replyEndpoint);
    }
    // Drop the CAS only if the client is remote and the CAS is an input CAS OR
    // the CAS is a child but there was a failure delivering it to a client. The client
    // may have terminated while its input CAS was being processed, for example. 
    // If this CAS has a parent the client will send Release CAS notification to release the CAS.
    if (!casStateEntry.isSubordinate() || (casStateEntry.isSubordinate() && isCasMultiplier() && 
    		casStateEntry.deliveryToClientFailed() )) {
    	if ( cmOutstandingCASes.containsKey(casStateEntry.getCasReferenceId())) {
        	  cmOutstandingCASes.remove(casStateEntry.getCasReferenceId());
    	}
    	if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
           UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "sendReplyToRemoteClient",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_client_dead__FINE",
                new Object[] { getComponentName(), replyEndpoint.getDestination().toString(), casStateEntry.getCasReferenceId()});
        }
    	dropCAS(casStateEntry.getCasReferenceId(), true);
    	// If the cache is empty change the state of the Aggregate to idle
    	if (getInProcessCache().isEmpty()) {
    	  endProcess(AsynchAEMessage.Process);
    	}
     }
  }

  private void sendReplyToCollocatedClient(CacheEntry cacheEntry, CasStateEntry casStateEntry,
          Endpoint replyEndpoint) throws Exception {
    boolean casProducedInThisAggregate = getComponentName().equals(
            cacheEntry.getCasProducerAggregateName());
    boolean isSubordinate = casStateEntry.isSubordinate();
    boolean serviceIsCM = isCasMultiplier();
    if (sendExceptionToClient(cacheEntry, casStateEntry, replyEndpoint)) {
      try {
        sendReplyWithException(cacheEntry, casStateEntry, replyEndpoint);
      } catch (Exception e) {
      } finally {
        if (casProducedInThisAggregate) {
          // Drop the CAS generated in this Aggregate
          dropCAS(casStateEntry.getCasReferenceId(), true);
        }
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "sendReplyToCollocatedClient",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_sending_reply_to_client__FINE",
                new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                    replyEndpoint.getEndpoint() });
      }
      int mType = AsynchAEMessage.Response;
      // Check if the CAS was produced in this aggregate by any of its delegates
      // If so, send the CAS as a request. Otherwise, the CAS is an input CAS and
      // needs to return as reply.
      if (isSubordinate && serviceIsCM && casProducedInThisAggregate) {
        // this is a Cas Multiplier, send this CAS to the client in a request message.
        mType = AsynchAEMessage.Request;
        // Return the CAS to the colocated client. First make sure that this CAS
        // is associated with the input CAS. This CAS may have been produced from
        // an intermediate CAS (which was produced from the input CAS). From the
        // client perspective, this Cas Multiplier Aggregate is a black box,
        // all CASes produced here must be linked with the input CAS.
        // Find the top ancestor of this CAS. It is the input CAS sent by the client
        String inputCasId = getLocalCache().lookupInputCasReferenceId(casStateEntry);
        // Modify the parent of this CAS.
        if (inputCasId != null ) {
          if ( !inputCasId.equals(casStateEntry.getInputCasReferenceId())) {
            cacheEntry.setInputCasReferenceId(inputCasId);
          }
          // Update counters in the parents controller local cache. 
          CasStateEntry parentCasStateEntry = 
            parentController.getLocalCache().lookupEntry(inputCasId);
          if ( parentCasStateEntry != null ) {
            parentCasStateEntry.incrementSubordinateCasInPlayCount();
            parentCasStateEntry.incrementOutstandingFlowCounter();
          }
        }
      }
      // Send CAS to a given reply endpoint
      sendVMMessage(mType, replyEndpoint, cacheEntry);
    }
  }

  private boolean validEndpoint(Endpoint endpoint, CasStateEntry casStateEntry) {
    if (endpoint == null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "validEndpoint", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_client_endpoint_not_found__INFO",
                new Object[] { getComponentName(), casStateEntry.getCasReferenceId() });
      }
      return false;
    }
    if (endpoint.getEndpoint() == null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "validEndpoint", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_no_reply_destination__INFO",
                new Object[] { casStateEntry.getCasReferenceId() });
      }
      HashMap map = new HashMap();
      map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
      map.put(AsynchAEMessage.CasReference, casStateEntry.getCasReferenceId());
      handleError(map, new UnknownDestinationException());
      return false;
    }
    // Dont send a reply to the client if the client is a CAS multiplier
    if (endpoint.isCasMultiplier()) {
      return false;
    }

    return true;
  }

  private Endpoint replyToClient(CacheEntry cacheEntry, CasStateEntry casStateEntry)
          throws Exception {
    Endpoint endpoint = getReplyEndpoint(cacheEntry, casStateEntry);
    if (!validEndpoint(endpoint, casStateEntry)) {
      return null; // the reason has already been logged
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(
              Level.FINEST,
              CLASS_NAME.getName(),
              "replyToClient",
              UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_final_step__FINEST",
              new Object[] { getComponentName(), casStateEntry.getCasReferenceId(),
                  (double) (System.nanoTime() - endpoint.getEntryTime()) / (double) 1000000 });
    }
    endpoint.setFinal(true);
    if (!isStopped()) {
      try {
        if (endpoint.isRemote()) {
          sendReplyToRemoteClient(cacheEntry, casStateEntry, endpoint);
        } else {
          sendReplyToCollocatedClient(cacheEntry, casStateEntry, endpoint);
        }
      } catch ( Exception e) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "replyToClient", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "replyToClient", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);

      }
    }
    return endpoint;
  }
  /**
    Check if a given destination exists in the DeadClient Map
  **/
  private boolean isClientDead(String destination) {
	  if ( super.deadClientDestinationMap.containsKey(destination)) {
		  return true;
	  }
	  return false;
  }
  private void sendVMMessage(int messageType, Endpoint endpoint, CacheEntry cacheEntry)
          throws Exception {
    // If the CAS was produced by this aggregate send the request message to the client
    // Otherwise send the response message.
    UimaTransport transport = getTransport(endpoint.getEndpoint());
    UimaMessage message = transport.produceMessage(AsynchAEMessage.Process, messageType, getName());
    if (cacheEntry.getCasProducerAggregateName() != null
            && cacheEntry.getCasProducerAggregateName().equals(getComponentName())) {
      message.addLongProperty(AsynchAEMessage.CasSequence, cacheEntry.getCasSequence());
    }
    message.addStringProperty(AsynchAEMessage.CasReference, cacheEntry.getCasReferenceId());
    if (cacheEntry.getInputCasReferenceId() != null) {
      message.addStringProperty(AsynchAEMessage.InputCasReference, cacheEntry
              .getInputCasReferenceId());
    }
    ServicePerformance casStats = getCasStatistics(cacheEntry.getCasReferenceId());

    message.addLongProperty(AsynchAEMessage.TimeToSerializeCAS, casStats
            .getRawCasSerializationTime());
    message.addLongProperty(AsynchAEMessage.TimeToDeserializeCAS, casStats
            .getRawCasDeserializationTime());
    message.addLongProperty(AsynchAEMessage.TimeInProcessCAS, casStats.getRawAnalysisTime());
    long iT = getIdleTimeBetweenProcessCalls(AsynchAEMessage.Process);
    message.addLongProperty(AsynchAEMessage.IdleTime, iT);
    // Send reply back to the client. Use internal (non-jms) transport
    transport.getUimaMessageDispatcher(endpoint.getEndpoint()).dispatch(message);
    dropStats(cacheEntry.getCasReferenceId(), getName());

  }

  private Endpoint getReplyEndpoint(CacheEntry cacheEntry, CasStateEntry casStateEntry)
          throws Exception {
    Endpoint endpoint = null;
    // Get the endpoint that represents a client that send the request
    // to this service. If the first arg to getEndpoint() is null, the method
    // should return the origin.
    if (isTopLevelComponent()) {
      if (casStateEntry.isSubordinate()) {
        endpoint = getInProcessCache().getTopAncestorEndpoint(cacheEntry);
      } else {
        endpoint = getInProcessCache().getEndpoint(null, casStateEntry.getCasReferenceId());
      }
    } else {
      endpoint = getReplyEndpoint(cacheEntry);
      dropFlow(casStateEntry.getCasReferenceId(), false);
    }
    return endpoint;
  }

  private Endpoint getReplyEndpoint(CacheEntry cacheEntry) throws Exception {
    if (cacheEntry == null) {
      return null;
    }
    Endpoint endpoint = getMessageOrigin(cacheEntry.getCasReferenceId());
    if (endpoint == null && cacheEntry.getInputCasReferenceId() != null) {
      // Recursively call self until an endpoint is found
      endpoint = getReplyEndpoint(getInProcessCache().getCacheEntryForCAS(
              cacheEntry.getInputCasReferenceId()));
    }
    return endpoint;
  }

  private void executeFlowStep(FlowContainer aFlow, String aCasReferenceId, boolean newCAS)
          throws AsynchAEException {
    Step step = null;
    try {
      //  Guard a call to next(). Allow one thread and block the rest
      synchronized( flowControllerContainer ) {
        step = aFlow.next();
      }
    } catch (Exception e) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
              "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_service_exception_WARNING", getComponentName());
      // Any error here is automatic termination
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
              "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_exception__WARNING", e);
      try {
        sendReplyWithShutdownException(aCasReferenceId);

        // getInProcessCache().destroy();
        ErrorContext ec = new ErrorContext();
        ec.add(ErrorContext.THROWABLE_ERROR, e);
        ec.add(AsynchAEMessage.CasReference, aCasReferenceId);
        handleAction(ErrorHandler.TERMINATE, null, ec);
      } catch (Exception ex) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getComponentName());

          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", e);
        }
      }
      return;
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
              "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_step__FINEST",
              new Object[] { getComponentName(), aCasReferenceId });
    }
    try {
      if (step instanceof SimpleStep) {
        simpleStep((SimpleStep) step, aCasReferenceId);
      } else if (step instanceof ParallelStep) {
        parallelStep((ParallelStep) step, aCasReferenceId);
      } else if (step instanceof FinalStep) {
        // Special case: check if this CAS has just been produced by a Cas Multiplier.
        // If so, we received a new CAS but there are no delegates in the pipeline.
        // The CM was the last in the flow. In this case, set a property in the cache
        // to simulate receipt of the reply to this CAS. This is so that the CAS is
        // released in the finalStep() when the Aggregate is not a Cas Multiplier.
        if (newCAS) {
          CasStateEntry casStateEntry = localCache.lookupEntry(aCasReferenceId);
          if (casStateEntry != null) {
            casStateEntry.setReplyReceived();
          }
        }
        finalStep((FinalStep) step, aCasReferenceId);
      } else {
        //  catch all case. If we are here, there is a fatal configuration problem of the 
        //  flow controller 
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.SEVERE, CLASS_NAME.getName(),
                  "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_invalid_step__SEVERE",
                  new Object[] { getComponentName(), step, aCasReferenceId });
        throw new UIMARuntimeException(new Exception("Invalid Flow Step:"+step+" Object. Check Descriptor For Invalid AE Key" )); 
      }
      if ( UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_completed_step__FINEST",
                  new Object[] { getComponentName(), aCasReferenceId });
      }

    } catch (Exception e) {
//      HashMap map = new HashMap();
//      map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
//      map.put(AsynchAEMessage.CasReference, aCasReferenceId);
      ErrorContext ec = new ErrorContext();
      ec.add(ErrorContext.THROWABLE_ERROR, e);
      ec.add(AsynchAEMessage.CasReference, aCasReferenceId);
      try {
        handleAction(ErrorHandler.TERMINATE, null, ec);
      } catch( Exception ex) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "executeFlowStep", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", ex);
      }
//      handleError(map, e);
    }
  }

  private void dispatch(CacheEntry entry, Endpoint anEndpoint) throws AsynchAEException {
    if (!anEndpoint.isRemote()) {
      try {
        UimaTransport transport = getTransport(anEndpoint.getEndpoint());
        UimaMessage message = transport.produceMessage(AsynchAEMessage.Process,
                AsynchAEMessage.Request, getName());
        message.addStringProperty(AsynchAEMessage.CasReference, entry.getCasReferenceId());
        transport.getUimaMessageDispatcher(anEndpoint.getEndpoint()).dispatch(message);

      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "dispatch", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getComponentName());

          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                  "dispatch", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", e);
        }
      }
    } else {
      // Check delegate's state before sending it a CAS. The delegate
      // may have previously timed out and is in a process of pinging
      // the delegate to check its availability. While the delegate
      // is in this state, delay CASes by placing them on a list of
      // CASes pending dispatch. Once the ping reply is received all
      // delayed CASes will be dispatched to the delegate.
      if (!delayCasIfDelegateInTimedOutState(entry.getCasReferenceId(), anEndpoint.getDelegateKey(), entry.getCas().hashCode())) {
        // The delegate is in the normal state so send it this CAS
        getOutputChannel().sendRequest(AsynchAEMessage.Process, entry.getCasReferenceId(), anEndpoint);
      }
    }
  }

  /**
   * Checks the state of a delegate to see if it is in TIMEOUT State. If it is, push the CAS id onto
   * a list of CASes pending dispatch. The delegate is in a questionable state and the aggregate
   * sends a ping message to check delegate's availability. If the delegate responds to the ping,
   * all CASes in the pending dispatch list will be immediately dispatched.
   **/
  public boolean delayCasIfDelegateInTimedOutState(String aCasReferenceId, String aDelegateKey, long casHashcode)
          throws AsynchAEException {
    Delegate delegate = lookupDelegate(aDelegateKey);
    if (delegate != null && delegate.getState() == Delegate.TIMEOUT_STATE) {
      // Add CAS id to the list of delayed CASes.
      int listSize = delegate.addCasToPendingDispatchList(aCasReferenceId, casHashcode, false); // false=no timer thread per CAS
      // If the list was empty (before the add), send the GetMeta request
      // as a PING to see if the delegate service is alive.
      if (listSize == 1) {
        delegate.setAwaitingPingReply();
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "delayCasIfDelegateInTimedOutState", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_aggregate_sending_ping__INFO",
                  new Object[] { getComponentName(), delegate.getKey() });
        }
        retryMetadataRequest(delegate.getEndpoint());
      }
      return true;
    }
    return false; // Cas Not Delayed
  }

  private void dispatchProcessRequest(CacheEntry entry, Endpoint anEndpoint,
          boolean addEndpointToCache) throws AsynchAEException {
    if (addEndpointToCache) {
      getInProcessCache().addEndpoint(anEndpoint, entry.getCasReferenceId());
    }
    anEndpoint.setController(this);
    dispatch(entry, anEndpoint);

  }

  public void retryProcessCASRequest(String aCasReferenceId, Endpoint anEndpoint,
          boolean addEndpointToCache) throws AsynchAEException {
    Endpoint endpoint = null;
    String key = lookUpDelegateKey(anEndpoint.getEndpoint());

    if ((endpoint = getInProcessCache().getEndpoint(anEndpoint.getEndpoint(), aCasReferenceId)) != null) {
      Endpoint masterEndpoint = lookUpEndpoint(key, true);
      // check if the master endpoint destination has changed. This can be a case when
      // a new temp queue is created when the previous temp queue is destroyed due to
      // a broken connection.
      if (masterEndpoint.getDestination() != null) {
        // Make sure that we use the current destination for replies
        if (!masterEndpoint.getDestination().toString()
                .equals(endpoint.getDestination().toString())) {
          // Override the endopoint reply-to destination with the master destination
          endpoint.setDestination(masterEndpoint.getDestination());
        }
      }
    } else {
      endpoint = anEndpoint;
      endpoint = lookUpEndpoint(key, true);
      getInProcessCache().addEndpoint(endpoint, aCasReferenceId);
    }
    if ( getInProcessCache().entryExists(aCasReferenceId) ) {
      dispatchProcessRequest(getInProcessCache().getCacheEntryForCAS(aCasReferenceId), endpoint, addEndpointToCache);
    }
  }

  private void dispatchProcessRequest(CacheEntry entry, Endpoint[] anEndpointList,
          boolean addEndpointToCache) throws AsynchAEException {
    List<Endpoint> endpointList = new ArrayList<Endpoint>();
    for (int i = 0; i < anEndpointList.length; i++) {
      // Check if the delegate previously timed out. If so, add the CAS
      // Id to the list pending dispatch. This list holds CASes that are
      // delayed until the service responds to a Ping.
      if (delayCasIfDelegateInTimedOutState(entry.getCasReferenceId(), anEndpointList[i].getEndpoint(), entry.getCas().hashCode())) {
        // The CAS was delayed until the delegate responds to a Ping
        continue;
      } else {
        endpointList.add(anEndpointList[i]);
      }
      if (addEndpointToCache) {
        getInProcessCache().addEndpoint(anEndpointList[i], entry.getCasReferenceId());
      }
    }
    Endpoint[] endpoints = new Endpoint[endpointList.size()];
    endpointList.toArray(endpoints);
    for (Endpoint endpoint : endpoints) {
      if (endpoint.isRemote()) {
        //  In Parallel Step serialization must be xmi or CompressedBinary. Plain Binary serialization doesnt support merge.
        if (endpoint.getSerialFormat() == SerialFormat.BINARY) {
          endpoint.setSerialFormat(SerialFormat.XMI);  
        }
        getOutputChannel().sendRequest(AsynchAEMessage.Process, entry.getCasReferenceId(), endpoint);
      }    
    }

  }

  public boolean isDelegateKeyValid(String aDelegateKey) {
    if (destinationMap.containsKey(aDelegateKey)) {
      return true;
    }

    return false;
  }

  public String lookUpDelegateKey(String anEndpointName) {
    return lookUpDelegateKey(anEndpointName, null);
  }

  /**
   * Returns a delegate key given an endpoint (queue) name and a server uri. If a server is null,
   * only the endpoint name will be used for matching.
   */
  public String lookUpDelegateKey(String anEndpointName, String server) {
    String key = null;
    if (destinationToKeyMap.containsKey(anEndpointName)) {
      Set keys = destinationMap.keySet();
      Iterator it = keys.iterator();
      // Find an endpoint for the GetMeta reply. To succeed, match the endpoint (queue) name
      // as well as the server URI. We allow endpoints managed by different servers to have
      // the same queue name.
      // iterate over all endpoints until a match [queue,server] is found.
      while (it.hasNext()) {
        key = (String) it.next();
        Endpoint_impl endp = (Endpoint_impl) destinationMap.get(key);

        // Check if a queue name matches
        if (endp != null && endp.getEndpoint().equalsIgnoreCase(anEndpointName)) {
          // Check if server match is requested as well
          if (server != null) {
            // server URIs must match
            if (endp.getServerURI() != null && endp.getServerURI().equalsIgnoreCase(server)) {
              // found a match for [queue,server]
              break;
            }
            // Not found yet. Reset the key
            key = null;
            continue;
          }
          // found a match for [queue]
          break;
        }
        // Not found yet. Reset the key
        key = null;
      }
    }

    return key;
  }

  public Endpoint lookUpEndpoint(String anAnalysisEngineKey, boolean clone)
          throws AsynchAEException {
    Endpoint endpoint = (Endpoint) destinationMap.get(anAnalysisEngineKey);
    if (endpoint != null && clone) {
      return (Endpoint) ((Endpoint_impl) endpoint).clone(); // (Endpoint)
    }
    return endpoint;
  }

  public PrimitiveServiceInfo getDelegateServiceInfo(String aDelegateKey) {
    if (delegateStatMap == null || aDelegateKey == null || delegateStatMap.containsKey(aDelegateKey) == false) {
      return null;
    }
    Object[] delegateStats;
    delegateStats = (Object[]) delegateStatMap.get(aDelegateKey);
    if (delegateStats != null) {
      return (PrimitiveServiceInfo) delegateStats[SERVICE_INFO_INDX];
    }
    return null;
  }

  public ServicePerformance getDelegateServicePerformance(String aDelegateKey) {
    Object[] delegateStats;
    delegateStats = (Object[]) delegateStatMap.get(aDelegateKey);
    if (delegateStats != null) {
      return (ServicePerformance) delegateStats[SERVICE_PERF_INDX];
    }

    return null;
  }

  public ServiceErrors getDelegateServiceErrors(String aDelegateKey) {
    Object[] delegateStats;
    delegateStats = (Object[]) delegateStatMap.get(aDelegateKey);
    if (delegateStats != null) {
      return (ServiceErrors) delegateStats[SERVICE_ERROR_INDX];
    }
    return null;
  }

  public void setRemoteSerializationSupported(int remoteAEserializationSupported, String fromDestination, String fromServer) {
    String key = lookUpDelegateKey(fromDestination, fromServer);
    Endpoint_impl endpoint = (key != null) ?  (Endpoint_impl) destinationMap.get(key) : null;    
    if (endpoint == null) {
      return;
    }
    if (endpoint.getSerialFormat() != SerialFormat.XMI) {
      // only set if xmi not specified.  Otherwise, leave alone
      endpoint.setSerialFormat((remoteAEserializationSupported == AsynchAEMessage.XMI_SERIALIZATION)    ? SerialFormat.XMI :
                               (remoteAEserializationSupported == AsynchAEMessage.BINARY_SERIALIZATION) ? SerialFormat.BINARY :
                                                                                                     SerialFormat.COMPRESSED_FILTERED);
    }
  }
  
  public void mergeTypeSystem(String aTypeSystem, String fromDestination) throws AsynchAEException {
    mergeTypeSystem(aTypeSystem, fromDestination, null);
  }

  public synchronized void mergeTypeSystem(String aTypeSystem, String fromDestination,
          String fromServer) throws AsynchAEException {

    try {
      // Find the endpoint for this service, given its input queue name and broker URI.
      // We now allow endpoints managed by different servers to have the same queue name.
      // But if the external name of the broker is unknown (i.e. an old 2.2.2 service)
      // then use just the queue name, i.e. queue names must be unique for 2.2.2
      Endpoint_impl endpoint = null;
      String key = lookUpDelegateKey(fromDestination, fromServer);
      if (key != null) {
        endpoint = (Endpoint_impl) destinationMap.get(key);
      }
      if (endpoint == null) {
        // Log invalid reply and move on
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "mergeTypeSystem", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_metadata_recvd_from_invalid_delegate__INFO",
                  new Object[] { getName(), fromDestination });
        }
      } else if (endpoint.isWaitingForResponse()) {
        endpoint.setWaitingForResponse(false);
        endpoint.cancelTimer();
        boolean collocatedAggregate = false;
        if ( endpoint.getServiceInfo() != null ) {
          endpoint.getServiceInfo().setState(ServiceState.RUNNING.name());
        }
        ResourceMetaData resource = null;
        ServiceInfo remoteDelegateServiceInfo = null;
        if (aTypeSystem.trim().length() > 0) {
          if (endpoint.isRemote()) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.CONFIG)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG, CLASS_NAME.getName(),
                      "mergeTypeSystem", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_remote_delegate_ready__CONFIG",
                      new Object[] { getComponentName(), fromDestination });
            }
          }
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.CONFIG)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG, CLASS_NAME.getName(),
                    "mergeTypeSystem", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_merge_ts_from_delegate__CONFIG", new Object[] { fromDestination });
          }
          ByteArrayInputStream bis = new ByteArrayInputStream(aTypeSystem.getBytes());
          XMLInputSource in1 = new XMLInputSource(bis, null);

          resource = UIMAFramework.getXMLParser().parseResourceMetaData(in1);
          if (isStopped()) {
            return;
          }
          // for remote serialization with type filtering, create and save the type system impl
          if (endpoint.isRemote()) {
            endpoint.setTypeSystemImpl(getTypeSystemImpl((ProcessingResourceMetaData) resource));
          }
          getCasManagerWrapper().addMetadata((ProcessingResourceMetaData) resource);
          analysisEngineMetaDataMap.put(key, (ProcessingResourceMetaData) resource);

          if (((ProcessingResourceMetaData) resource).getOperationalProperties()
                  .getOutputsNewCASes()) {
            endpoint.setIsCasMultiplier(true);
            remoteCasMultiplierList.add(key);
          }
          if (endpoint.isRemote()) {
            Object o = null;
            remoteDelegateServiceInfo = getDelegateServiceInfo(key);
            if (remoteDelegateServiceInfo != null
                    && (o = ((ProcessingResourceMetaData) resource)
                            .getConfigurationParameterSettings().getParameterValue(
                                    AnalysisEngineController.AEInstanceCount)) != null) {
              ((PrimitiveServiceInfo) remoteDelegateServiceInfo)
                      .setAnalysisEngineInstanceCount(((Integer) o).intValue());
            }
          }
        } else {
          collocatedAggregate = true;
        }

        endpoint.setInitialized(true);
        // If getMeta request not yet sent, send meta request to all remote delegate
        // Special case when all delegates are remote is handled in the setInputChannel

        synchronized (unregisteredDelegateList) {
          // TODO can't find where this list is checked. Is it still used???
          if (requestForMetaSentToRemotes == false && !allDelegatesAreRemote) {
            String unregisteredDelegateKey = null;
            for (int i = 0; i < unregisteredDelegateList.size(); i++) {
              unregisteredDelegateKey = (String) unregisteredDelegateList.get(i);
              if (unregisteredDelegateKey.equals(key)) {
                unregisteredDelegateList.remove(i);
              }
            }
          }
        }

        //  
        if (collocatedAggregate || resource instanceof ProcessingResourceMetaData) {
          if (allTypeSystemsMerged()) {
            for (int i = 0; i < remoteCasMultiplierList.size(); i++) {
              Endpoint endpt = (Endpoint) destinationMap.get((String) remoteCasMultiplierList
                      .get(i));
              if (endpt != null && endpt.isCasMultiplier() && endpt.isRemote()) {
           	    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            	        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
            	                "mergeTypeSystem", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
            	                "UIMAEE_shadow_caspool_set__INFO",new Object[] { getComponentName(),endpt.getShadowPoolSize(),(String) remoteCasMultiplierList.get(i) } );
        	    }
                getCasManagerWrapper().initialize(endpt.getShadowPoolSize(),
                        (String) remoteCasMultiplierList.get(i));
                if (remoteDelegateServiceInfo != null) {
                  remoteDelegateServiceInfo.setCASMultiplier();
                }
              }
            }
            if (!isStopped()) {
              try {
                completeInitialization();
              } catch (ResourceInitializationException ex) {
                handleInitializationError(ex);
                return;
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }
  
  public static TypeSystemImpl getTypeSystemImpl(ProcessingResourceMetaData resource) throws ResourceInitializationException {
    CASMgr casMgr = CASFactory.createCAS(8, false);
    try {
      CasCreationUtils.setupTypeSystem(casMgr, resource.getTypeSystem());
      ((CASImpl) casMgr).commitTypeSystem();
    } catch (ResourceInitializationException e) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, "AggregateAnalysisEngineController_impl",
          "getTypeSystemImpl", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
          "UIMAEE_exception__WARNING", e);
      throw e;
    }
    return ((CASImpl) casMgr).getTypeSystemImpl();
  }

  private synchronized void completeInitialization() throws Exception {
    if (initialized) {
      return;
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.CONFIG)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG, CLASS_NAME.getName(),
              "completeInitialization", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_all_ts_merged__CONFIG");
    }
    if (errorHandlerChain == null) {
      plugInDefaultErrorHandlerChain();
    }
    AnalysisEngineDescription specifier = (AnalysisEngineDescription) super.getResourceSpecifier();
    aggregateMetadata = specifier.getAnalysisEngineMetaData();
    flowControllerContainer = UimaClassFactory.produceAggregateFlowControllerContainer(specifier,
            flowControllerDescriptor, analysisEngineMetaDataMap, getUimaContextAdmin(),
            ((AnalysisEngineDescription) getResourceSpecifier()).getSofaMappings(), super
                    .getManagementInterface());
    if (isTopLevelComponent()) {
      //  Add FC's meta
      getCasManagerWrapper().addMetadata((ProcessingResourceMetaData)flowControllerContainer.getMetaData());
      // Top level component is the outer most component in the containment hierarchy.
      getCasManagerWrapper().initialize("AggregateContext");
      aggregateMetadata.setTypeSystem(getCasManagerWrapper().getMetadata().getTypeSystem());
      aggregateMetadata.setTypePriorities(getCasManagerWrapper().getMetadata().getTypePriorities());

      aggregateMetadata.setFsIndexCollection(getCasManagerWrapper().getMetadata()
              .getFsIndexCollection());
    }
    if (disabledDelegateList.size() > 0) {
      flowControllerContainer.removeAnalysisEngines(disabledDelegateList);
    }

    // Before processing CASes, send notifications to all collocated delegates to
    // complete initialization. Currently this call forces all collocated Cas Multiplier delegates
    // to initialize their internal Cas Pools. CM Cas Pool is lazily initialized on
    // the first process call. The JMX Monitor needs all the Cas Pools to initialize
    // before the process call.
    if (isTopLevelComponent()) {
      onInitialize();
    }

    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
              "completeInitialization", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_initialized_controller__INFO", new Object[] { getComponentName() });
    }

    // Open latch to allow messages to be processed. The
    // latch was closed to prevent messages from entering
    // the controller before it is initialized.
    latch.openLatch(getName(), isTopLevelComponent(), true);
    initialized = true;
    // Notify client listener that the initialization of the controller was successfull
    notifyListenersWithInitializationStatus(null);
    //  If this is a collocated aggregate change its state to RUNNING from INITIALIZING.
    //  The top level aggregate state is changed when listeners on its input queue are
    //  succesfully started in SpringContainerDeployer.doStartListeners() method.
    if ( !isTopLevelComponent() ) {
      changeState(ServiceState.RUNNING);
    }
  }

  private String findKeyForValue(String fromDestination) {

    Set set = destinationMap.entrySet();
    for (Iterator it = set.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      Endpoint endpoint = (Endpoint) entry.getValue();
      if (endpoint != null) {
        String value = endpoint.getEndpoint();
        if (value.equals(fromDestination)) {
          return (String) entry.getKey();
        }
      }
    }

    return null;
  }

  private boolean allTypeSystemsMerged() {
    if (typeSystemsMerged) {
      return true;
    }
    Set set = destinationMap.entrySet();
    int delegateCount = 0;
    for (Iterator it = set.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      Endpoint endpoint = (Endpoint) entry.getValue();
      if (endpoint != null && endpoint.getStatus() != Endpoint.DISABLED
              && endpoint.isInitialized() == false) {
        break; // At least one delegate has not replied to GetMeta Request
      }
      delegateCount++;
    }
    if (delegateCount == destinationMap.size()) {
      return true; // All delegates responded to GetMeta request
    }
    return false;
  }

  public void initialize() throws AsynchAEException {
    Statistic statistic = null;
    if ((statistic = getMonitor().getLongNumericStatistic("", Monitor.ProcessCount)) == null) {
      statistic = new LongNumericStatistic(Monitor.ProcessCount);
      getMonitor().addStatistic("", statistic);
    }
    if ((statistic = getMonitor().getLongNumericStatistic("", Monitor.ProcessErrorCount)) == null) {
      statistic = new LongNumericStatistic(Monitor.ProcessErrorCount);
      getMonitor().addStatistic("", statistic);
    }
    if ((statistic = getMonitor().getLongNumericStatistic("", Monitor.ProcessErrorRetryCount)) == null) {
      statistic = new LongNumericStatistic(Monitor.ProcessErrorRetryCount);
      getMonitor().addStatistic("", statistic);
    }
  }

  public void dispatchMetadataRequest(Endpoint anEndpoint) throws AsynchAEException {
    if (isStopped()) {
      return;
    }
    if (anEndpoint == null) {
      throw new AsynchAEException("Controller:" + getComponentName()
              + " Unable To Dispatch GetMeta Request. Provided Endpoint is Invalid (NULL)");
    }
    anEndpoint.setController(this);
    anEndpoint.setWaitingForResponse(true);
    String key = lookUpDelegateKey(anEndpoint.getEndpoint());
    if (key != null && !delegateStatMap.containsKey(key)) {
      if (key != null) {
        ServiceInfo serviceInfo = anEndpoint.getServiceInfo();
        PrimitiveServiceInfo pServiceInfo = new PrimitiveServiceInfo(serviceInfo.isCASMultiplier(),null);
        pServiceInfo.setBrokerURL(serviceInfo.getBrokerURL());
        pServiceInfo.setInputQueueName(serviceInfo.getInputQueueName());
        pServiceInfo.setState(serviceInfo.getState());
        pServiceInfo.setAnalysisEngineInstanceCount(1);
        ServicePerformance servicePerformance = new ServicePerformance();
        if (anEndpoint.isRemote()) {
          servicePerformance.setRemoteDelegate();
        }
        ServiceErrors serviceErrors = new ServiceErrors();

        serviceErrorMap.put(key, serviceErrors);
        Object[] delegateStatsArray = new Object[] { pServiceInfo, servicePerformance,
            serviceErrors };

        delegateStatMap.put(key, delegateStatsArray);

      }
    }
    getOutputChannel().sendRequest(AsynchAEMessage.GetMeta, null, anEndpoint);
  }

  public void retryMetadataRequest(Endpoint anEndpoint) throws AsynchAEException {
    dispatchMetadataRequest(anEndpoint);
  }

  public void sendMetadata(Endpoint anEndpoint) {
    super.sendMetadata(anEndpoint, aggregateMetadata);
  }

  public ControllerLatch getControllerLatch() {
    return latch;
  }

  public Monitor getMonitor() {
    return super.monitor;
  }

  public void setMonitor(Monitor monitor) {
    this.monitor = monitor;
  }

  public void handleDelegateLifeCycleEvent(String anEndpoint, int aDelegateCount) {
    Endpoint endpoint = null;
    if (destinationMap.containsKey(anEndpoint)) {
      endpoint = (Endpoint) destinationMap.get(anEndpoint);
    }
    if (aDelegateCount == 0) {

      // Set the state of the delegate endpoint
      endpoint.setNoConsumers(true);
      // Get entries from the in-process cache that match the endpoint name
      CacheEntry[] cachedEntries = getInProcessCache().getCacheEntriesForEndpoint(anEndpoint);

      for (int i = 0; cachedEntries != null && i < cachedEntries.length; i++) {
        String casReferenceId = cachedEntries[i].getCasReferenceId();
        String parentCasReferenceId = null;
        try {
          CasStateEntry parentEntry = getLocalCache().getTopCasAncestor(casReferenceId);// cachedEntries[i].getInputCasReferenceId();
          if (parentEntry != null) {
            parentCasReferenceId = parentEntry.getCasReferenceId();
          }
        } catch (Exception e) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                        "handleDelegateLifeCycleEvent", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_parent_cas_notin_cache__INFO", new Object[] { getComponentName(), casReferenceId });
            }
        }
        getInProcessCache().getEndpoint(anEndpoint, casReferenceId).cancelTimer();
        Endpoint requestOrigin = cachedEntries[i].getMessageOrigin();
        try {
          getOutputChannel().sendReply(
                  new UimaEEServiceException("Delegates Not Found To Process CAS on Endpoint:"
                          + anEndpoint), casReferenceId, parentCasReferenceId, requestOrigin,
                  AsynchAEMessage.Process);
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                    "handleDelegateLifeCycleEvent", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_no_consumers__INFO", new Object[] { casReferenceId, anEndpoint });
          }
        } finally {
          getInProcessCache().remove(casReferenceId);
        }
      }
    } else if (endpoint != null && endpoint.hasNoConsumers()) {
      // At least one consumer is available
      endpoint.setNoConsumers(false);
    }

  }

  public void retryLastCommand(int aCommand, Endpoint anEndpoint, String aCasReferenceId) {
    try {
      getOutputChannel().sendRequest(aCommand, aCasReferenceId, anEndpoint);
    } catch (AsynchAEException e) {

    }
  }

  public ServiceErrors getServiceErrors(String aDelegateKey) {
    if (!serviceErrorMap.containsKey(aDelegateKey)) {
      return null;
    }
    return (ServiceErrors) serviceErrorMap.get(aDelegateKey);
  }

  public AggregateServiceInfo getServiceInfo() {
    if (serviceInfo == null) {
      serviceInfo = new AggregateServiceInfo(isCasMultiplier(), this);
      // if this is a top level service and the input channel not yet initialized
      // block in getInputChannel() on the latch
      if (isTopLevelComponent() && getInputChannel() != null) {
        serviceInfo.setInputQueueName(getInputChannel().getName());
        serviceInfo.setBrokerURL(super.getBrokerURL());
      } else {
        serviceInfo.setInputQueueName(getName());
        serviceInfo.setBrokerURL("vm://localhost");
      }
      serviceInfo.setDeploymentDescriptorPath(super.aeDescriptor);
      //serviceInfo.setState(super.getState().name());
    }
    return serviceInfo;
  }

  public ServicePerformance getServicePerformance(String aDelegateKey) {
    return getDelegateServicePerformance(aDelegateKey);
  }

  /**
   * Accumulate analysis time for the aggregate
   * 
   * @param anAnalysisTime
   */
  public synchronized void incrementAnalysisTime(long anAnalysisTime) {
    servicePerformance.incrementAnalysisTime(anAnalysisTime);
  }

  public void stopTimers() {
    Set set = destinationMap.entrySet();
    for (Iterator it = set.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      Endpoint endpoint = (Endpoint) entry.getValue();
      if (endpoint != null) {
        endpoint.cancelTimer();
      }
    }

  }

  public boolean requestForMetaSentToRemotes() {
    return requestForMetaSentToRemotes;
  }

  public void setRequestForMetaSentToRemotes() {
    requestForMetaSentToRemotes = true;
  }

  public void cleanUp() {
    if (flowMap != null) {
      flowMap.clear();
    }
    if (destinationMap != null) {
      destinationMap.clear();
    }

    if (destinationToKeyMap != null) {
      destinationToKeyMap.clear();
    }
    if (analysisEngineMetaDataMap != null) {
      analysisEngineMetaDataMap.clear();
    }
    if (remoteCasMultiplierList != null) {
      remoteCasMultiplierList.clear();
    }
    if (originMap != null) {
      originMap.clear();
    }
    
    if (childControllerList != null) {
      synchronized( childControllerList ) {
        childControllerList.clear();
      }
    }
    if (delegateStats != null) {
      delegateStats.clear();
    }
    if (flowControllerContainer != null) {
      synchronized (flowControllerContainer) {
        flowControllerContainer.destroy();
      }
    }
    perCasStatistics.clear();

    if (disabledDelegateList != null) {
      disabledDelegateList.clear();
    }

    if (delegateStatMap != null) {
      delegateStatMap.clear();
    }

    InProcessCache cache = getInProcessCache();
    if (cache != null) {
      cache.destroy();
    }
  }

  public void stop() {
	  super.stop(true);  // shutdown now
	  
	// release all permits
	  if ( semaphore != null ) {
		  while ( semaphore.availablePermits() > 0) {
		  		semaphore.release();
	  	  }
	  }

   
    
    this.cleanUp();
        // dont kill jUnit tests
    if (isTopLevelComponent() &&  System.getProperty("dontKill") == null) {
    	System.exit(0);
    }
    
  }

  public List getChildControllerList() {
    return childControllerList;
  }

  /**
   * Force all collocated delegates to perform any post-initialization steps.
   */
  public void onInitialize() {
    // For each collocated delegate
    synchronized(childControllerList) {
      if ( childControllerList.size() > 0 ) {
        for( AnalysisEngineController childController : childControllerList ) {
          // notify the delegate
          childController.onInitialize();
        }
      }
    }
  }
  
  public void changeCollocatedDelegateState( String delegateKey, ServiceState state ) throws Exception {
    if ( delegateKey != null && state != null ) {
      synchronized(childControllerList) {
        if ( childControllerList.size() > 0 ) {
          for( AnalysisEngineController childController : childControllerList ) {
            if ( delegateKey.equals(childController.getKey())) {
              childController.changeState(state);
            }
          }
        }
      }
    } else {
      throw new Exception("Controller:"+getComponentName()+" Unable to change state of colocated delegate");
    }
  }

  
  public void dumpState(StringBuffer buffer, String lbl1) {
    
    StringBuffer delegates = new StringBuffer();
    if ( buffer.length() == 0) {
      delegates.append("\n"+lbl1+getComponentName()+" Delegates:");
    }
    Set set = destinationMap.entrySet();
    // For all delegates ...
    for (Iterator it = set.iterator(); it.hasNext();) {
        delegates.append("\n");
        Map.Entry entry = (Map.Entry) it.next();
        Endpoint endpoint = (Endpoint) entry.getValue();
        ServiceState state = ServiceState.INITIALIZING;
        if ( endpoint.isRemote() ) {
            if ( endpoint.isInitialized() ) {
              state = ServiceState.RUNNING;
            }
            delegates.append(lbl1+lbl1+"Remote Delegate:"+endpoint.getDelegateKey()+" State: "+state.toString());
        } else {
          if ( endpoint.isInitialized() ) {
            state = ServiceState.RUNNING;
          }
          delegates.append(lbl1+lbl1+"Co-located Delegate:"+endpoint.getDelegateKey()+" State: "+state.toString());
          //  If a delegate is an aggregate, call its dumpState method
          synchronized(childControllerList) {
            if ( childControllerList.size() > 0 ) {
              for( AnalysisEngineController childController : childControllerList ) {
                if ( endpoint.getDelegateKey().equals(childController.getKey()) && !childController.isPrimitive()) {
                  buffer.append(delegates.toString());
                  childController.dumpState(buffer,lbl1+lbl1);
                  delegates.setLength(0);
                }
              }
            }
          }
        }
    }
    buffer.append(delegates.toString());
  }
  public LocalCache getLocalCache() {
    return localCache;
  }

  /**
   * Return {@link Delegate} object for a given delegate key.
   * 
   */
  public Delegate lookupDelegate(String aDelegateKey) {

    for (Delegate delegate : delegates) {
      if (delegate.getKey().equals(aDelegateKey)) {
        return delegate;
      }
    }
    return null;
  }
  
  public void setCasLoggingDirectory(String key, String directory) {
    String baseDir = System.getProperty("user.dir");
    if (null != System.getProperty("UIMA_CASLOG_BASE_DIRECTORY")) {
      baseDir = System.getProperty("UIMA_CASLOG_BASE_DIRECTORY");
      //TODO how to fail this next line?
      boolean status = new File(baseDir).mkdir();
    }
    directory = directory.replace('/', '-');
    if (casLogDirMap == null)
      casLogDirMap = new HashMap<String, String>();
    casLogDirMap.put(key, baseDir+"/"+directory);
    if (enableCasLogMap == null)
      enableCasLogMap = new HashMap<String, Boolean>();
    enableCasLogMap.put(key, false);
    initializationTime = System.nanoTime();
  }
  public int getServiceCasPoolSize() {
	 return ((AsynchAECasManager_impl)casManager).getCasPoolSize();
  }
}
