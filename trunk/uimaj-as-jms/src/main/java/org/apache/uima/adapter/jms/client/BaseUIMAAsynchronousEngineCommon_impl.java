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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.uima.UIMAFramework;
import org.apache.uima.UIMARuntimeException;
import org.apache.uima.UIMA_IllegalStateException;
import org.apache.uima.aae.AsynchAECasManager;
import org.apache.uima.aae.UIDGenerator;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaSerializer;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaASProcessStatusImpl;
import org.apache.uima.aae.client.UimaASStatusCallbackListener;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController_impl;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.delegate.Delegate.DelegateEntry;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.InvalidMessageException;
import org.apache.uima.aae.error.MessageTimeoutException;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.error.UimaASCollectionProcessCompleteTimeout;
import org.apache.uima.aae.error.UimaASMetaRequestTimeout;
import org.apache.uima.aae.error.UimaASPingTimeout;
import org.apache.uima.aae.error.UimaASProcessCasTimeout;
import org.apache.uima.aae.error.UimaEEServiceException;
import org.apache.uima.aae.jmx.UimaASClientInfo;
import org.apache.uima.aae.jmx.UimaASClientInfoMBean;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.adapter.jms.ConnectionValidator;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.message.PendingMessage;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.AllowPreexistingFS;
import org.apache.uima.cas.impl.BinaryCasSerDes6;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.cas.impl.TypeSystemImpl;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.cas.impl.BinaryCasSerDes6.ReuseInfo;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.jms.error.handler.BrokerConnectionException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;
import org.apache.uima.resourceSpecifier.factory.SerializationStrategy;
import org.apache.uima.util.Level;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.impl.ProcessTrace_impl;

public abstract class BaseUIMAAsynchronousEngineCommon_impl implements UimaAsynchronousEngine,
        MessageListener {
  private static final Class CLASS_NAME = BaseUIMAAsynchronousEngineCommon_impl.class;

  public enum ClientState {INITIALIZING, RUNNING, FAILED, RECONNECTING, STOPPING, STOPPED};
  
  protected ClientState state = ClientState.INITIALIZING;

  protected String brokerURI = null;

  protected static final String SHADOW_CAS_POOL = "ShadowCasPool";

  protected static final int MetadataTimeout = 1;

  protected static final int CpCTimeout = 2;

  protected static final int ProcessTimeout = 3;

  protected static final int PingTimeout = 4;

  protected volatile boolean initialized;

  protected List listeners = new ArrayList();

  protected AsynchAECasManager asynchManager;

  protected boolean remoteService = false;

  protected CollectionReader collectionReader = null;

  protected volatile boolean running = false;

  protected ProcessingResourceMetaData resourceMetadata;
  
  protected CAS sendAndReceiveCAS = null;

  protected UIDGenerator idGenerator = new UIDGenerator();

  protected ConcurrentHashMap<String, ClientRequest> clientCache = new ConcurrentHashMap<String, ClientRequest>();

  protected ConcurrentHashMap<Long, ThreadMonitor> threadMonitorMap = new ConcurrentHashMap<Long, ThreadMonitor>();

  // Default timeout for ProcessCas requests
  protected int processTimeout = 0;

  // Default timeout for GetMeta requests
  protected int metadataTimeout = 60000;

  // Default timeout for CpC requests is no timeout
  protected int cpcTimeout = 0;

  protected volatile boolean abort = false;

  protected static final String uniqueIdentifier = String.valueOf(System.nanoTime());

  protected Exception exc;

  // Counter maintaining a number of CASes sent to a service. The counter
  // is incremented every time a CAS is sent and decremented when the CAS
  // reply is received. It is also adjusted down in case of a timeout or
  // error.
  protected AtomicLong outstandingCasRequests = new AtomicLong();

  protected AtomicLong totalCasRequestsSentBetweenCpCs = new AtomicLong();

  protected ConcurrentHashMap springContainerRegistry = new ConcurrentHashMap();

  protected MessageConsumer consumer = null;

  protected SerialFormat serialFormat = SerialFormat.XMI;
  
  protected TypeSystemImpl remoteTypeSystem; // of the remote service, for filtered binary compression

  protected UimaASClientInfoMBean clientSideJmxStats = new UimaASClientInfo();

  private UimaSerializer uimaSerializer = new UimaSerializer();

  protected ClientServiceDelegate serviceDelegate = null;

  private Object stopMux = new Object();

  private Object sendMux = new Object();

  private BlockingQueue<CasQueueEntry> threadQueue = new LinkedBlockingQueue<CasQueueEntry>();

  private ConcurrentHashMap<Long, CasQueueEntry> threadRegistrar = new ConcurrentHashMap<Long, CasQueueEntry>();

  private volatile boolean casQueueProducerReady;

  private Object casProducerMux = new Object();

  protected BlockingQueue<PendingMessage> pendingMessageQueue = new LinkedBlockingQueue<PendingMessage>();

  // Create Semaphore that will signal when the producer object is initialized
  protected Semaphore producerSemaphore = new Semaphore(1);

  // Create Semaphore that will signal when CPC reply has been received
  protected Semaphore cpcSemaphore = new Semaphore(1);

  // Create Semaphore that will signal when GetMeta reply has been received
  protected Semaphore getMetaSemaphore = new Semaphore(0, true);

  // Signals when the client is ready to send CPC request
  protected Semaphore cpcReadySemaphore = new Semaphore(1);

  // Signals receipt of a CPC reply
  protected Semaphore cpcReplySemaphore = new Semaphore(1);

  protected volatile boolean producerInitialized;

  protected static ConcurrentHashMap<String, SharedConnection> sharedConnections =
		  new ConcurrentHashMap<String, SharedConnection>();
  
  //protected static SharedConnection sharedConnection = null;

  protected Thread shutdownHookThread = null;

  private ExecutorService exec = Executors.newFixedThreadPool(1);
  
  private volatile boolean casMultiplierDelegate;
  
  abstract public String getEndPointName() throws Exception;

  abstract protected TextMessage createTextMessage() throws Exception;

  abstract protected BytesMessage createBytesMessage() throws Exception;

  abstract protected void setMetaRequestMessage(Message msg) throws Exception;

  abstract protected void setCASMessage(String casReferenceId, CAS aCAS, Message msg)
          throws Exception;

  abstract protected void setCASMessage(String casReferenceId, String aSerializedCAS, Message msg)
          throws Exception;

  abstract protected void setCASMessage(String casReferenceId, byte[] aSerializedCAS, Message msg)
          throws Exception;

  abstract public void setCPCMessage(Message msg) throws Exception;

  abstract public void initialize(Map anApplicationContext) throws ResourceInitializationException;

  abstract protected void cleanup() throws Exception;

  abstract public String deploy(String[] aDeploymentDescriptorList, Map anApplicationContext)
          throws Exception;

  abstract protected String deploySpringContainer(String[] springContextFiles)
          throws ResourceInitializationException;

  abstract protected MessageSender getDispatcher();
  
  abstract protected void initializeConsumer(String aBrokerURI, Connection connection) throws Exception;

  // enables/disable timer per CAS. Defaul is to use single timer for
  // all outstanding CASes
  protected volatile boolean timerPerCAS=false;
  
  //abstract protected  String getBrokerURI();

  protected void setBrokeryURI(String brokerURI ) {
	  this.brokerURI = brokerURI;
  }
  protected String getBrokerURI() {
	  return brokerURI;
  }
  
  public void addStatusCallbackListener(UimaAsBaseCallbackListener aListener) {
    if (running) {
	   throw new UIMA_IllegalStateException(JmsConstants.JMS_LOG_RESOURCE_BUNDLE,"UIMAJMS_listener_added_after_initialize__WARNING", new Object[]{});
	}
    listeners.add(aListener);
  }

  public SerialFormat getSerialFormat() {
    return serialFormat;
  }

  protected void setSerialFormat(SerialFormat aSerialFormat) {
    serialFormat = aSerialFormat;
  }
  
  public TypeSystemImpl getRemoteTypeSystem() {
    return remoteTypeSystem;
  }

  protected void setRemoteTypeSystem(TypeSystemImpl remoteTypeSystem) {
    this.remoteTypeSystem = remoteTypeSystem;
  }
  
  /**
   * Serializes a given CAS.
   * 
   * @param aCAS
   *          - CAS to serialize
   * @return - serialized CAS
   * 
   * @throws Exception
   */
  protected String serializeCAS(CAS aCAS, XmiSerializationSharedData serSharedData)
          throws Exception {
    return uimaSerializer.serializeCasToXmi(aCAS, serSharedData);
  }

  protected String serializeCAS(CAS aCAS) throws Exception {
    XmiSerializationSharedData serSharedData = new XmiSerializationSharedData();
    return uimaSerializer.serializeCasToXmi(aCAS, serSharedData);
  }

  public void removeStatusCallbackListener(UimaAsBaseCallbackListener aListener) {
    listeners.remove(aListener);
  }

  public void onBeforeMessageSend(UimaASProcessStatus status) {
      try {
   	    for (int i = 0; listeners != null && i < listeners.size(); i++) {
   	        UimaAsBaseCallbackListener statCL = (UimaAsBaseCallbackListener) listeners.get(i);
   	        statCL.onBeforeMessageSend(status);
        }
      } catch( Throwable t) {
			UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "onBeforeMessageSend", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", t);
      }	  
  }

  public void onBeforeProcessCAS(UimaASProcessStatus status, String nodeIP, String pid) {
    for (int i = 0; listeners != null && i < listeners.size(); i++) {
      UimaAsBaseCallbackListener statCL = (UimaAsBaseCallbackListener) listeners.get(i);
      try {
          statCL.onBeforeProcessCAS(status, nodeIP, pid);
    	  
      } catch( Throwable t) {
			UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "onBeforeProcessCAS", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", t);
      }
    }
  }

  public void onBeforeProcessMeta(String nodeIP, String pid) {
	  for (int i = 0; listeners != null && i < listeners.size(); i++) {
		  UimaAsBaseCallbackListener statCL = (UimaAsBaseCallbackListener) listeners
				  .get(i);
		  try {
			  statCL.onBeforeProcessMeta(nodeIP, pid);
		  } catch (Throwable t) {
			  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING,
					  getClass().getName(), "onBeforeProcessMeta",
					  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
					  "UIMAEE_exception__WARNING", t);
		  }
	  }
  }
  public synchronized void setCollectionReader(CollectionReader aCollectionReader)
          throws ResourceInitializationException {
    if (initialized) {
      // Uima ee client has already been initialized. CR should be
      // set before calling initialize()
      throw new ResourceInitializationException();
    }
    collectionReader = aCollectionReader;
  }

  private void addMessage(PendingMessage msg) {
    pendingMessageQueue.add(msg);
  }

  protected void acquireCpcReadySemaphore() {
    try {
      // Acquire cpcReady semaphore to block sending CPC request until
      // ALL outstanding CASes are received.
      cpcReadySemaphore.acquire();
    } catch (InterruptedException e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "collectionProcessingComplete", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_client_interrupted_while_acquiring_cpcReadySemaphore__WARNING", new Object[] {});
          }
    }
  }

  public synchronized void collectionProcessingComplete() throws ResourceProcessException {
    try {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                "collectionProcessingComplete", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_app_cpc_request_FINEST", new Object[] {});
      }
      if (outstandingCasRequests.get() > 0) {
        UIMAFramework.getLogger(CLASS_NAME)
                .logrb(
                        Level.INFO,
                        CLASS_NAME.getName(),
                        "collectionProcessingComplete",
                        JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_in_cpc_INFO",
                        new Object[] { outstandingCasRequests.get(),
                            totalCasRequestsSentBetweenCpCs.get() });
      }
      // If the client was initialized but never sent any CASes its cpcReadySemaphore
      // must be first explicitly released to enable the code to send CPC to a service.
      // The semaphore is initially acquired in the initialize(Map) method and typically
      // released when the number of CASes sent equals the number of CASes received. Since
      // no CASes were sent we must do the release here to be able to continue.
      if (totalCasRequestsSentBetweenCpCs.get() == 0 && !serviceDelegate.isAwaitingPingReply()) {
        cpcReadySemaphore.release();
      }
      // The cpcReadySemaphore was initially acquired in the initialize() method
      // so below we wait until ALL CASes are processed. Once all
      // CASes are received the semaphore will be released
      acquireCpcReadySemaphore();
      serviceDelegate.cancelDelegateTimer();
      if (!running) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "collectionProcessingComplete", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_cpc_request_not_done_INFO", new Object[] {});
        }
        return;
      }

      ClientRequest requestToCache = new ClientRequest(uniqueIdentifier, this); // , timeout);
      requestToCache.setIsRemote(remoteService);
      requestToCache.setCPCRequest(true);
      requestToCache.setCpcTimeout(cpcTimeout);
      requestToCache.setEndpoint(getEndPointName());

      clientCache.put(uniqueIdentifier, requestToCache);

      PendingMessage msg = new PendingMessage(AsynchAEMessage.CollectionProcessComplete);
      if (cpcTimeout > 0) {
        requestToCache.startTimer();
        msg.put(UimaAsynchronousEngine.CpcTimeout, String.valueOf(cpcTimeout));
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                "collectionProcessingComplete", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_started_cpc_request_timer_FINEST", new Object[] {});
      }
      // Add CPC message to the pending queue
      addMessage(msg);
      // Acquire cpc semaphore. When a CPC reply comes or there is a timeout or the client
      // is stopped, the semaphore will be released.
      try {
        cpcReplySemaphore.acquire();
      } catch (InterruptedException ex) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "collectionProcessingComplete", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_client_interrupted_while_acquiring_cpcReplySemaphore__WARNING", new Object[] {});
          }
      }
      // Wait for CPC Reply. This blocks on the cpcReplySemaphore
      waitForCpcReply();
      totalCasRequestsSentBetweenCpCs.set(0); // reset number of CASes sent to a service
      cancelTimer(uniqueIdentifier);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                "collectionProcessingComplete", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_cancelled_cpc_request_timer_FINEST", new Object[] {});
      }
      if (running) {
        for (int i = 0; listeners != null && i < listeners.size(); i++) {
          ((UimaASStatusCallbackListener) listeners.get(i)).collectionProcessComplete(null);
        }
      }
    } catch (Exception e) {
      throw new ResourceProcessException(e);
    }
  }

  private void releaseCacheEntries() {
    Iterator it = clientCache.keySet().iterator();
    while (it.hasNext()) {
      ClientRequest entry = clientCache.get((String) it.next());
      if (entry != null && entry.getCAS() != null) {
        entry.getCAS().release();
      }
    }
  }

  private void clearThreadRegistrar() {
    Iterator it = threadRegistrar.keySet().iterator();
    while (it.hasNext()) {
      Long key = (Long) it.next();
      CasQueueEntry entry = threadRegistrar.get(key);
      if (entry != null) {
        entry.clear();
      }
    }
  }

  public void doStop() {
    synchronized (stopMux) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "stop",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_stopping_as_client_INFO",
                new Object[] {});
      }
      if (!running) {
        return;
      }

      exec.shutdownNow();
      
      casQueueProducerReady = false;
      if (serviceDelegate != null) {
        serviceDelegate.cancelDelegateTimer();
        serviceDelegate.cancelDelegateGetMetaTimer();
      }
      try {
        try {
          clearThreadRegistrar();
          releaseCacheEntries();
        } catch (Exception ex) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "stop", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", ex);
          }
        }

        // Unblock threads
        if (threadMonitorMap.size() > 0) {
          Iterator it = threadMonitorMap.keySet().iterator();
          while (it.hasNext()) {
            long key = ((Long) it.next()).longValue();
            ThreadMonitor threadMonitor = (ThreadMonitor) threadMonitorMap.get(key);
            if (threadMonitor == null || threadMonitor.getMonitor() == null) {
              continue;
            }
            threadMonitor.getMonitor().release();
          }
        }
        cpcReadySemaphore.release();
        outstandingCasRequests.set(0); // reset global counter of outstanding requests

        cpcReplySemaphore.release();
        getMetaSemaphore.release();

        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "stop",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_stopped_as_client_INFO",
                  new Object[] {});
        }
        for (Iterator i = springContainerRegistry.entrySet().iterator(); i.hasNext();) {
          Map.Entry entry = (Map.Entry) i.next();
          Object key = entry.getKey();
          undeploy((String) key);
        }
        asynchManager = null;
        springContainerRegistry.clear();
        listeners.clear();
        clientCache.clear();
        threadQueue.clear();
        // Add empty CasQueueEntry object to the queue so that we wake up a reader thread which
        // may be sitting in the threadQueue.take() method. The reader will
        // first check the state of the 'running' flag and find it false which
        // will cause the reader to exit run() method
        threadQueue.add(new CasQueueEntry());
        threadRegistrar.clear();
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                  "stop", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", e);
        }
      } finally {
        //  Shutdown hook has been previously added in case an application 'forgets'
        //  to call stop. Since we are in the stop() method, the hook is no longer 
        //  needed. Catch IllegalStateException which is thrown by JVM if it is already 
        //  in the process of shutting down
        if ( shutdownHookThread != null ) {
          try {
            Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
          } catch( IllegalStateException e) {}
        }
      }
    }
  }

  /**
   * This method spins a thread where CASes are distributed to requesting threads in an orderly
   * fashion. CASes are distributed among the threads based on FIFO. Oldest waiting thread receive
   * the CAS first.
   * 
   */
  private void serveCASes() {
    synchronized (casProducerMux) {
      if (casQueueProducerReady) {
        return; // Only one CAS producer thread is needed/allowed
      }
      casQueueProducerReady = true;
    }
    // Spin a CAS producer thread
    new Thread() {
      public void run() {
        // terminate when the client API is stopped
        while (running) {
          try {
            // Remove the oldest CAS request from the queue.
            // Every thread requesting a CAS adds an entry to this
            // queue.
            CasQueueEntry entry = threadQueue.take();
            CAS cas = null;
            long startTime = System.nanoTime();
            // Wait for a free CAS instance
            if (!running || asynchManager == null) {
              return; // client API has been stopped
            }
            if (remoteService) {
              cas = asynchManager.getNewCas("ApplicationCasPoolContext");
            } else {
              cas = asynchManager.getNewCas();
            }
            long waitingTime = System.nanoTime() - startTime;
            clientSideJmxStats.incrementTotalTimeWaitingForCas(waitingTime);
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(
                      Level.FINEST,
                      CLASS_NAME.getName(),
                      "serveCASes.run()",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_new_cas_FINEST",
                      new Object[] { "Time Waiting for CAS",
                          (double) waitingTime / (double) 1000000 });
            }
            if (running) { // only if the client is still running handle the new cas
							//	Assigns a CAS and releases a semaphore
	            entry.setCas(cas);
            } else {
              return; // Client is terminating
            }
          } catch (Exception e) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                      "serveCASes.run()", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_exception__WARNING", e);
            }
          }
        }
      }
    }.start();
  }

  /**
   * Returns a CAS. If multiple threads call this method, the order of each request is preserved.
   * The oldest waiting thread receives the CAS. Each request for a CAS is queued, and when the CAS
   * becomes available the oldest waiting thread will receive it for processing.
   */
  public CAS getCAS() throws Exception {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "getCAS",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_request_for_cas_FINEST",
              new Object[] {});
    }
    if (!running) {
      throw new RuntimeException("Uima AS Client Is Stopping");
    }
    if (!initialized) {
      throw new ResourceInitializationException();
    }
    // Spin a thread that fetches CASes from the CAS Pool
    if (!casQueueProducerReady) {
      serveCASes(); // start CAS producer thread
    }
    // Each thread has an entry in the map. The entry is created in the
    // map once and cached.
    CasQueueEntry entry = getQueueEntry(Thread.currentThread().getId());
    // Add this thread entry to the queue of threads waiting for a CAS
    threadQueue.add(entry);
    if (entry != null) {
      while (running) {
        CAS cas = null;
        //  getCas() blocks until a CAS is set in the CAS producer thread
        //  ( see serveCASes() above).
        //  If the client is stopped, the cleanup code will force getCas()
        //  to release a semaphore to prevent a hang.
        if ( (cas = entry.getCas()) == null) {
          break;
        } else {
          //  We may have been waiting in entry.getCas() for awhile 
          //  so first check the status of the client. If is not running
          //  the semaphore in getCas() was forcefully released to prevent
          //  a hang. If we are stopping just return null.
          if ( !running ) {
            break;
          }
          return cas;
        }
      } // while
    }
    return null; // client has terminated
  }

  private CasQueueEntry getQueueEntry(long aThreadId) {
    CasQueueEntry entry = null;
    if (threadRegistrar.containsKey(aThreadId)) {
      entry = threadRegistrar.get(aThreadId);
    } else {
    	//	Creates a new instance and acquires a semaphore. The semaphore
    	//	is released in the CasQueueEntry.setCAS()
      entry = new CasQueueEntry();
      threadRegistrar.put(aThreadId, entry);
    }
    return entry;
  }

  protected void reset() {
  }

  /**
   * This class manages access to a CAS. A CAS is assigned
   * to an instance of this class in the CAS producer thread and
   * it is consumed in the getCAS() method. Access to a CAS
   * is protected by a semaphore.
   *
   */
  private static class CasQueueEntry {
    private CAS cas;

    private Semaphore semaphore = null;

    public CasQueueEntry() {
      semaphore = new Semaphore(1);
      //  Acquire the semaphore to force getCAS() to block
      //  until setCAS() or clear() is called
      semaphore.acquireUninterruptibly();
    }
    public CAS getCas() {
    	//	Wait until setCAS() is called
      semaphore.acquireUninterruptibly();
      return cas;
    }
    public void setCas(CAS cas) {
      this.cas = cas;
      //	Release semaphore so that getCAS() can return a CAS instance
      semaphore.release();
    }
    //	Called when the client is stopping to
    //	release a semaphore
    public void clear() {
      cas = null;
      semaphore.release();
    }

  }

  protected void sendMetaRequest() throws Exception {
    PendingMessage msg = new PendingMessage(AsynchAEMessage.GetMeta);
    ClientRequest requestToCache = new ClientRequest(uniqueIdentifier, this); // , metadataTimeout);
    requestToCache.setIsRemote(remoteService);
    requestToCache.setMetaRequest(true);
    requestToCache.setMetadataTimeout(metadataTimeout);

    requestToCache.setEndpoint(getEndPointName());

    clientCache.put(uniqueIdentifier, requestToCache);
    
    // Add message to the pending queue
    addMessage(msg);
  }

  protected void waitForCpcReply() {
    try {
      // wait for CPC reply
      cpcReplySemaphore.acquire();
    } catch (InterruptedException e) {

    } finally {
      cpcReplySemaphore.release();
    }
  }

  /**
   * Blocks while trying to acquire a semaphore awaiting receipt of GetMeta Reply. When the GetMeta
   * is received, or there is a timeout, or the client stops the semaphore will be released.
   */
  protected void waitForMetadataReply() {
    try {
      getMetaSemaphore.acquire();
    } catch (InterruptedException e) {
    	e.printStackTrace();
    } 
  }

  public String getPerformanceReport() {
    return null;
  }

  public synchronized void process() throws ResourceProcessException {
    if (!initialized) {
      throw new ResourceProcessException();
    }
    if (collectionReader == null) {
      throw new ResourceProcessException();
    }
    if (!casQueueProducerReady) {
      serveCASes(); // start CAS producer thread
    }
    CAS cas = null;
    boolean hasNext = true;
    while (initialized && running ) {
      try {
         if ( (hasNext = collectionReader.hasNext()) == true) {
             cas = getCAS();
             collectionReader.getNext(cas);
             sendCAS(cas);
         } else {
           break;
         }
      } catch (Exception e) {
        e.printStackTrace();
		throw new ResourceProcessException(e);
      }
    }
    //	If the CR is done, enter a polling loop waiting for outstanding CASes to return
    //  from a service
    if (hasNext == false ) {
    	Object mObject = new Object();
    	//	if client is running and there are outstanding CASe go sleep for awhile and
    	//  try again, until all CASes come back from a service
    	while ( running && serviceDelegate.getCasPendingReplyListSize() > 0 ) {
    		synchronized(mObject) {
    			try {
        			mObject.wait(100);
    			} catch( Exception e) {
    		        e.printStackTrace();
    				throw new ResourceProcessException(e);
    			}
    		}
    	}
    	collectionProcessingComplete();
    }
  }

  protected ConcurrentHashMap<String, ClientRequest> getCache() {
    return clientCache;
  }

  /**
   * Sends a given CAS for analysis to the UIMA EE Service.
   * 
   */
  private String sendCAS(CAS aCAS, ClientRequest requestToCache) throws ResourceProcessException {
    synchronized (sendMux) {
      if ( requestToCache == null ) {
        throw new ResourceProcessException(new Exception("Invalid Process Request. Cache Entry is Null"));
      }
      String casReferenceId = requestToCache.getCasReferenceId();
      try {
        if (!running) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "sendCAS",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_not_sending_cas_INFO",
                    new Object[] { "Asynchronous Client is Stopping" });
          }
          return null;
        }

        clientCache.put(casReferenceId, requestToCache);
        PendingMessage msg = new PendingMessage(AsynchAEMessage.Process);
        long t1 = System.nanoTime();
        switch (serialFormat) {
        case XMI:
          XmiSerializationSharedData serSharedData = new XmiSerializationSharedData();
          String serializedCAS = serializeCAS(aCAS, serSharedData);
          msg.put(AsynchAEMessage.CAS, serializedCAS);
          if (remoteService) {  // always true 5/2013
            // Store the serialized CAS in case the timeout occurs and need to send the
            // the offending CAS to listeners for reporting
            requestToCache.setCAS(serializedCAS);
            requestToCache.setXmiSerializationSharedData(serSharedData);
          }
          break;
        case BINARY:
          byte[] serializedBinaryCAS = uimaSerializer.serializeCasToBinary(aCAS);
          msg.put(AsynchAEMessage.CAS, serializedBinaryCAS);
          break;
        case COMPRESSED_FILTERED:
          // can't use uimaserializer directly - project doesn't have ref to this one
          // for storing the reuse info
          BinaryCasSerDes6 bcs = new BinaryCasSerDes6(aCAS, this.getRemoteTypeSystem());
          ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
          bcs.serialize(baos);
          requestToCache.setCompress6ReuseInfo(bcs.getReuseInfo());           
          msg.put(AsynchAEMessage.CAS, baos.toByteArray());
          break;
        default:
          throw new UIMARuntimeException(new Exception("Internal Error"));    
        }
        
        requestToCache.setCAS(aCAS);

        requestToCache.setSerializationTime(System.nanoTime() - t1);
        msg.put(AsynchAEMessage.CasReference, casReferenceId);
        requestToCache.setIsRemote(remoteService);
        requestToCache.setEndpoint(getEndPointName());
        requestToCache.setProcessTimeout(processTimeout);
        requestToCache.clearTimeoutException();

        // The sendCAS() method is synchronized no need to synchronize the code below
        if (serviceDelegate.getState() == Delegate.TIMEOUT_STATE ) {
          SharedConnection sharedConnection = lookupConnection(getBrokerURI());
          
          //  Send Ping to service as getMeta request
          if ( !serviceDelegate.isAwaitingPingReply() && sharedConnection.isOpen() ) {
            serviceDelegate.setAwaitingPingReply();
            // Add the cas to a list of CASes pending reply. Also start the timer if necessary
			// serviceDelegate.addCasToOutstandingList(requestToCache.getCasReferenceId());
        	  
        	  // since the service is in time out state, we dont send CASes to it just yet. Instead, place
        	  // a CAS in a pending dispatch list. CASes from this list will be sent once a response to PING
        	  // arrives.
            serviceDelegate.addCasToPendingDispatchList(requestToCache.getCasReferenceId(), aCAS.hashCode(), timerPerCAS); 
            if ( cpcReadySemaphore.availablePermits() > 0 ) {
              acquireCpcReadySemaphore();
            }

            // Send PING Request to check delegate's availability
            sendMetaRequest();
            // @@@@@@@@@@@@@@@ Changed on 4/20 serviceDelegate.cancelDelegateTimer();
            // Start a timer for GetMeta ping and associate a cas id
            // with this timer. The delegate is currently in a timed out
            // state due to a timeout on a CAS with a given casReferenceId.
            //  
            serviceDelegate.startGetMetaRequestTimer(casReferenceId);
            
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "sendCAS",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_client_sending_ping__FINE",
                      new Object[] { serviceDelegate.getKey() });
            }
            return casReferenceId;
          } else {
            if ( !requestToCache.isSynchronousInvocation() && !sharedConnection.isOpen() ) {
              Exception exception = new BrokerConnectionException("Unable To Deliver CAS:"+requestToCache.getCasReferenceId()+" To Destination. Connection To Broker "+getBrokerURI()+" Has Been Lost");
              handleException(exception, requestToCache.getCasReferenceId(), null, requestToCache, true);
              return casReferenceId;
            } else {
              //  Add to the outstanding list.  
			  //  serviceDelegate.addCasToOutstandingList(requestToCache.getCasReferenceId());
        	  // since the service is in time out state, we dont send CASes to it just yet. Instead, place
        	  // a CAS in a pending dispatch list. CASes from this list will be sent once a response to PING
        	  // arrives.
              serviceDelegate.addCasToPendingDispatchList(requestToCache.getCasReferenceId(), aCAS.hashCode(), timerPerCAS);
              return casReferenceId;
            }
          }
        }
        SharedConnection sharedConnection = lookupConnection(getBrokerURI());
        if ( !sharedConnection.isOpen() ) {
          if (requestToCache != null && !requestToCache.isSynchronousInvocation() && aCAS != null ) {
            aCAS.release();
          }
          throw new ResourceProcessException(new BrokerConnectionException("Unable To Deliver Message To Destination. Connection To Broker "+sharedConnection.getBroker()+" Has Been Lost")); 
        }    
        // Incremented number of outstanding CASes sent to a service. When a reply comes
        // this counter is decremented
        outstandingCasRequests.incrementAndGet();
        // Increment total number of CASes sent to a service. This is reset
        // on CPC
        totalCasRequestsSentBetweenCpCs.incrementAndGet();
        // Add message to the pending queue
        addMessage(msg);
      } catch (ResourceProcessException e) {
        clientCache.remove(casReferenceId);
        throw e;
      } catch (Exception e) {
        clientCache.remove(casReferenceId);
        throw new ResourceProcessException(e);
      }
      return casReferenceId;
    }
  }

  /**
   * Checks the state of a delegate to see if it is in TIMEOUT State. If it is, push the CAS id onto
   * a list of CASes pending dispatch. The delegate is in a questionable state and the aggregate
   * sends a ping message to check delegate's availability. If the delegate responds to the ping,
   * all CASes in the pending dispatch list will be immediately dispatched.
   **/
  public boolean delayCasIfDelegateInTimedOutState(String aCasReferenceId, long casHashcode) throws AsynchAEException {
    if (serviceDelegate != null && serviceDelegate.getState() == Delegate.TIMEOUT_STATE) {
      // Add CAS id to the list of delayed CASes.
      serviceDelegate.addCasToPendingDispatchList(aCasReferenceId, casHashcode, timerPerCAS); 
      return true;
    }
    return false; // Cas Not Delayed
  }

  private ClientRequest produceNewClientRequestObject() {
    String casReferenceId = idGenerator.nextId();
    return new ClientRequest(casReferenceId, this);
  }

  /**
   * Sends a given CAS for analysis to the UIMA EE Service.
   * 
   */
  public synchronized String sendCAS(CAS aCAS) throws ResourceProcessException {
    if ( !running ) {
    	throw new ResourceProcessException(new UimaEEServiceException("Uima AS Client Has Been Stopped. Rejecting Request to Process CAS"));
    }
	  return this.sendCAS(aCAS, produceNewClientRequestObject());
  }

  /**
   * Handles response to CollectionProcessComplete request.
   * 
   * @throws Exception
   */
  protected void handleCollectionProcessCompleteReply(Message message) throws Exception {
    int payload = ((Integer) message.getIntProperty(AsynchAEMessage.Payload)).intValue();
    try {
      if (AsynchAEMessage.Exception == payload) {
        ProcessTrace pt = new ProcessTrace_impl();
        UimaASProcessStatusImpl status = new UimaASProcessStatusImpl(pt);
        Exception exception = retrieveExceptionFromMessage(message);

        status.addEventStatus("CpC", "Failed", exception);
        notifyListeners(null, status, AsynchAEMessage.CollectionProcessComplete);
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.INFO,
                  CLASS_NAME.getName(),
                  "handleCollectionProcessCompleteReply",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_received_exception_msg_INFO",
                  new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    getBrokerURI(),  
                    message.getStringProperty(AsynchAEMessage.CasReference), exception });
        }
      } else {
        // After receiving CPC reply there may be cleanup to do. Delegate this
        // to platform specific implementation (ActiveMQ or WAS)
        cleanup();
      }
    } catch (Exception e) {
      throw e;
    } finally {
      // Release the semaphore acquired in collectionProcessingComplete()
      cpcReplySemaphore.release();
    }
  }

  /**
   * Handles response to GetMeta Request. Deserializes ResourceMetaData and initializes CasManager.
   * 
   * @param message
   *          - jms message containing serialized ResourceMetaData
   * 
   * @throws Exception
   */
  protected void handleMetadataReply(Message message) throws Exception {

    serviceDelegate.cancelDelegateGetMetaTimer();
    serviceDelegate.setState(Delegate.OK_STATE);
    // check if the reply msg contains replyTo destination. It will be
    // added by the Cas Multiplier to the getMeta reply
    if (message.getJMSReplyTo() != null) {
      serviceDelegate.setFreeCasDestination(message.getJMSReplyTo());
    }
    // Check if this is a reply for a Ping sent in response to a timeout
    if (serviceDelegate.isAwaitingPingReply()) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.INFO,
                CLASS_NAME.getName(),
                "handleMetadataReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_rcvd_ping_reply__INFO",
                new Object[] { 
                  message.getStringProperty(AsynchAEMessage.MessageFrom),
                  message.getStringProperty(AsynchAEMessage.ServerIP)});
      }
      //  reset the state of the service. The client received its ping reply  
      serviceDelegate.resetAwaitingPingReply();
      String casReferenceId = null;
      if (serviceDelegate.getCasPendingReplyListSize() > 0 || serviceDelegate.getCasPendingDispatchListSize() > 0) {
    	  serviceDelegate.restartTimerForOldestCasInOutstandingList();
          //	We got a reply to GetMeta ping. Send all CASes that have been
          //    placed on the pending dispatch queue to a service.
    	  while( serviceDelegate.getState()==Delegate.OK_STATE && (casReferenceId = serviceDelegate.removeOldestFromPendingDispatchList()) != null ) {
    	        ClientRequest cachedRequest = (ClientRequest) clientCache.get(casReferenceId);
    	        if (cachedRequest != null) {
    	          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
    	            UIMAFramework.getLogger(CLASS_NAME).logrb(
    	                    Level.INFO,
    	                    CLASS_NAME.getName(),
    	                    "handleMetadataReply",
    	                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
    	                    "UIMAJMS_dispatch_delayed_cas__INFO",
    	                    new Object[] { casReferenceId, String.valueOf(cachedRequest.cas.hashCode())});
    	          }
    	          sendCAS(cachedRequest.getCAS(), cachedRequest);
    	        }
    	  }
      } else {
        ProcessTrace pt = new ProcessTrace_impl();
        UimaASProcessStatusImpl status = new UimaASProcessStatusImpl(pt);
        notifyListeners(null, status, AsynchAEMessage.GetMeta);
      }
      // Handled Ping reply
      return;
    }
    int payload = ((Integer) message.getIntProperty(AsynchAEMessage.Payload)).intValue();
    removeFromCache(uniqueIdentifier);

    try {
      if (AsynchAEMessage.Exception == payload) {
        ProcessTrace pt = new ProcessTrace_impl();
        UimaASProcessStatusImpl status = new UimaASProcessStatusImpl(pt);
        Exception exception = retrieveExceptionFromMessage(message);
        clientSideJmxStats.incrementMetaErrorCount();
        status.addEventStatus("GetMeta", "Failed", exception);
        notifyListeners(null, status, AsynchAEMessage.GetMeta);
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.INFO,
                  CLASS_NAME.getName(),
                  "handleMetadataReply",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_received_exception_msg_INFO",
                  new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                      getBrokerURI(),
                      message.getStringProperty(AsynchAEMessage.CasReference), exception });
        }
        abort = true;
        initialized = false;
      } else {
        // Check serialization supported by the service against client configuration.
        // If the client is configured to use Binary serialization *but* the service
        // doesnt support it, change the client serialization to xmi. Old services will
        // not return in a reply the type of serialization supported which implies "xmi".
        // New services *always* return "binary" or "compressedBinaryXXX" 
        // as a default serialization. The client
        // however may still want to serialize messages using xmi though.
        if (!message.propertyExists(AsynchAEMessage.SERIALIZATION)) {
          // Dealing with an old service here, check if there is a mismatch with the
          // client configuration. If the client is configured with binary serialization
          // override this and change serialization to "xmi".
          if (getSerialFormat() != SerialFormat.XMI) {
            // Override configured serialization
            setSerialFormat(SerialFormat.XMI);
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "handleMetadataReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_client_serialization_ovveride__WARNING", new Object[] {});
          }
        } else {
          final int c = message.getIntProperty(AsynchAEMessage.SERIALIZATION);
          if (getSerialFormat() != SerialFormat.XMI) {
            // don't override if XMI - because the remote may have different type system
              setSerialFormat((c == AsynchAEMessage.XMI_SERIALIZATION)    ? SerialFormat.XMI :
                              (c == AsynchAEMessage.BINARY_SERIALIZATION) ? SerialFormat.BINARY :
                                                                            SerialFormat.COMPRESSED_FILTERED);
          }
        }
        String meta = ((TextMessage) message).getText();
        ByteArrayInputStream bis = new ByteArrayInputStream(meta.getBytes());
        XMLInputSource in1 = new XMLInputSource(bis, null);
        // Adam - store ResouceMetaData in field so we can return it from getMetaData().
        resourceMetadata = (ProcessingResourceMetaData) UIMAFramework.getXMLParser()
                .parseResourceMetaData(in1);
        // if remote delegate, save type system 
        if (!brokerURI.startsWith("vm:")) { // test if remote
          setRemoteTypeSystem(AggregateAnalysisEngineController_impl.getTypeSystemImpl(resourceMetadata));
        }
        casMultiplierDelegate = resourceMetadata.getOperationalProperties().getOutputsNewCASes();
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "handleMetadataReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_handling_meta_reply_FINEST",
                  new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom), meta });
        }
        //  check the state of the client 
        if ( running && asynchManager != null ) {
          //  Merge the metadata only if the client is still running
          asynchManager.addMetadata(resourceMetadata);
        }
      }

    } catch (Exception e) {
      throw e;
    } finally {
      getMetaSemaphore.release();
    }
  }
  @SuppressWarnings("unchecked")
  protected void notifyListeners(CAS aCAS, EntityProcessStatus aStatus, int aCommand, String serializedComponentStats) {
    if ( aCommand == AsynchAEMessage.Process) {
      ((UimaASProcessStatusImpl)aStatus).
        setPerformanceMetrics(UimaSerializer.deserializePerformanceMetrics(serializedComponentStats));
      for (int i = 0; listeners != null && i < listeners.size(); i++) {
        UimaAsBaseCallbackListener statCL = (UimaAsBaseCallbackListener) listeners.get(i);
        statCL.entityProcessComplete(aCAS, aStatus);
      }
    }
  }
  protected void notifyListeners(CAS aCAS, EntityProcessStatus aStatus, int aCommand) {
    for (int i = 0; listeners != null && i < listeners.size(); i++) {
      UimaAsBaseCallbackListener statCL = (UimaAsBaseCallbackListener) listeners.get(i);
      switch (aCommand) {
        case AsynchAEMessage.GetMeta:
          statCL.initializationComplete(aStatus);
          break;

        case AsynchAEMessage.CollectionProcessComplete:
          statCL.collectionProcessComplete(aStatus);
          break;

        case AsynchAEMessage.Process:
        case AsynchAEMessage.Ping:
          statCL.entityProcessComplete(aCAS, aStatus);
          break;

      }

    }
  }

  protected void cancelTimer(String identifier) {
    ClientRequest request = null;
    if (clientCache.containsKey(identifier)) {
      request = (ClientRequest) clientCache.get(identifier);
      if (request != null) {
        request.cancelTimer();
      }
    }
  }

  private boolean isException(Message message) throws Exception {
    int payload;
    if (message.propertyExists(AsynchAEMessage.Payload)) {
      payload = ((Integer) message.getIntProperty(AsynchAEMessage.Payload)).intValue();
    } else {
      throw new InvalidMessageException("Message Does not Contain Payload property");
    }

    return (AsynchAEMessage.Exception == payload ? true : false);
  }

  private Exception retrieveExceptionFromMessage(Message message) throws Exception {
    Exception exception = null;
    try {
      if (message instanceof ObjectMessage
              && ((ObjectMessage) message).getObject() instanceof Exception) {
        exception = (Exception) ((ObjectMessage) message).getObject();
      } else if (message instanceof TextMessage) {
        exception = new UimaEEServiceException(((TextMessage) message).getText());
      }
    } catch( Exception e) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
              "retrieveExceptionFromMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_exception__WARNING", e);   
      exception = new UimaEEServiceException("UIMA AS client is unable to de-serialize Exception from a remote service",e);
    }
    return exception;
  }

  private void handleProcessReplyFromSynchronousCall(ClientRequest cachedRequest, Message message)
          throws Exception {
    // Save reply message in the cache
    cachedRequest.setMessage(message);
    wakeUpSendThread(cachedRequest);
  }

  protected void wakeUpSendThread(ClientRequest cachedRequest) throws Exception {
    if (threadMonitorMap.containsKey(cachedRequest.getThreadId())) {
      ThreadMonitor threadMonitor = (ThreadMonitor) threadMonitorMap.get(cachedRequest
              .getThreadId());
      // Unblock the sending thread so that it can complete processing
      // of the reply. The message has been stored in the cache and
      // when the thread wakes up due to notification below, it will
      // retrieve the reply and process it.
      if (threadMonitor != null) {
        cachedRequest.setReceivedProcessCasReply();
        threadMonitor.getMonitor().release();
      }
    } 

  }

  /**
   * Handles a ServiceInfo message returned from the Cas Multiplier. The primary purpose of this
   * message is to provide the client with a dedicated queue object where the client may send
   * messages to the specific CM service instance. An example of this would be a stop request that
   * client needs to send to the specific Cas Multiplier.
   * 
   * @param message
   *          - message received from a service
   * @throws Exception
   */
  protected void handleServiceInfo(Message message) throws Exception {
    String casReferenceId = message.getStringProperty(AsynchAEMessage.CasReference);
    ClientRequest casCachedRequest = null;
    if ( casReferenceId != null ) {
    	casCachedRequest = (ClientRequest) clientCache.get(casReferenceId);
    }
    try {
    	if ( casCachedRequest != null ) {
    	      //  entering user provided callback. Handle exceptions.
    	      UimaASProcessStatus status = new UimaASProcessStatusImpl(new ProcessTrace_impl(),casCachedRequest.getCAS(),
    	              casReferenceId);
    	      
    	      String nodeIP = message.getStringProperty(AsynchAEMessage.ServerIP);
    	      String pid = message.getStringProperty(AsynchAEMessage.UimaASProcessPID);
    	      if ( casReferenceId != null && nodeIP != null && pid != null) {
    	    	  if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
                      UIMAFramework.getLogger(CLASS_NAME).logrb(
                              Level.FINE,
                              CLASS_NAME.getName(),
                              "handleServiceInfo",
                              JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                              "UIMAJMS_calling_onBeforeProcessCAS_FINE",
                              new Object[] {
                            	  casReferenceId,
                             	 String.valueOf(casCachedRequest.getCAS().hashCode())
                              });
                  }
    	    	  onBeforeProcessCAS(status,nodeIP, pid);
    	    	  if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
                      UIMAFramework.getLogger(CLASS_NAME).logrb(
                              Level.FINE,
                              CLASS_NAME.getName(),
                              "handleServiceInfo",
                              JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                              "UIMAJMS_completed_onBeforeProcessCAS_FINE",
                              new Object[] {
                            	  casReferenceId,
                             	 String.valueOf(casCachedRequest.getCAS().hashCode())
                              });
                  }
    	     } 
   	      casCachedRequest.setHostIpProcessingCAS(message.getStringProperty(AsynchAEMessage.ServerIP));
   	      if (message.getJMSReplyTo() != null && serviceDelegate.isCasPendingReply(casReferenceId)) {
   	        casCachedRequest.setFreeCasNotificationQueue(message.getJMSReplyTo());
   	      }
    	} else {
    		
    		
    		ClientRequest requestToCache = (ClientRequest) clientCache.get(uniqueIdentifier);
    		if ( requestToCache != null && requestToCache.isMetaRequest()) {
        	  String nodeIP = message.getStringProperty(AsynchAEMessage.ServerIP);
      	      String pid = message.getStringProperty(AsynchAEMessage.UimaASProcessPID);
      	      if ( pid != null && nodeIP != null ) {
          	     UimaASProcessStatus status = new UimaASProcessStatusImpl(new ProcessTrace_impl(),null,
          	              casReferenceId);
          	      // notify client that the last request (GetMeta ) has been received by a service.
          	     onBeforeProcessMeta(nodeIP, pid);
      	      }
    			
    		}
    		
    	}

    } catch( Exception e) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
              "handleServiceInfo", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_exception__WARNING", e);
    }
  }
  protected void decrementOutstandingCasCounter() {
    // Received a reply, decrement number of outstanding CASes
    long outstandingCasCount = outstandingCasRequests.decrementAndGet();
    if (outstandingCasCount == 0) {
      cpcReadySemaphore.release();
    }
  }
  /**
   * Handles response to Process CAS request. If the message originated in a service that is running
   * in a separate jvm (remote), deserialize the CAS and notify the application of the completed
   * analysis via application listener.
   * 
   * @param message
   *          - jms message containing serialized CAS
   * 
   * @throws Exception
   */
  protected void handleProcessReply(Message message, boolean doNotify, ProcessTrace pt)
          throws Exception {
    if (!running) {
      return;
    }
    int payload = -1;
    String casReferenceId = message.getStringProperty(AsynchAEMessage.CasReference);
    // Determine the type of payload in the message (XMI,Cas Reference,Exception,etc)
    if (message.propertyExists(AsynchAEMessage.Payload)) {
      payload = ((Integer) message.getIntProperty(AsynchAEMessage.Payload)).intValue();
    }
    // Fetch entry from the client cache for a cas id returned from the service
    // The client cache maintains an entry for every outstanding CAS sent to the
    // service.
    ClientRequest cachedRequest = null;

    if (casReferenceId != null) {
      cachedRequest = (ClientRequest) clientCache.get(casReferenceId);
      // Increment number of replies
      if (cachedRequest != null && casReferenceId.equals(cachedRequest.getCasReferenceId())) {
        // Received a reply, decrement number of outstanding CASes
        decrementOutstandingCasCounter();
      }
      serviceDelegate.removeCasFromOutstandingList(casReferenceId);
    }
    if (AsynchAEMessage.Exception == payload) {
      handleException(message, cachedRequest, true);
      return;
    }
    // cachedRequest is only null if we are receiving child CASes from a 
    // Cas Multiplier. Otherwise, we drop the message as it is out of band
    if ( cachedRequest == null && !casMultiplierDelegate ) {
    	// most likely a reply came in after the thread was interrupted
    	return;
    }
    
    
    // If the Cas Reference id not in the message check if the message contains an
    // exception and if so, handle the exception and return.
    if (casReferenceId == null) {
      return;
    }

    if (message instanceof TextMessage
            && UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(
              Level.FINEST,
              CLASS_NAME.getName(),
              "handleProcessReply",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_handling_process_reply_FINEST",
              new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                  message.getStringProperty(AsynchAEMessage.CasReference),
                  message.toString() + ((TextMessage) message).getText() });
    }

    if (cachedRequest != null) {
      // Store the total latency for this CAS. The departure time is set right before the CAS
      // is sent to a service.
      cachedRequest.setTimeWaitingForReply(System.nanoTime() - cachedRequest.getCASDepartureTime());
  	  if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                "handleProcessReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_cas_reply_rcvd_FINE", new Object[] { casReferenceId, String.valueOf(cachedRequest.getCAS().hashCode())});
      }

      // If the CAS was sent from a synchronous API sendAndReceive(), wake up the thread that
      // sent the CAS and process the reply
      if (cachedRequest.isSynchronousInvocation()) {
        handleProcessReplyFromSynchronousCall(cachedRequest, message);
      } else {
        deserializeAndCompleteProcessingReply(casReferenceId, message, cachedRequest, pt, doNotify);
      }
    } else if (message.propertyExists(AsynchAEMessage.InputCasReference)) {
    	int command = message.getIntProperty(AsynchAEMessage.Command);
    	if (AsynchAEMessage.ServiceInfo != command) {
    	      handleProcessReplyFromCasMultiplier(message, casReferenceId, payload);
    	}
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        // Most likely expired message. Already handled as timeout. Discard the message and move on
        // to the next
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.INFO,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_received_expired_msg_INFO",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    message.getStringProperty(AsynchAEMessage.CasReference) });
      }
    }
  }

  private void handleProcessReplyFromCasMultiplier(Message message, String casReferenceId,
          int payload /* , ClientRequest inputCasCachedRequest */) throws Exception {
    // Check if the message contains a CAS that was generated by a Cas Multiplier. If so,
    // verify that the message also includes an input CAS id and that such input CAS id
    // exists in the client's cache.
    // Fetch the input CAS Reference Id from which the CAS being processed was generated from
    String inputCasReferenceId = message.getStringProperty(AsynchAEMessage.InputCasReference);
    // Fetch the destination for Free CAS notification
    Destination freeCASNotificationDestination = message.getJMSReplyTo();
    if (freeCASNotificationDestination != null) {
      TextMessage msg = createTextMessage();
      msg.setText("");
      setReleaseCASMessage(msg, casReferenceId);
      // Create Message Producer for the Destination
      MessageProducer msgProducer = getMessageProducer(freeCASNotificationDestination);
      if (msgProducer != null) {
        try {
          // Send FreeCAS message to a Cas Multiplier
          msgProducer.send(msg);
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINEST,
                    CLASS_NAME.getName(),
                    "handleProcessReplyFromCasMultiplier",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_client_sending_release_cas_FINEST",
                    new Object[] { freeCASNotificationDestination,
                        message.getStringProperty(AsynchAEMessage.CasReference) });
          }
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "handleProcessReplyFromCasMultiplier", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_error_while_sending_msg__WARNING",
                    new Object[] { "Free Cas Temp Destination", e });
          }
        }
      }
    }
    
    // Fetch an entry from the client cache for a given input CAS id. This would be an id
    // of the CAS that the client sent out to the service.
    ClientRequest inputCasCachedRequest = (ClientRequest) clientCache.get(inputCasReferenceId);
    if (inputCasCachedRequest == null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        // Most likely expired message. Already handled as timeout. Discard the message and move on
        // to the next
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.INFO,
                CLASS_NAME.getName(),
                "handleProcessReplyFromCasMultiplier",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_received_expired_msg_INFO",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    message.getStringProperty(AsynchAEMessage.CasReference) });
      }
      return;
    }
    if (inputCasCachedRequest.isSynchronousInvocation()) {
    	// with synchronous invocation, child CASes are thrown away. With sync API, the UIMA-AS client
    	// is not using callbacks. 
    	if ( casReferenceId.equals(inputCasCachedRequest.getCasReferenceId())) {
    	      handleProcessReplyFromSynchronousCall(inputCasCachedRequest, message);
    	} else {
    		return;
    	}
    }
    CAS cas = null;
    if (message instanceof TextMessage) {
      cas = deserializeCAS(((TextMessage) message).getText(), SHADOW_CAS_POOL);
    } else {
      long bodyLength = ((BytesMessage) message).getBodyLength();
      byte[] serializedCas = new byte[(int) bodyLength];
      ((BytesMessage) message).readBytes(serializedCas);
      cas = deserializeCAS(serializedCas, SHADOW_CAS_POOL);

    }
    completeProcessingReply(cas, casReferenceId, payload, true, message, inputCasCachedRequest,
            null);
  }

  private boolean isShutdownException(Exception exception) throws Exception {
    if (exception != null) {
      if (exception instanceof ServiceShutdownException || exception.getCause() != null
              && exception.getCause() instanceof ServiceShutdownException) {
        return true;
      }
    }
    return false;
  }
  protected void handleNonProcessException(Exception exception ) 
  throws Exception {
    ProcessTrace pt = new ProcessTrace_impl();
    UimaASProcessStatusImpl status = new UimaASProcessStatusImpl(pt);
    clientSideJmxStats.incrementMetaErrorCount();
    status.addEventStatus("GetMeta", "Failed", exception);
    notifyListeners(null, status, AsynchAEMessage.GetMeta);
  }
  protected void handleException(Exception exception, String casReferenceId, String inputCasReferenceId, ClientRequest cachedRequest, boolean doNotify)
  throws Exception {
    handleException(exception, casReferenceId, inputCasReferenceId, cachedRequest, doNotify, true);
  }
  protected void handleException(Exception exception, String casReferenceId, String inputCasReferenceId, ClientRequest cachedRequest, boolean doNotify, boolean rethrow)
          throws Exception {
    if (!isShutdownException(exception)) {
      clientSideJmxStats.incrementProcessErrorCount();
    }
    if (exception != null && cachedRequest != null) {
      cachedRequest.setException(exception);
      cachedRequest.setProcessException();
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(
              Level.INFO,
              CLASS_NAME.getName(),
              "handleException",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_received_exception_msg_INFO",
              new Object[] { serviceDelegate.getComponentName(),
                getBrokerURI(),
                casReferenceId, exception });
    }
    try {
      if (doNotify) {
        ProcessTrace pt = new ProcessTrace_impl();

        // HACK! Service should only send exceptions for CASes that we sent.
        // Check if this or its input parent is known.
        if (inputCasReferenceId != null) {
          serviceDelegate.removeCasFromOutstandingList(inputCasReferenceId);
        } else if (casReferenceId != null ) {
          serviceDelegate.removeCasFromOutstandingList(casReferenceId);
        }
        UimaASProcessStatusImpl status;
        
        if (cachedRequest != null ) {
            // Add Cas referenceId(s) to enable matching replies with requests (ids may be null)
            status = new UimaASProcessStatusImpl(pt, cachedRequest.getCAS(), casReferenceId,
                    inputCasReferenceId);
        } else {
            status = new UimaASProcessStatusImpl(pt, null, casReferenceId,
                    inputCasReferenceId);
        }
        status.addEventStatus("Process", "Failed", exception);
        if (cachedRequest != null && !cachedRequest.isSynchronousInvocation()
                && cachedRequest.getCAS() != null) {
          notifyListeners(cachedRequest.getCAS(), status, AsynchAEMessage.Process);
        } else {
          notifyListeners(null, status, AsynchAEMessage.Process);
        }
        // Done here
        return;
      } else {
        if ( rethrow ) {
          throw new ResourceProcessException(exception);
        }
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (cachedRequest != null) {
        if (cachedRequest.isSynchronousInvocation() && cachedRequest.isProcessException()) {
          // Wake up the send thread that is blocking waiting for a reply. When the thread
          // receives the signal, it checks if the reply contains an exception and will
          // not return control back to the client
          wakeUpSendThread(cachedRequest);
        }
        // Dont release the CAS if the application uses synchronous API
        if (!cachedRequest.isSynchronousInvocation() && cachedRequest.getCAS() != null) {
          cachedRequest.getCAS().release();
        }
      }
      removeFromCache(casReferenceId);
      serviceDelegate.removeCasFromOutstandingList(casReferenceId);
      decrementOutstandingCasCounter();
    }
  }
  
  protected void handleException(Message message, ClientRequest cachedRequest, boolean doNotify)
          throws Exception {
    
    Exception exception = retrieveExceptionFromMessage(message);
    String casReferenceId = message.getStringProperty(AsynchAEMessage.CasReference);
    String inputCasReferenceId = message.getStringProperty(AsynchAEMessage.InputCasReference);
    handleException(exception, casReferenceId, inputCasReferenceId, cachedRequest, doNotify);
  }

  
  private void completeProcessingReply(CAS cas, String casReferenceId, int payload,
          boolean doNotify, Message message, ClientRequest cachedRequest, ProcessTrace pt)
          throws Exception {
    if (AsynchAEMessage.XMIPayload == payload || AsynchAEMessage.BinaryPayload == payload
            || AsynchAEMessage.CASRefID == payload) {
      if (pt == null) {
        pt = new ProcessTrace_impl();
      }
      try {
        // Log stats and populate ProcessTrace object
        logTimingInfo(message, pt, cachedRequest);
        if (doNotify) {
          UimaASProcessStatusImpl status;
          String inputCasReferenceId = message.getStringProperty(AsynchAEMessage.InputCasReference);
          if (inputCasReferenceId != null
                  && inputCasReferenceId.equals(cachedRequest.getCasReferenceId())) {
            status = new UimaASProcessStatusImpl(pt, cas,casReferenceId, inputCasReferenceId);
          } else {
            status = new UimaASProcessStatusImpl(pt, cas, casReferenceId);
          }
          if ( message.propertyExists(AsynchAEMessage.CASPerComponentMetrics)) {
            // Add CAS identifier to enable matching replies with requests
            notifyListeners(cas, status, AsynchAEMessage.Process, message.getStringProperty(AsynchAEMessage.CASPerComponentMetrics));
          } else {
            // Add CAS identifier to enable matching replies with requests
            notifyListeners(cas, status, AsynchAEMessage.Process);
          }
        } else {  // synchronous sendAndReceive() was used
            if (casReferenceId != null && message.propertyExists(AsynchAEMessage.CASPerComponentMetrics) ) {
                cachedRequest = (ClientRequest) clientCache.get(casReferenceId);
                if ( cachedRequest != null && cachedRequest.getComponentMetricsList() != null ) {
                	cachedRequest.getComponentMetricsList().
                		addAll(UimaSerializer.deserializePerformanceMetrics(message.getStringProperty(AsynchAEMessage.CASPerComponentMetrics)));
                }
            }
        }
      } finally {
        // Dont release the CAS if the application uses synchronous API
        if (remoteService && !cachedRequest.isSynchronousInvocation()) {
          if (cas != null) {
            cas.release();
          }
        }
        
        removeFromCache(casReferenceId);
      }
    }
  }

  private void logTimingInfo(Message message, ProcessTrace pt, ClientRequest cachedRequest)
          throws Exception {
    clientSideJmxStats.incrementTotalNumberOfCasesProcessed();

    if (message.getStringProperty(AsynchAEMessage.CasReference) != null) {
      String casReferenceId = message.getStringProperty(AsynchAEMessage.CasReference);
      if (clientCache.containsKey(casReferenceId)) {
        ClientRequest cacheEntry = (ClientRequest) clientCache.get(casReferenceId);
        if (cacheEntry == null) {
          return;
        }
        // Add time waiting for reply to the client JMX stats
        long timeWaitingForReply = cacheEntry.getTimeWaitingForReply();
        clientSideJmxStats.incrementTotalTimeWaitingForReply(timeWaitingForReply);
        // Add CAS response latency time to the client JMX stats
        long responseLatencyTime = cacheEntry.getSerializationTime() + timeWaitingForReply
                + cacheEntry.getDeserializationTime();
        clientSideJmxStats.incrementTotalResponseLatencyTime(responseLatencyTime);
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME)
                  .logrb(
                          Level.FINEST,
                          CLASS_NAME.getName(),
                          "handleProcessReply",
                          JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAJMS_timer_detail_FINEST",
                          new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                              "Total Time Waiting For Reply",
                              (float) timeWaitingForReply / (float) 1000000 });
        }
        pt.addEvent("UimaEE", "process", "Total Time Waiting For Reply",
                (int) (timeWaitingForReply / 1000000), "");
      }
    }
    if (message.propertyExists(AsynchAEMessage.TimeToSerializeCAS)) {
      long timeToSerializeCAS = message.getLongProperty(AsynchAEMessage.TimeToSerializeCAS);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_timer_detail_FINEST",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    "Time To Serialize Cas", (float) timeToSerializeCAS / (float) 1000000 });
      }
      pt.addEvent("UimaEE", "process", "Time To Serialize Cas",
              (int) (timeToSerializeCAS / 1000000), "");
      // Add the client serialization overhead to the value returned from a service
      timeToSerializeCAS += cachedRequest.getSerializationTime();
      clientSideJmxStats.incrementTotalSerializationTime(timeToSerializeCAS);
    }
    if (message.propertyExists(AsynchAEMessage.TimeToDeserializeCAS)) {
      long timeToDeserializeCAS = message.getLongProperty(AsynchAEMessage.TimeToDeserializeCAS);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_timer_detail_FINEST",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    "Time To Deserialize Cas", (float) timeToDeserializeCAS / (float) 1000000 });
      }
      pt.addEvent("UimaEE", "process", "Time To Deserialize Cas",
              (int) (timeToDeserializeCAS / 1000000), "");
      // Add the client deserialization overhead to the value returned from a service
      timeToDeserializeCAS += cachedRequest.getDeserializationTime();
      clientSideJmxStats.incrementTotalDeserializationTime(timeToDeserializeCAS);
    }
    if (message.propertyExists(AsynchAEMessage.TimeWaitingForCAS)) {
      long timeWaitingForCAS = message.getLongProperty(AsynchAEMessage.TimeWaitingForCAS);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_timer_detail_FINEST",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    "Time to Wait for CAS", (float) timeWaitingForCAS / (float) 1000000 });
      }
      pt.addEvent("UimaEE", "process", "Time to Wait for CAS", (int) (timeWaitingForCAS / 1000000),
              "");
    }
    if (message.propertyExists(AsynchAEMessage.TimeInService)) {
      long ttimeInService = message.getLongProperty(AsynchAEMessage.TimeInService);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_timer_detail_FINEST",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    "Time In Service", (float) ttimeInService / (float) 1000000 });
      }
      pt.addEvent("UimaEE", "process", "Time In Service", (int) (ttimeInService / 1000000), "");

    }
    if (message.propertyExists(AsynchAEMessage.TotalTimeSpentInAnalytic)) {
      long totaltimeInService = message.getLongProperty(AsynchAEMessage.TotalTimeSpentInAnalytic);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_timer_detail_FINEST",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    "Total Time In Service", (float) totaltimeInService / (float) 1000000 });
      }
      pt.addEvent("UimaEE", "process", "Total Time In Service",
              (int) (totaltimeInService / 1000000), "");
    }
    if (message.propertyExists(AsynchAEMessage.TimeInProcessCAS)) {
      long totaltimeInProcessCAS = message.getLongProperty(AsynchAEMessage.TimeInProcessCAS);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_timer_detail_FINEST",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    "Total Time In Process CAS", (float) totaltimeInProcessCAS / (float) 1000000 });
      }
      float timeInMillis = (float) totaltimeInProcessCAS / (float) 1000000;
      pt.addEvent("UimaEE", "process", "Total Time In Process CAS", (int) timeInMillis, "");
      clientSideJmxStats.incrementTotalTimeToProcess(totaltimeInProcessCAS);
    }
    if (message.propertyExists(AsynchAEMessage.IdleTime)) {
      long totalIdletime = message.getLongProperty(AsynchAEMessage.IdleTime);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "handleProcessReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_timer_detail_FINEST",
                new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom),
                    "Idle Time Waiting For CAS", (float) totalIdletime / (float) 1000000 });
      }
      pt.addEvent("UimaEE", "process", "Idle Time Waiting For CAS",
              (int) (totalIdletime / 1000000), "");
      clientSideJmxStats.incrementTotalIdleTime(totalIdletime);
    }
    if (message.propertyExists(AsynchAEMessage.ServerIP)) {
      pt.addEvent("UimaEE", "process", "Service IP", 0, message
              .getStringProperty(AsynchAEMessage.ServerIP));
    }

  }

  protected void removeFromCache(String aCasReferenceId) {
    if (aCasReferenceId != null && clientCache.containsKey(aCasReferenceId)) {
      ClientRequest requestToCache = (ClientRequest) clientCache.get(aCasReferenceId);
      if (requestToCache != null) {
        requestToCache.removeEntry(aCasReferenceId);
      }
      clientCache.remove(aCasReferenceId);
    }
  }

  protected CAS deserialize(String aSerializedCAS, CAS aCAS) throws Exception {
    XmiSerializationSharedData deserSharedData = new XmiSerializationSharedData();
    uimaSerializer.deserializeCasFromXmi(aSerializedCAS, aCAS, deserSharedData, true, -1);
    return aCAS;
  }

  protected CAS deserialize(String aSerializedCAS, CAS aCAS,
          XmiSerializationSharedData deserSharedData, boolean deltaCas) throws Exception {
    if (deltaCas) {
      uimaSerializer.deserializeCasFromXmi(aSerializedCAS, aCAS, deserSharedData, true,
              deserSharedData.getMaxXmiId(), AllowPreexistingFS.allow);
    } else {
      uimaSerializer.deserializeCasFromXmi(aSerializedCAS, aCAS, deserSharedData, true, -1);
    }
    return aCAS;
  }

  protected CAS deserialize(byte[] binaryData, ClientRequest cachedRequest) throws Exception {
    CAS cas = cachedRequest.getCAS();
    uimaSerializer.deserializeCasFromBinary(binaryData, cas);
    return cas;
  }

  protected CAS deserializeCAS(String aSerializedCAS, ClientRequest cachedRequest) throws Exception {
    CAS cas = cachedRequest.getCAS();
    return deserialize(aSerializedCAS, cas);
  }

  /**
   * handle both ordinary binary and compressed6 binary
   * @param aSerializedCAS
   * @param cachedRequest
   * @return
   * @throws Exception
   */
  protected CAS deserializeCAS(byte[] aSerializedCAS, ClientRequest cachedRequest) throws Exception {
    CAS cas = cachedRequest.getCAS();
    ReuseInfo reuseInfo = cachedRequest.getCompress6ReuseInfo();
    ByteArrayInputStream bais = new ByteArrayInputStream(aSerializedCAS);
    if (reuseInfo != null) {
      Serialization.deserializeCAS(cas, bais, null, reuseInfo);
    } else {
      ((CASImpl)cas).reinit(bais);
    }
//    uimaSerializer.deserializeCasFromBinary(aSerializedCAS, cas);
    return cas;
  }

  // never called 5/2013 ??
  protected CAS deserializeCAS(byte[] aSerializedCAS, CAS aCas) throws Exception {
    uimaSerializer.deserializeCasFromBinary(aSerializedCAS, aCas);
    return aCas;
  }

  protected CAS deserializeCAS(String aSerializedCAS, ClientRequest cachedRequest, boolean deltaCas)
          throws Exception {
    CAS cas = cachedRequest.getCAS();
    return deserialize(aSerializedCAS, cas, cachedRequest.getXmiSerializationSharedData(), deltaCas);
  }

  protected CAS deserializeCAS(String aSerializedCAS, String aCasPoolName) throws Exception {
    CAS cas = asynchManager.getNewCas(aCasPoolName);
    return deserialize(aSerializedCAS, cas);
  }

  protected CAS deserializeCAS(byte[] aSerializedCAS, String aCasPoolName) throws Exception {
    CAS cas = asynchManager.getNewCas(aCasPoolName);
    uimaSerializer.deserializeCasFromBinary(aSerializedCAS, cas);
    return cas;
  }

  /**
   * Listener method receiving JMS Messages from the response queue.
   * 
   */
  public void onMessage(final Message message) {
    // Process message in a separate thread. Previously the message was processed in ActiveMQ dispatch thread.
    // This onMessage() method is called by ActiveMQ code from a critical region protected with a lock. The lock 
    // is only released if this method returns. Running in a dispatch thread caused a hang when an application
    // decided to call System.exit() in any of its callback listener methods. The UIMA AS client adds a 
    // ShutdownHoook to the JVM to enable orderly shutdown which includes stopping JMS Consumer, JMS Producer
    // and finally stopping JMS Connection. The ShutdownHook support was added to the client in case the 
    // application doesnt call client's stop() method. Now, the hang was caused by the fact that the dispatch
    // thread was used to call System.exit() which in turn executed client's ShutdownHook code. The ShutdownHook
    // code runs in a separate thread, but the the JVM blocks the dispatch thread until the ShutdownHook 
    // finishes. It never will though, since the ShutdownHook is calling ActiveMQSession.close() which tries to enter
    // the same critical region that the dispatch thread is still stuck into. DEADLOCK.
    // The code below uses a simple FixedThreadPool Executor with a single thread. This thread is reused instead
    // creating one on the fly.
    exec.execute( new Runnable() {
      
      public void run() {
        try {

          
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "onMessage",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_received_msg_FINEST",
                    new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom) });
          }
          if (!message.propertyExists(AsynchAEMessage.Command)) {
            return;
          }

          int command = message.getIntProperty(AsynchAEMessage.Command);
          if (AsynchAEMessage.CollectionProcessComplete == command) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "onMessage",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_received_cpc_reply_FINE",
                      new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom) });
            }
            handleCollectionProcessCompleteReply(message);
          } else if (AsynchAEMessage.GetMeta == command) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "onMessage",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_received_meta_reply_FINE",
                      new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom) });
            }
            handleMetadataReply(message);
          } else if (AsynchAEMessage.Process == command) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "onMessage",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_received_process_reply_FINE",
                      new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom) });
            }
            handleProcessReply(message, true, null);
          } else if (AsynchAEMessage.ServiceInfo == command) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                      "onMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_received_service_info_FINEST",
                      new Object[] { message.getStringProperty(AsynchAEMessage.MessageFrom) });
            }
            handleServiceInfo(message);
          }
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "onMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", e);
          }

        }
        
      }
    });

  }

  /**
   * Gets the ProcessingResourceMetadata for the asynchronous AnalysisEngine.
   */
  public ProcessingResourceMetaData getMetaData() throws ResourceInitializationException {
    return resourceMetadata;
  }
  public String sendAndReceiveCAS(CAS aCAS) throws ResourceProcessException {
    return sendAndReceiveCAS(aCAS, null, null);
  }
  public String sendAndReceiveCAS(CAS aCAS, ProcessTrace pt) throws ResourceProcessException {
    return sendAndReceiveCAS(aCAS, pt, null);
  }
  public String sendAndReceiveCAS(CAS aCAS, List<AnalysisEnginePerformanceMetrics> componentMetricsList) throws ResourceProcessException {
    return sendAndReceiveCAS(aCAS, null, componentMetricsList);
  }

  /**
   * This is a synchronous method which sends a message to a destination and blocks waiting for a
   * reply.
   */
  public String sendAndReceiveCAS(CAS aCAS, ProcessTrace pt, List<AnalysisEnginePerformanceMetrics> componentMetricsList) throws ResourceProcessException {
    if (!running) {
      throw new ResourceProcessException(new Exception("Uima EE Client Not In Running State"));
    }
    if (!serviceDelegate.isSynchronousAPI()) {
      // Change the flag to indicate synchronous invocation.
      // This info will be needed to handle Ping replies.
      // Different code is used for handling PING replies for
      // sync and async API.
      serviceDelegate.setSynchronousAPI();
    }
    String casReferenceId = null;
    // keep handle to CAS, we'll deserialize into this same CAS later
    sendAndReceiveCAS = aCAS;

    ThreadMonitor threadMonitor = null;

    if (threadMonitorMap.containsKey(Thread.currentThread().getId())) {
      threadMonitor = (ThreadMonitor) threadMonitorMap.get(Thread.currentThread().getId());
    } else {
      threadMonitor = new ThreadMonitor(Thread.currentThread().getId());
      threadMonitorMap.put(Thread.currentThread().getId(), threadMonitor);
    }

    ClientRequest cachedRequest = produceNewClientRequestObject();
    cachedRequest.setSynchronousInvocation();
    
    //	save application provided List where the performance stats will be copied
    //  when reply comes back
    cachedRequest.setComponentMetricsList(componentMetricsList);
    
    // This is synchronous call, acquire and hold the semaphore before
    // dispatching a CAS to a service. The semaphore will be released
    // iff:
    // a) reply is received (success or failure with exception)
    // b) timeout occurs
    // c) client is stopped
    // Once the semaphore is acquired and the CAS is dispatched
    // the thread will block in trying to acquire the semaphore again
    // below.
    if (threadMonitor != null && threadMonitor.getMonitor() != null) {
      try {
        threadMonitor.getMonitor().acquire();
      } catch (InterruptedException e) {
      	if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                    "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_client_interrupted_INFO", new Object[] { casReferenceId, String.valueOf(aCAS.hashCode())});
        }
    	// cancel the timer if it is associated with a CAS this thread is waiting for. This would be
    	// the oldest CAS submitted to a queue for processing. The timer will be canceled and restarted
    	// for the second oldest CAS in the outstanding list.
    	serviceDelegate.cancelTimerForCasOrPurge(casReferenceId);
    	throw new ResourceProcessException(e);
      } 
    }
    try {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_cas_submitting_FINE", new Object[] { casReferenceId, String.valueOf(aCAS.hashCode()), Thread.currentThread().getId()});
      }
      // send CAS. This call does not block. Instead we will block the sending thread below.
      casReferenceId = sendCAS(aCAS, cachedRequest);

    } catch( ResourceProcessException e) {
      
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", new Object[] { e });
      }
      threadMonitor.getMonitor().release();
      removeFromCache(casReferenceId);
      throw e;
    }
    if (threadMonitor != null && threadMonitor.getMonitor() != null) {
      while (running) {
        try {
          // Block sending thread until a reply is received. The thread
          // will be signaled either when a reply to the request just
          // sent is received OR a Ping reply was received. The latter
          // is necessary to allow handling of CASes delayed due to
          // a timeout. A previous request timed out and the service
          // state was changed to TIMEDOUT. While the service is in this
          // state all sending threads add outstanding CASes to the list
          // of CASes pending dispatch and each waits until the state
          // of the service changes to OK. The state is changed to OK
          // when the client receives a reply to a PING request. When
          // the Ping reply comes, the client will signal this thread.
          // The thread checks the list of CASes pending dispatch trying
          // to find an entry that matches ID of the CAS previously
          // delayed. If the CAS is found in the delayed list, it will
          // be removed from the list and send to the service for
          // processing. The 'wasSignaled' flag is only set when the
          // CAS reply is received. Ping reply logic does not change
          // this flag.
          threadMonitor.getMonitor().acquire();
          // Send thread was awoken by either process reply or ping reply
          // If the service is in the ok state and the CAS is in the
          // list of CASes pending dispatch, remove the CAS from the list
          // and send it to the service.
          if (cachedRequest.isTimeoutException() || cachedRequest.isProcessException()) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                      "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_process_exception_handler5__WARNING", new Object[] { String.valueOf(aCAS.hashCode()), Thread.currentThread().getId()});
            }
            // Handled outside of the while-loop below
            break;
          }
          if (running && serviceDelegate.getState() == Delegate.OK_STATE
                  && serviceDelegate.removeCasFromPendingDispatchList(casReferenceId)) {
            sendCAS(aCAS, cachedRequest);
          } else {
            break; // done here, received a reply or the client is not running
          }
        } catch (InterruptedException e) {
        	if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                        "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_client_interrupted_INFO", new Object[] { Thread.currentThread().getId(), casReferenceId, String.valueOf(aCAS.hashCode())});
          }
        	// try to remove from pending dispatch list. If not there, remove from pending reply list
        	if ( !serviceDelegate.removeCasFromPendingDispatchList(casReferenceId)) {
            serviceDelegate.removeCasFromOutstandingList(casReferenceId);
        	}
        	// cancel the timer if it is associated with a CAS this thread is waiting for. This would be
        	// the oldest CAS submitted to a queue for processing. The timer will be canceled and restarted
        	// for the second oldest CAS in the outstanding list.
        	serviceDelegate.cancelTimerForCasOrPurge(casReferenceId);
        	if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                        "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_client_canceled_timer_INFO", new Object[] { Thread.currentThread().getId(), casReferenceId, String.valueOf(aCAS.hashCode())});
            }
            removeFromCache(casReferenceId);
        	throw new ResourceProcessException(e);
        } finally {
          threadMonitor.getMonitor().release();
        }
      }
    } // if

    if (abort) {
      throw new ResourceProcessException(new RuntimeException("Uima AS Client API Stopping"));
    }
    // check if timeout exception
    if (cachedRequest.isTimeoutException()) {
      String qName="";
      try {
        qName = getEndPointName();
      } catch( Exception e) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
      }
     // Request To Process Cas Has Timed-out.  
      throw new ResourceProcessException(JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "" +
      		"UIMAJMS_process_timeout_WARNING", 
      		new Object[]{qName, getBrokerURI(), cachedRequest.getHostIpProcessingCAS()},
      		new UimaASProcessCasTimeout("UIMA AS Client Timed Out Waiting for Reply From Service:"+qName+" Broker:"+getBrokerURI()));
    }
    // If a reply contains process exception, throw an exception and let the
    // listener decide what happens next
    if (cachedRequest.isProcessException()) {
      String qName="";
      try {
        qName = getEndPointName();
      } catch( Exception e) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "sendAndReceiveCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
      }
      throw new ResourceProcessException(
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "" +
              "UIMAJMS_received_exception_msg_INFO",
              new Object[]{qName, getBrokerURI(), casReferenceId},
              cachedRequest.getException());
    }
    try {
      // Process reply in the send thread
      Message message = cachedRequest.getMessage();
      if (message != null) {
        deserializeAndCompleteProcessingReply(casReferenceId, message, cachedRequest, pt, false);
      }
    } catch (ResourceProcessException rpe) {
      throw rpe;
    } catch (Exception e) {
      throw new ResourceProcessException(e);
    }
    return casReferenceId;
  }

  private void deserializeAndCompleteProcessingReply(String casReferenceId, Message message,
          ClientRequest cachedRequest, ProcessTrace pt, boolean doNotify) throws Exception {
    if (!running) {
      return;
    }
    int payload = ((Integer) message.getIntProperty(AsynchAEMessage.Payload)).intValue();
    if (message.propertyExists(AsynchAEMessage.CasSequence)) {
      handleProcessReplyFromCasMultiplier(message, casReferenceId, payload);// , cachedRequest);
    } else {
      long t1 = System.nanoTime();
      boolean deltaCas = false;
      if (message.propertyExists(AsynchAEMessage.SentDeltaCas)) {
        deltaCas = message.getBooleanProperty(AsynchAEMessage.SentDeltaCas);
      }
      CAS cas = null;
      if (message instanceof TextMessage) {
        cas = deserializeCAS(((TextMessage) message).getText(), cachedRequest, deltaCas);
      } else {
        long bodyLength = ((BytesMessage) message).getBodyLength();
        byte[] serializedCas = new byte[(int) bodyLength];
        ((BytesMessage) message).readBytes(serializedCas);
        cas = deserializeCAS(serializedCas, cachedRequest);
      }
      cachedRequest.setDeserializationTime(System.nanoTime() - t1);
      completeProcessingReply(cas, casReferenceId, payload, doNotify, message, cachedRequest, pt);
    }
  }

  protected void notifyOnTimout(CAS aCAS, String anEndpoint, int aTimeoutKind, String casReferenceId) {

    ProcessTrace pt = new ProcessTrace_impl();
    UimaASProcessStatusImpl status = new UimaASProcessStatusImpl(pt, aCAS, casReferenceId);

    switch (aTimeoutKind) {
      case (MetadataTimeout):
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "notifyOnTimout", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_meta_timeout_WARNING", new Object[] { anEndpoint });
        }
        status.addEventStatus("GetMeta", "Failed", new UimaASMetaRequestTimeout("UIMA AS Client Timed Out Waiting For GetMeta Reply From a Service On Queue:"+anEndpoint));
        notifyListeners(null, status, AsynchAEMessage.GetMeta);
        abort = true;
        getMetaSemaphore.release();
        break;
      case (PingTimeout):
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "notifyOnTimout", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_meta_timeout_WARNING", new Object[] { anEndpoint });
        }
        status.addEventStatus("Ping", "Failed", new UimaASPingTimeout("UIMA AS Client Timed Out Waiting For Ping Reply From a Service On Queue:"+anEndpoint));
        notifyListeners(null, status, AsynchAEMessage.Ping);
        // The main thread could be stuck waiting for a CAS. Grab any CAS in the
        // client cache and release it so that we can shutdown.
        if (!clientCache.isEmpty()) {
          ClientRequest anyCasRequest = clientCache.elements().nextElement();
          if (anyCasRequest.getCAS() != null) {
            anyCasRequest.getCAS().release();
          }
        }
        abort = true;
        break;
      case (CpCTimeout):
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "notifyOnTimout", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_cpc_timeout_INFO", new Object[] { anEndpoint });
        }
        status.addEventStatus("CpC", "Failed", 
                new UimaASCollectionProcessCompleteTimeout("UIMA AS Client Timed Out Waiting For CPC Reply From a Service On Queue:"+anEndpoint));
        // release the semaphore acquired in collectionProcessingComplete()
        cpcReplySemaphore.release();
        notifyListeners(null, status, AsynchAEMessage.CollectionProcessComplete);
        break;

      case (ProcessTimeout):
    	  if ( casReferenceId != null ) {
    	        ClientRequest cachedRequest = (ClientRequest) clientCache.get(casReferenceId);
    	        if (cachedRequest != null) {
    	          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
    	            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
    	                  "notifyOnTimout", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
    	                  "UIMAJMS_process_timeout_WARNING", new Object[] { anEndpoint, getBrokerURI(), cachedRequest.getHostIpProcessingCAS() });
    	          }
    	        } else {
    	          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
    	            // if missing for any reason ...
    	            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
    	                    "notifyOnTimout", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
    	                    "UIMAJMS_received_expired_msg_INFO",
    	                    new Object[] { anEndpoint, casReferenceId });
    	          }
    	          return;
    	        }
    	        // Store the total latency for this CAS. The departure time is set right before the CAS
    	        // is sent to a service.
    	        cachedRequest.setTimeWaitingForReply(System.nanoTime()
    	                - cachedRequest.getCASDepartureTime());

    	        // mark timeout exception
    	        cachedRequest.setTimeoutException();

    	        if (cachedRequest.isSynchronousInvocation()) {
    	          // Signal a thread that we received a reply, if in the map
    	          if (threadMonitorMap.containsKey(cachedRequest.getThreadId())) {
    	            ThreadMonitor threadMonitor = (ThreadMonitor) threadMonitorMap.get(cachedRequest
    	                    .getThreadId());
    	            // Unblock the sending thread so that it can complete processing with an error
    	            if (threadMonitor != null) {
    	              threadMonitor.getMonitor().release();
    	              cachedRequest.setReceivedProcessCasReply(); // should not be needed
    	            }
    	          } 
    	        } else {
    	          // notify the application listener with the error
    	          if ( serviceDelegate.isPingTimeout()) {
    	            exc = new UimaASProcessCasTimeout(new UimaASPingTimeout("UIMA AS Client Ping Time While Waiting For Reply From a Service On Queue:"+anEndpoint));
    	            serviceDelegate.resetPingTimeout();
    	          } else {
    	            exc = new UimaASProcessCasTimeout("UIMA AS Client Timed Out Waiting For CAS:"+casReferenceId+ " Reply From a Service On Queue:"+anEndpoint);
    	          }
    	          status.addEventStatus("Process", "Failed", exc);
    	          notifyListeners(aCAS, status, AsynchAEMessage.Process);
    	        }
    	        boolean isSynchronousCall = cachedRequest.isSynchronousInvocation();
    	        
    	        cachedRequest.removeEntry(casReferenceId);
    	        serviceDelegate.removeCasFromOutstandingList(casReferenceId);
    	        // Check if all replies have been received
    	        long outstandingCasCount = outstandingCasRequests.decrementAndGet();
    	        if (outstandingCasCount == 0) {
    	          cpcReadySemaphore.release();
    	        }
    	        //	
    	        if ( !isSynchronousCall && serviceDelegate.getCasPendingReplyListSize() > 0) {
    	            String nextOutstandingCasReferenceId = 
    	            		serviceDelegate.getOldestCasIdFromOutstandingList();
    	        	if ( nextOutstandingCasReferenceId != null ) {
    	        		cachedRequest = (ClientRequest) clientCache.get(nextOutstandingCasReferenceId);
    	        		if ( cachedRequest != null && cachedRequest.getCAS() != null ) {
        	        		try {
        	            		sendCAS(cachedRequest.getCAS());
        	        		} catch( Exception e) {
        	        			UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
        	                            "notifyOnTimout", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
        	                            "UIMAEE_exception__WARNING", e);
        	        		}
    	        		}
    	        	}
    	        }
    	  }
        break;
    } // case
  }

  /**
   * @override
   */
  protected MessageProducer getMessageProducer(Destination destination) throws Exception {
    return null;
  }

  /**
   * has some cache values for CAS, similar to class CacheEntry 
   *
   */
  public class ClientRequest {
    private Timer timer = null;

    private long processTimeout = 0L;

    private long metadataTimeout = 0L;

    private long cpcTimeout = 0L;

    private String casReferenceId = null;

    private BaseUIMAAsynchronousEngineCommon_impl uimaEEEngine = null;

    private volatile boolean isSerializedCAS;

    private String serializedCAS;

    private CAS cas;

    private volatile boolean isMetaRequest = false;

    private volatile boolean isCPCRequest = false;

    private volatile boolean isRemote = true;

    private String endpoint;

    private long threadId = Thread.currentThread().getId();
    private Message message;

    private volatile boolean synchronousInvocation;

    private volatile boolean timeoutException;

    private long casDepartureTime;

    private long timeWaitingForReply;

    private long serializationTime;

    private long deserializationTime;

    private long metaTimeoutErrorCount;

    private long processTimeoutErrorCount;

    private long processErrorCount;

    private XmiSerializationSharedData sharedData;
    
    private ReuseInfo compress6ReuseInfo;

    private byte[] binaryCas = null;

    private volatile boolean isBinaryCas = false;

    private Exception exception;

    private volatile boolean processException;

    private Destination freeCasNotificationQueue = null;

    private String hostIpProcessingCAS;
    
    List<AnalysisEnginePerformanceMetrics> componentMetricsList;
    
    public List<AnalysisEnginePerformanceMetrics> getComponentMetricsList() {
		return componentMetricsList;
	}

	public void setComponentMetricsList(
			List<AnalysisEnginePerformanceMetrics> componentMetricsList) {
		this.componentMetricsList = componentMetricsList;
	}

	public String getHostIpProcessingCAS() {
      return hostIpProcessingCAS;
    }

    public void setHostIpProcessingCAS(String hostIpProcessingCAS) {
      this.hostIpProcessingCAS = hostIpProcessingCAS;
    }

    public Destination getFreeCasNotificationQueue() {
      return freeCasNotificationQueue;
    }

    public void setFreeCasNotificationQueue(Destination freeCasNotificationQueue) {
      this.freeCasNotificationQueue = freeCasNotificationQueue;
    }

    public boolean isProcessException() {
      return processException;
    }

    public void setProcessException() {
      this.processException = true;
    }
    

    public Exception getException() {
      return exception;
    }

    public void setException(Exception exception) {
      this.exception = exception;
    }

    public long getMetaTimeoutErrorCount() {
      return metaTimeoutErrorCount;
    }

    public void setMetaTimeoutErrorCount(long timeoutErrorCount) {
      metaTimeoutErrorCount = timeoutErrorCount;
    }

    public long getProcessTimeoutErrorCount() {
      return processTimeoutErrorCount;
    }

    public void setProcessTimeoutErrorCount(long timeoutErrorCount) {
      processTimeoutErrorCount = timeoutErrorCount;
    }

    public long getProcessErrorCount() {
      return processErrorCount;
    }

    public void setProcessErrorCount(long processErrorCount) {
      this.processErrorCount = processErrorCount;
    }

    public long getSerializationTime() {
      return serializationTime;
    }

    public void setSerializationTime(long serializationTime) {
      this.serializationTime = serializationTime;
    }

    public long getDeserializationTime() {
      return deserializationTime;
    }

    public void setDeserializationTime(long deserializationTime) {
      this.deserializationTime = deserializationTime;
    }

    public boolean isSynchronousInvocation() {
      return synchronousInvocation;
    }

    public void setSynchronousInvocation() {
      synchronousInvocation = true;
    }

    public boolean isTimeoutException() {
      return timeoutException;
    }

    public void setTimeoutException() {
      timeoutException = true;
    }

    public void clearTimeoutException() {
      timeoutException = false;
    }

    public Message getMessage() {
      return message;
    }

    public void setMessage(Message message) {
      this.message = message;
    }

    public ClientRequest(String aCasReferenceId, BaseUIMAAsynchronousEngineCommon_impl aUimaEEEngine) // ,
                                                                                                      // long
                                                                                                      // aTimeout)
    {
      uimaEEEngine = aUimaEEEngine;
      casReferenceId = aCasReferenceId;
      sharedData = null;
    }

    public String getCasReferenceId() {
      return casReferenceId;
    }

    public void setThreadId(long aThreadId) {
      threadId = aThreadId;
    }

    public long getThreadId() {
      return threadId;
    }

    public void setReceivedProcessCasReply() {
    }

    public void setMetadataTimeout(int aTimeout) {
      metadataTimeout = aTimeout;
    }

    public void setProcessTimeout(int aTimeout) {
      processTimeout = aTimeout;
    }

    public long getProcessTimeout() {
      return processTimeout;
    }

    public void setCpcTimeout(int aTimeout) {
      cpcTimeout = aTimeout;
    }

    public void setEndpoint(String anEndpoint) {
      endpoint = anEndpoint;
    }

    public void setIsRemote(boolean aRemote) {
      isRemote = aRemote;
    }

    public boolean isRemote() {
      return isRemote;
    }

    public void setCAS(CAS aCAS) {
      cas = aCAS;
    }

    public CAS getCAS() {
      return cas;
    }

    public void setCAS(String aSerializedCAS) {
      serializedCAS = aSerializedCAS;
      isSerializedCAS = true;
    }

    public void setBinaryCAS(byte[] aBinaryCas) {
      binaryCas = aBinaryCas;
      isBinaryCas = true;
    }

    public boolean isBinaryCAS() {
      return isBinaryCas;
    }

    public byte[] getBinaryCAS() {
      return binaryCas;
    }

    public String getXmiCAS() {
      return serializedCAS;
    }

    public void startTimer() {
      Date timeToRun = null;
      final ClientRequest _clientReqRef = this;
      if (isMetaRequest()) {
        timeToRun = new Date(System.currentTimeMillis() + metadataTimeout);
      } else if (isCPCRequest()) {
        timeToRun = new Date(System.currentTimeMillis() + cpcTimeout);
      } else {
        timeToRun = new Date(System.currentTimeMillis() + processTimeout);
      }

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "startTimer",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_starting_timer_FINEST",
                new Object[] { endpoint });
      }
      timer = new Timer();
      timer.schedule(new TimerTask() {
        public void run() {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "run",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_timer_expired_INFO",
                    new Object[] { endpoint, casReferenceId });
          }
          CAS cas = null;
          if (isSerializedCAS) {
            try {
              if (isRemote) {
                if (isBinaryCas) {
                  cas = deserialize(binaryCas, _clientReqRef);
                } else {
                  cas = deserializeCAS(serializedCAS, _clientReqRef);
                }
              } else {
                cas = null; // not supported for collocated
              }
            } catch (Exception e) {
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                        "startTimer.run()", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_exception__WARNING", e);
              }
            }
          }

          int timeOutKind;
          if (isMetaRequest()) {
            timeOutKind = MetadataTimeout;
            initialized = false;
            abort = true;
            metaTimeoutErrorCount++;
            clientSideJmxStats.incrementMetaTimeoutErrorCount();
            getMetaSemaphore.release();
          } else if (isCPCRequest()) {
            try {
                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                    UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                            "startTimer.run()", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                            "UIMAJMS_client_timedout_waiting_for_CPC__WARNING", getEndPointName());
                  }
            } catch (Exception e) {

            }
            timeOutKind = CpCTimeout;
            cpcReadySemaphore.release();
          } else {
            timeOutKind = ProcessTimeout;
            processTimeoutErrorCount++;
            clientSideJmxStats.incrementProcessTimeoutErrorCount();
          }
          uimaEEEngine.notifyOnTimout(cas, endpoint, timeOutKind, getCasReferenceId());
          timer.cancel();
          if (cas != null) {
            cas.release();
          }
          return;
        }
      }, timeToRun);

    }

    public void removeEntry(String aCasReferenceId) {
      if (uimaEEEngine.clientCache.containsKey(casReferenceId)) {
        uimaEEEngine.clientCache.remove(casReferenceId);
      }

    }

    public void cancelTimer() {
      if (timer != null) {
        timer.cancel();
      }
    }

    public boolean isCPCRequest() {
      return isCPCRequest;
    }

    public void setCPCRequest(boolean isCPCRequest) {
      this.isCPCRequest = isCPCRequest;
    }

    public boolean isMetaRequest() {
      return isMetaRequest;
    }

    public void setMetaRequest(boolean isMetaRequest) {
      this.isMetaRequest = isMetaRequest;
    }

    public void setCASDepartureTime(long aDepartureTime) {
      casDepartureTime = aDepartureTime;
    }

    public long getCASDepartureTime() {
      return casDepartureTime;
    }

    public void setTimeWaitingForReply(long aTimeWaitingForReply) {
      timeWaitingForReply = aTimeWaitingForReply;
    }

    public long getTimeWaitingForReply() {
      return timeWaitingForReply;
    }

    public XmiSerializationSharedData getXmiSerializationSharedData() {
      return sharedData;
    }

    public void setXmiSerializationSharedData(XmiSerializationSharedData data) {
      this.sharedData = data;
    }
    
    public ReuseInfo getCompress6ReuseInfo() {
      return compress6ReuseInfo;
    }
    
    public void setCompress6ReuseInfo(ReuseInfo compress6ReuseInfo) {
      this.compress6ReuseInfo = compress6ReuseInfo;
    }
  }

  protected static class ThreadMonitor {
    private long threadId;

    private Semaphore monitor = new Semaphore(1);

    public ThreadMonitor(long aThreadId) {
      threadId = aThreadId;
    }

    public long getThreadId() {
      return threadId;
    }

    public Semaphore getMonitor() {
      return monitor;
    }
  }

  /**
   * Called when the producer thread is fully initialized
   */
  protected void onProducerInitialized() {
    producerInitialized = true;
  }

  public boolean connectionOpen() {
	SharedConnection sharedConnection;
    if ( (sharedConnection = lookupConnection(getBrokerURI())) != null ) {
      return sharedConnection.isConnectionValid();
    }
    return false;
  }
  /**
   * Continuously tries to recover connection a broker. it gives up
   * when the client is stopped or the connection is recovered.
   */
  public boolean recoverSharedConnectionIfClosed() {
	SharedConnection sharedConnection;
    if ( !connectionOpen() ) {
      sharedConnection = lookupConnection(getBrokerURI());
      while ( running ) {
        //  blocks until connection is refreshed 
        try {
          sharedConnection.retryConnectionUntilSuccessfull();
          break;
        } catch( Exception e) {
          //  will retry until successful or the client is not running
        }
      }
      //  if still running inject new connection
      if ( running ) {
        //  Inject a new Connection object into an object that sends 
        //  messages to a service. This call invalidates all Session
        //  and Producer objects.
        getDispatcher().setConnection(sharedConnection.getConnection());
      }
      return true;
    }
    return false;
  }
  public void onException(Exception aFailure, String aDestination) {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "onException",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_error_while_sending_msg__WARNING",
              new Object[] { aDestination, aFailure });
    }
    try {
      stop();
    } catch( Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * @override
   */
  protected void setReleaseCASMessage(TextMessage msg, String aCasReferenceId) throws Exception {
  }

  
  protected SharedConnection lookupConnection(String brokerUrl) {
    if ( brokerUrl != null ) {
      if ( sharedConnections.containsKey(brokerUrl) ) {
        return sharedConnections.get(brokerUrl);
      }
    }
	  return null;
  }
  
  // This class is used to share JMS Connection by all instances of UIMA AS
  // client deployed in the same JVM.
  public static class SharedConnection {
    
    private static final Class CLASS_NAME = SharedConnection.class;
    
    public enum ConnectionState { CLOSED, FAILED, WAITING_FOR_BROKER, OPEN };
    
    private volatile Connection connection;
    private volatile boolean stop = false;
    
    private ConnectionState state = ConnectionState.CLOSED;
    private Object stateMonitor = new Object();
    private Object mux = new Object();
    private String brokerURL;
    private ConnectionValidator connectionValidator;
    private Object destroyMux = new Object();
    private ConnectionFactory connectionFactory = null;
    
    private List<BaseUIMAAsynchronousEngineCommon_impl> clientList = 
      new ArrayList<BaseUIMAAsynchronousEngineCommon_impl>();
      
    public SharedConnection( ConnectionFactory connectionFactory , String brokerURL ) {
      this.connectionFactory = connectionFactory;
      this.brokerURL = brokerURL;
    }
    public String getBroker() {
      return brokerURL;
    }
    public void setConnectionValidator( ConnectionValidator validator ) {
      connectionValidator = validator;
    }
    public boolean isOpen() {
      return state == ConnectionState.OPEN;
    }
    public  boolean isConnectionValid() {
      if ( connectionValidator == null ) {
        return false;
      }
      if ( connectionValidator.connectionClosedOrInvalid(connection) == false ) {
        return true;
      } 
      return false;
    }
    /**
     * Using jndi context look the connection factory and 
     * attempt to create broker connection. Throws exception
     * if not successfull. 
     */ 
    public void create() throws Exception {
      if ( connectionFactory == null ) {
        throw new InstantiationException("UIMA AS Client Unable to Initialize SharedConnection Object. ConnectionFactory Has Not Been Provided");
      }
      //  Create shared jms connection to a broker
      connection = connectionFactory.createConnection();
      state = ConnectionState.OPEN;
    }
    private void reinitializeClientListeners() {
      for( BaseUIMAAsynchronousEngineCommon_impl client : clientList ) {
        try {
          client.initializeConsumer(brokerURL, connection);
        } catch( Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "reinitializeClientListeners", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", e);
          }
        }
      }
    }
   private void forceTimeout(List<DelegateEntry> casList, BaseUIMAAsynchronousEngineCommon_impl client) throws Exception {
     //  Force timeout on all pending CASes. Replies will never come, we've lost broker
     //  connection.
     Exception forcedTimeoutException = new MessageTimeoutException("Client Lost Connection To Broker. Forcing Timeout Exception");
     ArrayList<DelegateEntry> copyOfPendingCasList = new ArrayList<DelegateEntry>(casList);
     for( DelegateEntry entry : copyOfPendingCasList ) {
       try {
         ClientRequest cachedRequest = (ClientRequest) client.clientCache.get(entry.getCasReferenceId());
         //  Handle forced timeout. This method removes CAS from the list of CASes pending reply
         client.handleException(forcedTimeoutException, entry.getCasReferenceId(), entry.getCasReferenceId(), cachedRequest, true);
       } catch( Exception e) {
         if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
           UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                   "forceTimeout", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                   "UIMAEE_exception__WARNING", e);
         }
       }
     }
     
   }
    public synchronized void retryConnectionUntilSuccessfull() {
      //  Check if the connection has been restored to the broker. Another thread
      //  may have previously recovered the connection here while we were blocked
      //  on entry to this method ( it is synchronized)
      if ( isConnectionValid() ) {
        return;
      }
      //  Change state of each client in this JVM that uses this shared connection.
      for(BaseUIMAAsynchronousEngineCommon_impl client: clientList) {
        client.state = ClientState.RECONNECTING;
        client.producerInitialized = false;
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "retryConnectionUntilSuccessfull",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_client_lost_connection_to_broker__WARNING",
              new Object[] { brokerURL, (stop==true) });
      }
      //  This loop attempts to recover broker connection every 5 seconds and ends when all clients 
      //  using this shared object terminate or a connection is recovered
      while( !stop ) {
        if ( clientList.size() == 0 ) {
          break; // no more active clients - break out of connection recovery
        }
        try {
          //  Attempt a new connection to a broker
          create();
          //  Got it, start the connection
          start();
          //  Forces clients to drop old Session, Temp Queue, and Consumer objects and create 
          //  new ones. This is needs to be done after a new Connection is created.
          reinitializeClientListeners();
          synchronized( stateMonitor) {
            state = ConnectionState.OPEN;
          }
          break;
        } catch( Exception e) {
          synchronized( stateMonitor ) {
            try {
              stateMonitor.wait(5000); // retry every 5 secs
            } catch( InterruptedException ie) {}
          }
        }
      }
      
      
      if ( !stop ) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "retryConnectionUntilSuccessfull",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_client_recovered_connection__INFO",
                new Object[] { brokerURL });
        }
      }
      for(BaseUIMAAsynchronousEngineCommon_impl client: clientList) {
        client.state = ClientState.RUNNING;
      }
    }
    
    public void start() throws Exception {
      if ( connectionValidator != null && connectionValidator.connectionClosedOrInvalid(connection) ) {
        throw new ResourceInitializationException(new Exception("Unable to start JMS connection that is not open."));
      }
      connection.start();
    }
    public ConnectionState getState() {
      synchronized( stateMonitor) {
        return this.state;
      }
    }
    

    public synchronized Connection getConnection() {
      return connection;
    }

    public synchronized void setConnection(Connection connection) {
      this.connection = connection;
    }

    public void registerClient(BaseUIMAAsynchronousEngineCommon_impl client) {
      synchronized(mux) {
        clientList.add(client);
      }
    }
    public void unregisterClient(BaseUIMAAsynchronousEngineCommon_impl client) {
      synchronized(mux) {
        clientList.remove(client);
      }
      
    }
    public int getClientCount() {
      synchronized (mux) {
        return clientList.size();
      }
    }
    /**
     * This method is called from stop(). It will stop the shared connection if all of the clients
     * have already terminated
     * @return
     */
    public boolean destroy() {
    	return destroy(false);
    }
    
    public boolean destroy(boolean doShutdown) {
      synchronized(destroyMux) {
        //  Check if all clients have terminated and only than stop the shared connection
        if (getClientCount() == 0 && connection != null
                && !((ActiveMQConnection) connection).isClosed() 
                && !((ActiveMQConnection) connection).isClosing()) {
          try {
            stop = true;
            connection.stop();
            connection.close();
            while( !((ActiveMQConnection) connection).isClosed() ) {
              try {
                destroyMux.wait(100);
              } catch( InterruptedException exx) {}
            }
          } catch (Exception e) {
            /* ignore */
          }
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "destroy",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_client_connection_closed__INFO",
                  new Object[] {  });
          }
          return true;

        } else {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "destroy",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_client_shared_connection_not_closed__INFO",
                  new Object[] { getClientCount() });
          }

        }
        return false;
      }
    }
  }
  public static class UimaASShutdownHook implements Runnable {
    UimaAsynchronousEngine asEngine=null;
    public UimaASShutdownHook( UimaAsynchronousEngine asEngine) {
      this.asEngine = asEngine;
    }
    public void run() {
      try {
        if ( asEngine != null ) {
          asEngine.stop();
        } 
      } catch( Exception ex) {
        ex.printStackTrace();
      }
    }
    
  }
}
