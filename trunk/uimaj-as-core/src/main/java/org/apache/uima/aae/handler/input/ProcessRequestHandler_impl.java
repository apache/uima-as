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

import java.io.ByteArrayInputStream;
import java.util.concurrent.Semaphore;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.SerializerCache;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaSerializer;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController_impl;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.Endpoint_impl;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.controller.PrimitiveAnalysisEngineController;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.error.InvalidMessageException;
import org.apache.uima.aae.handler.HandlerBase;
import org.apache.uima.aae.jmx.ServicePerformance;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.monitor.Monitor;
import org.apache.uima.aae.monitor.statistics.DelegateStats;
import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.aae.monitor.statistics.TimerStats;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Marker;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.BinaryCasSerDes6.ReuseInfo;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.util.Level;

public class ProcessRequestHandler_impl extends HandlerBase {
  private static final Class CLASS_NAME = ProcessRequestHandler_impl.class;
  /*
   * Declare a semaphore which is used to block UIMA AS aggregate receiving thread until 
   * a CAS is fully processed. This semaphore prevents the receiving thread from grabbing
   * another CAS from an input queue while a CAS it received previously is still 
   * in-play. Fixes load balancing across multiple UIMA AS aggregate processes.
   */
  final ThreadLocal<Semaphore> threadCompletionMonitor = new ThreadLocal<Semaphore>();

  private Object mux = new Object();

  // controlls access to Aggregates semaphore which
  // throttles ingestion of CASes from service input queue
  private Object lock = new Object();
  
  
  
  public ProcessRequestHandler_impl(String aName) {
    super(aName);
  }

  private void cacheStats(String aCasReferenceId, long aTimeWaitingForCAS,
          long aTimeToDeserializeCAS) throws Exception {
    CacheEntry entry = getController().getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
    entry.incrementTimeWaitingForCAS(aTimeWaitingForCAS);
    entry.incrementTimeToDeserializeCAS(aTimeToDeserializeCAS);
  }

  private boolean messageContainsXMI(MessageContext aMessageContext, String casReferenceId)
          throws Exception {
    // Fetch serialized CAS from the message
    String xmi = aMessageContext.getStringMessage();
    // *****************************************************************
    // ***** NO XMI In Message. Kick this back to sender with exception
    // *****************************************************************
    if (xmi == null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "handleProcessRequestWithXMI", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_message_has_no_cargo__INFO",
                new Object[] { aMessageContext.getEndpoint().getEndpoint() });
      }
      CasStateEntry stateEntry = null;
      String parentCasReferenceId = null;
      try {
        stateEntry = getController().getLocalCache().lookupEntry(casReferenceId);
        if (stateEntry != null && stateEntry.isSubordinate()) {
          CasStateEntry topParentEntry = getController().getLocalCache().getTopCasAncestor(
                  casReferenceId);
          parentCasReferenceId = topParentEntry.getCasReferenceId();
        }
      } catch (Exception e) {
      }

      getController().getOutputChannel().sendReply(
              new InvalidMessageException("No XMI data in message"), casReferenceId,
              parentCasReferenceId, aMessageContext.getEndpoint(), AsynchAEMessage.Process);
      // Dont process this empty message
      return false;
    }
    return true;
  }

  private synchronized CAS getCAS(boolean fetchCASFromShadowCasPool, String shadowCasPoolKey,
          String casReceivedFrom) {
    CAS cas = null;
    // If this is a new CAS (generated by a CM), fetch a CAS from a Shadow Cas Pool associated with
    // a CM that
    // produced the CAS. Each CM will have its own Shadow Cas Pool
    if (fetchCASFromShadowCasPool) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "getCAS",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_request_cas_cm__FINE",
                new Object[] { shadowCasPoolKey });
      }
      // Aggregate time spent waiting for a CAS in the shadow cas pool
      ((AggregateAnalysisEngineController) getController()).getDelegateServicePerformance(
              shadowCasPoolKey).beginWaitOnShadowCASPool();
      cas = getController().getCasManagerWrapper().getNewCas(shadowCasPoolKey);
      ((AggregateAnalysisEngineController) getController()).getDelegateServicePerformance(
              shadowCasPoolKey).endWaitOnShadowCASPool();

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "getCAS",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_request_cas_granted_cm__FINE",
                new Object[] { shadowCasPoolKey });
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "getCAS",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_request_cas__FINE",
                new Object[] { casReceivedFrom });
      }
      // Aggregate time spent waiting for a CAS in the service cas pool
      getController().getServicePerformance().beginWaitOnCASPool();

      cas = getController().getCasManagerWrapper().getNewCas();
      getController().getServicePerformance().endWaitOnCASPool();
      ServicePerformance sp = getController().getServicePerformance();
      sp.incrementCasPoolWaitTime(sp.getTimeWaitingForCAS());

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "getCAS",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_request_cas_granted__FINE",
                new Object[] { casReceivedFrom });
      }
    }
    return cas;
  }

  /**
   * 
   * @param casReferenceId
   * @param freeCasEndpoint
   * @param shadowCasPoolKey
   * @param aMessageContext
   * @return
   * @throws Exception
   */
  private CacheEntry deserializeCASandRegisterWithCache(String casReferenceId,
          Endpoint freeCasEndpoint, String shadowCasPoolKey, MessageContext aMessageContext)
          throws Exception {
    long inTime = System.nanoTime();
    boolean casRegistered = false;

    // Time how long we wait on Cas Pool to fetch a new CAS
    long t1 = getController().getCpuTime();
    // *************************************************************************
    // Fetch CAS from a Cas Pool. If the CAS came from a Cas Multiplier
    // fetch the CAS from a shadow CAS pool. Otherwise, fetch the CAS
    // from the service CAS Pool.
    // *************************************************************************
    Endpoint endpoint = aMessageContext.getEndpoint();
    CAS cas = null;
    CacheEntry entry = null;
    
    try {
      cas = getCAS(aMessageContext.propertyExists(AsynchAEMessage.CasSequence), shadowCasPoolKey,
              endpoint.getEndpoint());
      long timeWaitingForCAS = getController().getCpuTime() - t1;
      // Check if we are still running
      if (getController().isStopped()) {
        // The Controller is in shutdown state.
        getController().dropCAS(cas);
        return null;
      }
      // *************************************************************************
      // Deserialize CAS from the message
      // *************************************************************************
      t1 = getController().getCpuTime();
      SerialFormat serialFormat = endpoint.getSerialFormat();
      XmiSerializationSharedData deserSharedData = null;
      ReuseInfo reuseInfo = null;
      
      UimaSerializer uimaSerializer = SerializerCache.lookupSerializerByThreadId();
      byte[] binarySource = aMessageContext.getByteMessage();

      switch (serialFormat) {
      case XMI:
        // Fetch serialized CAS from the message
        String xmi = aMessageContext.getStringMessage();
        deserSharedData = new XmiSerializationSharedData();
        uimaSerializer.deserializeCasFromXmi(xmi, cas, deserSharedData, true, -1);
        break;
      case BINARY:
        // *************************************************************************
        // Register the CAS with a local cache
        // *************************************************************************
        // CacheEntry entry = getController().getInProcessCache().register(cas, aMessageContext,
        // deserSharedData, casReferenceId);
        // BINARY format may be COMPRESSED etc, so update it upon reading
        serialFormat = uimaSerializer.deserializeCasFromBinary(binarySource, cas);
        // BINARY format may be COMPRESSED etc, so update it upon reading
        endpoint.setSerialFormat(serialFormat);
        break;
      case COMPRESSED_FILTERED:
        ByteArrayInputStream bais = new ByteArrayInputStream(binarySource);
        reuseInfo = Serialization.deserializeCAS(cas, bais, endpoint.getTypeSystemImpl(), null).getReuseInfo();
        break;
      default:
        throw new RuntimeException("Never Happen");
      }

      // *************************************************************************
      // Check and set up for Delta CAS reply
      // *************************************************************************
      boolean acceptsDeltaCas = false;
      Marker marker = null;
      if (aMessageContext.propertyExists(AsynchAEMessage.AcceptsDeltaCas)) {
        acceptsDeltaCas = aMessageContext.getMessageBooleanProperty(AsynchAEMessage.AcceptsDeltaCas);
        if (acceptsDeltaCas) {
          marker = cas.createMarker();
        }
      }
      // *************************************************************************
      // Register the CAS with a local cache
      // *************************************************************************
      // CacheEntry entry = getController().getInProcessCache().register(cas, aMessageContext,
      // deserSharedData, casReferenceId);
      entry = getController().getInProcessCache().register(cas, aMessageContext, deserSharedData, reuseInfo,
              casReferenceId, marker, acceptsDeltaCas);
      
      /*
		Lazily create a Semaphore that will be used to throttle CAS ingestion from service
		input queue. This only applies to async aggregates. The semaphore is initialized
		with the number of permits equal to the service CasPool size. The idea is that this
		service should only ingest as many CASes as it is capable of processing without
		waiting for a free instance of CAS from the service CasPool.
      */
      boolean inputCAS = aMessageContext
           .getMessageStringProperty(AsynchAEMessage.InputCasReference) == null ? true : false;
      if ( !getController().isPrimitive() && inputCAS) {
    	 
    	  try {
    		  synchronized(lock) {
    			  // lazily create a Semaphore on the first Process request. This semaphore
    			  // will throttle ingestion of CASes from service input queue.
    			  if (((AggregateAnalysisEngineController_impl) getController()).semaphore == null) {
    				  ((AggregateAnalysisEngineController_impl) getController()).semaphore = 
    						  new Semaphore(
    						  ((AggregateAnalysisEngineController) getController())
    						  .getServiceCasPoolSize()-1);
    				 // semaphore.acquire();
    			  }
    		  }
    	  } catch( Exception e) {
    		  throw e;
    	  }
        entry.setThreadCompletionSemaphore(((AggregateAnalysisEngineController_impl) getController()).semaphore);
      }
      long timeToDeserializeCAS = getController().getCpuTime() - t1;
      getController().incrementDeserializationTime(timeToDeserializeCAS);
      LongNumericStatistic statistic;
      if ((statistic = getController().getMonitor().getLongNumericStatistic("",
              Monitor.TotalDeserializeTime)) != null) {
        statistic.increment(timeToDeserializeCAS);
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                "handleProcessRequestWithXMI", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_deserialize_cas_time_FINE",
                new Object[] { (double) timeToDeserializeCAS / 1000000.0 });
      }

      // Update Stats
      ServicePerformance casStats = getController().getCasStatistics(casReferenceId);
      casStats.incrementCasDeserializationTime(timeToDeserializeCAS);
      if (getController().isTopLevelComponent()) {
        synchronized (mux) {
          getController().getServicePerformance().incrementCasDeserializationTime(
                  timeToDeserializeCAS);
        }
      }
      getController().saveTime(inTime, casReferenceId, getController().getName());

      if (getController() instanceof AggregateAnalysisEngineController) {
        // If the message came from a Cas Multiplier, associate the input/parent CAS id with this CAS
        if (aMessageContext.propertyExists(AsynchAEMessage.CasSequence)) {
          // Fetch parent CAS id
          String inputCasReferenceId = aMessageContext
                  .getMessageStringProperty(AsynchAEMessage.InputCasReference);
          if (shadowCasPoolKey != null) {
            // Save the key of the Cas Multiplier in the cache. It will be now known which Cas
            // Multiplier produced this CAS
            entry.setCasProducerKey(shadowCasPoolKey);
          }
          // associate this subordinate CAS with the parent CAS
          entry.setInputCasReferenceId(inputCasReferenceId);
          // Save a Cas Multiplier endpoint where a Free CAS notification will be sent
          entry.setFreeCasEndpoint(freeCasEndpoint);
          cacheStats(inputCasReferenceId, timeWaitingForCAS, timeToDeserializeCAS);
        } else {
          cacheStats(casReferenceId, timeWaitingForCAS, timeToDeserializeCAS);
        }
        DelegateStats stats = new DelegateStats();
        if (entry.getStat() == null) {
          entry.setStat(stats);
          // Add entry for self (this aggregate). MessageContext.getEndpointName()
          // returns the name of the queue receiving the message.
          stats.put(getController().getServiceEndpointName(), new TimerStats());
        } else {
          if (!stats.containsKey(getController().getServiceEndpointName())) {
            stats.put(getController().getServiceEndpointName(), new DelegateStats());
          }
        }
      } else {
        cacheStats(casReferenceId, timeWaitingForCAS, timeToDeserializeCAS);
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                "handleProcessRequestWithXMI", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_deserialized_cas_ready_to_process_FINE",
                new Object[] { aMessageContext.getEndpoint().getEndpoint() });
      }
      cacheProcessCommandInClientEndpoint();
       
    } catch( Exception e) {
      if ( cas != null ) {
        cas.release();
      }
      throw e;
    }
    return entry;
  }

  private String getCasReferenceId(MessageContext aMessageContext) throws Exception {
    if (!aMessageContext.propertyExists(AsynchAEMessage.CasReference)) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "handleProcessRequestWithCASReference", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_message_has_cas_refid__INFO",
                new Object[] { aMessageContext.getEndpoint().getEndpoint() });
      }
      getController().getOutputChannel().sendReply(
              new InvalidMessageException("No Cas Reference Id Received From Delegate In message"),
              null, null, aMessageContext.getEndpoint(), AsynchAEMessage.Process);
      return null;
    }
    return aMessageContext.getMessageStringProperty(AsynchAEMessage.CasReference);
  }

  /**
   * Handles process request from a remote client
   * 
   * @param aMessageContext
   *          - contains a message from UIMA-AS Client
   * @throws AsynchAEException
   */
  private void handleProcessRequestFromRemoteClient(MessageContext aMessageContext)
          throws AsynchAEException {
    CacheEntry entry = null;
    String casReferenceId = null;
    // Check if there is a cargo in the message
    if (aMessageContext.getMessageIntProperty(AsynchAEMessage.Payload) == AsynchAEMessage.XMIPayload
            && aMessageContext.getStringMessage() == null) {
      return; // No XMI just return
    }

    try {

      String newCASProducedBy = null;
      // Get the CAS Reference Id of the input CAS
      // Fetch id of the CAS from the message. If it doesnt exist the method will create an entry in
      // the log file and return null
      casReferenceId = getCasReferenceId(aMessageContext);
      if (casReferenceId == null) {
        return; // Invalid message. Nothing to do
      }
      // Initially make both equal
      String inputCasReferenceId = casReferenceId;
      // Destination where Free Cas Notification will be sent if the CAS came from a Cas Multiplier
      Endpoint freeCasEndpoint = null;

      CasStateEntry inputCasStateEntry = null;

      // CASes generated by a Cas Multiplier will have a CasSequence property set.
      if (aMessageContext.propertyExists(AsynchAEMessage.CasSequence)) {
        // Fetch the name of the Cas Multiplier's input queue
        // String cmEndpointName = aMessageContext.getEndpoint().getEndpoint();
        String cmEndpointName = aMessageContext
                .getMessageStringProperty(AsynchAEMessage.MessageFrom);
        newCASProducedBy = ((AggregateAnalysisEngineController) getController())
                .lookUpDelegateKey(cmEndpointName);
        // Fetch an ID of the parent CAS
        inputCasReferenceId = aMessageContext
                .getMessageStringProperty(AsynchAEMessage.InputCasReference);
        // Fetch Cache entry for the parent CAS
        CacheEntry inputCasCacheEntry = getController().getInProcessCache().getCacheEntryForCAS(
                inputCasReferenceId);
        // Fetch an endpoint where Free CAS Notification must be sent.
        // This endpoint is unique per CM instance. Meaning, each
        // instance of CM will have an endpoint where it expects Free CAS
        // notifications.
        freeCasEndpoint = aMessageContext.getEndpoint();
        // Clone an endpoint where Free Cas Request will be sent
        freeCasEndpoint = (Endpoint) ((Endpoint_impl) freeCasEndpoint).clone();

        if (getController() instanceof AggregateAnalysisEngineController) {
          inputCasStateEntry = ((AggregateAnalysisEngineController) getController())
                  .getLocalCache().lookupEntry(inputCasReferenceId);

          // Associate Free Cas Notification Endpoint with an input Cas
          inputCasStateEntry.setFreeCasNotificationEndpoint(freeCasEndpoint);
        }

        computeStats(aMessageContext, inputCasReferenceId);
        // Reset the destination
        aMessageContext.getEndpoint().setDestination(null);
        // This CAS came in from a CAS Multiplier. Treat it differently than the
        // input CAS. In case the Aggregate needs to send this CAS to the
        // client, retrieve the client destination by looking up the client endpoint
        // using input CAS reference id. CASes generated by the CAS multiplier will have
        // the same Cas Reference id.
        Endpoint replyToEndpoint = inputCasCacheEntry.getMessageOrigin();
        // The message context contains a Cas Multiplier endpoint. Since
        // we dont want to send a generated CAS back to the CM, override
        // with an endpoint provided by the client of
        // this service. Client endpoint is attached to an input Cas cache entry.
        aMessageContext.getEndpoint().setEndpoint(replyToEndpoint.getEndpoint());
        aMessageContext.getEndpoint().setServerURI(replyToEndpoint.getServerURI());

        // Before sending a CAS to Cas Multiplier, the aggregate has
        // saved the CM key in the CAS cache entry. Fetch the key
        // of the CM so that we can ask the right Shadow Cas Pool for
        // a new CAS. Every Shadow Cas Pool has a unique id which
        // corresponds to a Cas Multiplier key.
        // newCASProducedBy = inputCasCacheEntry.getCasMultiplierKey();
        if (getController() instanceof AggregateAnalysisEngineController) {
          Endpoint casMultiplierEndpoint = ((AggregateAnalysisEngineController) getController())
                  .lookUpEndpoint(newCASProducedBy, false);
          if (casMultiplierEndpoint != null) {
            // Save the URL of the broker managing the Free Cas Notification queue.
            // This is needed when we try to establish a connection to the broker.
            freeCasEndpoint.setServerURI(casMultiplierEndpoint.getServerURI());
          }
        }
      } else if (getController().isTopLevelComponent()) {
        if (getController() instanceof AggregateAnalysisEngineController) {
          ((AggregateAnalysisEngineController) getController()).addMessageOrigin(casReferenceId,
                  aMessageContext.getEndpoint());
        }

      }
      // To prevent processing multiple messages with the same CasReferenceId, check the CAS cache
      // to see if the message with a given CasReferenceId is already being processed. It is, the
      // message contains the same request possibly issued by the caller due to timeout. Also this
      // mechanism helps with dealing with scenario when this service is not up when the client
      // sends
      // request. The client can keep re-sending the same request until its timeout thresholds are
      // exceeded. By that time, there may be multiple messages in this service queue with the same
      // CasReferenceId. When the service finally comes back up, it will have multiple messages in
      // its queue possibly from the same client. Only the first message for any given
      // CasReferenceId
      // should be processed.
      CasStateEntry cse = null;
      if (!getController().getInProcessCache().entryExists(casReferenceId)) {
        
        if (getController().getLocalCache().lookupEntry(casReferenceId) == null) {
          // Create a new entry in the local cache for the CAS received from the remote
          cse = getController().getLocalCache().createCasStateEntry(casReferenceId);
          // Check if this CAS is a child
          if (aMessageContext.propertyExists(AsynchAEMessage.CasSequence)) {
            cse.setInputCasReferenceId(inputCasReferenceId);
          }
        } else {
          cse = getController().getLocalCache().lookupEntry(casReferenceId);
        }

        if (getController() instanceof AggregateAnalysisEngineController
                && aMessageContext.propertyExists(AsynchAEMessage.CasSequence)) {
          String delegateInputQueueName = aMessageContext
                  .getMessageStringProperty(AsynchAEMessage.MessageFrom);
          String delegateKey = ((AggregateAnalysisEngineController) getController())
                  .lookUpDelegateKey(delegateInputQueueName); // aMessageContext.getEndpoint().getEndpoint());
          if (delegateKey != null) {
            Delegate delegate = ((AggregateAnalysisEngineController) getController())
                    .lookupDelegate(delegateKey);
            // Save the last delegate handling this CAS
            cse.setLastDelegate(delegate);
            // If there is one thread receiving messages from Cas Multiplier increment number of
            // child Cases
            // of the parent CAS. If there are more threads (consumers) a special object
            // ConcurrentMessageListener
            // has already incremented the count. This special object enforces order of processing
            // for CASes
            // coming in from the Cas Multiplier.
            if (!delegate.hasConcurrentConsumersOnReplyQueue()) {
              inputCasStateEntry.incrementSubordinateCasInPlayCount();
            }
          }
        }

        entry = deserializeCASandRegisterWithCache(casReferenceId, freeCasEndpoint,
                newCASProducedBy, aMessageContext);
        if (getController().isStopped() || entry == null || entry.getCas() == null) {
          if (entry != null) {
            // The Controller is in shutdown state, release the CAS
            getController().dropCAS(entry.getCasReferenceId(), true);
            entry = null;
          }
          return;
        }
        // *****************************************************************
        // Process the CAS
        // *****************************************************************
        invokeProcess(entry.getCas(), inputCasReferenceId, casReferenceId, aMessageContext,
                newCASProducedBy);
        
        /**
         * Below comments apply to UIMA AS aggregate only.
         * CAS has been handed off to a delegate. Now block the receiving thread until
         * the CAS is processed or there is a timeout or error. Fetch this thread's ThreadLocal
         * semaphore to block the thread. It will be unblocked when the aggregate is done with
         * the CAS.
         */
        if (!getController().isPrimitive() && 
        		cse != null && !cse.isSubordinate() ) {
        		
          try {
        	  synchronized(lock) {
        		  if ( entry.getThreadCompletionSemaphore() != null) {
                 	 entry.getThreadCompletionSemaphore().acquire();
        		  }
        	  }
          } catch( InterruptedException ex) {
          } 
        }
        
      } else {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "handleProcessRequestFromRemoteClient", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_duplicate_request__INFO", new Object[] { casReferenceId });
        }
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( getController() != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handleProcessRequestFromRemoteClient", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getController().getComponentName());
        }

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "handleProcessRequestFromRemoteClient", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
      ErrorContext errorContext = new ErrorContext();
      errorContext.add(AsynchAEMessage.Endpoint, aMessageContext.getEndpoint());
      errorContext.add(AsynchAEMessage.Command, AsynchAEMessage.Process);
      errorContext.add(AsynchAEMessage.CasReference, casReferenceId);
      if (entry != null) {
        getController().dropCAS(entry.getCas());
      }
      getController().getErrorHandlerChain().handle(e, errorContext, getController());
    }

  }

  private void handleProcessRequestWithCASReference(MessageContext aMessageContext)
          throws AsynchAEException {
    boolean isNewCAS = false;
    String newCASProducedBy = null;

    try {
      // This is only used when handling CASes produced by CAS Multiplier
      String inputCasReferenceId = null;
      CAS cas = null;
      CasStateEntry cse = null;
      String casReferenceId = getCasReferenceId(aMessageContext);
      if ((cse = getController().getLocalCache().lookupEntry(casReferenceId)) == null) {
        // Create a new entry in the local cache for the CAS received from the remote
        cse = getController().getLocalCache().createCasStateEntry(casReferenceId);
      }

      // Check if this Cas has been sent from a Cas Multiplier. If so, its sequence will be > 0
      if (aMessageContext.propertyExists(AsynchAEMessage.CasSequence)) {
        isNewCAS = true;

        Endpoint casMultiplierEndpoint = aMessageContext.getEndpoint();

        if (casMultiplierEndpoint == null) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                    "handleProcessRequestWithCASReference",
                    UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_no_endpoint_for_reply__INFO",
                    new Object[] { casReferenceId });
          }
          return;
        }
        // Get the id of the parent Cas
        inputCasReferenceId = aMessageContext
                .getMessageStringProperty(AsynchAEMessage.InputCasReference);
        if (cse.getInputCasReferenceId() == null) {
          cse.setInputCasReferenceId(inputCasReferenceId);
        }

        if (getController() instanceof AggregateAnalysisEngineController) {
          CasStateEntry parentCasEntry = getController().getLocalCache().lookupEntry(
                  inputCasReferenceId);
          // Check if the parent CAS is in a failed state first
          if (parentCasEntry != null && parentCasEntry.isFailed()) {
            // handle CAS release
            getController().process(null, casReferenceId);
            return;
          }

          String delegateKey = ((AggregateAnalysisEngineController) getController())
                  .lookUpDelegateKey(aMessageContext.getEndpoint().getEndpoint());
          Delegate delegate = ((AggregateAnalysisEngineController) getController())
                  .lookupDelegate(delegateKey);
          cse.setLastDelegate(delegate);
          newCASProducedBy = delegate.getKey();
          casMultiplierEndpoint.setIsCasMultiplier(true);
          try {
            // Save the endpoint of the CM which produced the Cas
            getController().getInProcessCache().setCasProducer(casReferenceId, newCASProducedBy);
          } catch (Exception e) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
              if ( getController() != null ) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                        "handleProcessRequestWithCASReference", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_service_exception_WARNING", getController().getComponentName());
              }

              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                      "handleProcessRequestWithCASReference",
                      UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_exception__WARNING",
                      e);
            }
            return;
          }
          // Safety check. The input Cas should not be null here
          if (inputCasReferenceId != null) {
            try {
              Endpoint endp = null;

              // Located the origin of the parent Cas. The produced Cas will inherit the origin from
              // its parent.
              // Once the origin is identified, save the origin using the produced Cas id as a key.
              if (endp == null) {
                boolean gotTheEndpoint = false;
                String parentCasId = inputCasReferenceId;
                // Loop through the parent tree until an origin is found
                while (!gotTheEndpoint) {
                  // Check if the current parent has an associated origin
                  endp = ((AggregateAnalysisEngineController) getController())
                          .getMessageOrigin(parentCasId);
                  // Check if there is an origin. If so, we are done
                  if (endp != null) {
                    break;
                  }
                  // The current parent has no origin, get its parent and try again
                  CacheEntry entry = getController().getInProcessCache().getCacheEntryForCAS(
                          parentCasId);
                  parentCasId = entry.getInputCasReferenceId();
                  // Check if we reached the top of the hierarchy tree. If so, we have no origin.
                  // This should
                  // never be the case. Every Cas must have an origin
                  if (parentCasId == null) {
                    break;
                  }
                }
              }
              // If origin not found log it as this indicates an error
              if (endp == null) {
                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                          "handleProcessRequestWithCASReference",
                          UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_msg_origin_not_found__INFO",
                          new Object[] { getController().getComponentName(), inputCasReferenceId });
                }
              } else {
                ((AggregateAnalysisEngineController) getController()).addMessageOrigin(
                        casReferenceId, endp);
                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(
                          Level.FINEST,
                          CLASS_NAME.getName(),
                          "handleProcessRequestWithCASReference",
                          UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_msg_origin_added__FINEST",
                          new Object[] { getController().getComponentName(), casReferenceId,
                              newCASProducedBy });
                }
              }
            } catch (Exception e) {
             
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                if ( getController() != null ) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                          "handleProcessRequestWithCASReference", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_service_exception_WARNING", getController().getComponentName());
                }

                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                        "handleProcessRequestWithCASReference",
                        UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_exception__WARNING", e);
              }
            }
          } else {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(
                      Level.INFO,
                      CLASS_NAME.getName(),
                      "handleProcessRequestWithCASReference",
                      UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_input_cas_invalid__INFO",
                      new Object[] { getController().getComponentName(), newCASProducedBy,
                          casReferenceId });
            }
          }
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                  "handleProcessRequestWithCASReference", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_new_cas__FINE", new Object[] { casReferenceId, newCASProducedBy });
        }
        aMessageContext.getEndpoint().setEndpoint(casMultiplierEndpoint.getEndpoint());
        aMessageContext.getEndpoint().setServerURI(casMultiplierEndpoint.getServerURI());
      } else {
        if (getController() instanceof AggregateAnalysisEngineController) {
          ((AggregateAnalysisEngineController) getController()).addMessageOrigin(casReferenceId,
                  aMessageContext.getEndpoint());
        }

      }
      cas = getController().getInProcessCache().getCasByReference(casReferenceId);

      long arrivalTime = System.nanoTime();
      getController().saveTime(arrivalTime, casReferenceId, getController().getName());// aMessageContext.getEndpointName());

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                "handleProcessRequestWithCASReference", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_analyzing_cas__FINE", new Object[] { casReferenceId });
      }
      // Save Process command in the client endpoint.
      cacheProcessCommandInClientEndpoint();

      if (getController().isStopped()) {
        return;
      }

      if (isNewCAS) {
        invokeProcess(cas, inputCasReferenceId, casReferenceId, aMessageContext, newCASProducedBy);
      } else {
        invokeProcess(cas, casReferenceId, null, aMessageContext, newCASProducedBy);
      }
    } catch (AsynchAEException e) {
      throw e;
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }

  }
 // commented out 5/2013  xcas not used as a uima-as serialization form
//  private void handleProcessRequestWithXCAS(MessageContext aMessageContext)
//          throws AsynchAEException {
//
//    try {
//      // Get the CAS Reference Id of the input CAS
//      String casReferenceId = getCasReferenceId(aMessageContext);
//      String inputCasReferenceId = casReferenceId;
//      // This is only used when handling CASes produced by CAS Multiplier
//      String newCASProducedBy = null;
//
//      if (aMessageContext.propertyExists(AsynchAEMessage.CasSequence)) {
//        // This CAS came in from the CAS Multiplier. Treat it differently than the
//        // input CAS. First, in case the Aggregate needs to send this CAS to the
//        // client, retrieve the client destination by looking up the client endpoint
//        // using input CAS reference id. CASes generated by the CAS multiplier will have
//        // the same Cas Reference id.
//        Endpoint replyToEndpoint = getController().getInProcessCache().getCacheEntryForCAS(
//                casReferenceId).getMessageOrigin();
//
//        //	
//        if (getController() instanceof AggregateAnalysisEngineController) {
//          newCASProducedBy = ((AggregateAnalysisEngineController) getController())
//                  .lookUpDelegateKey(replyToEndpoint.getEndpoint());
//        }
//        // MessageContext contains endpoint set by the CAS Multiplier service. Overwrite
//        // this with the endpoint of the client who sent the input CAS. In case this
//        // aggregate is configured to send new CASes to the client we know where to send them.
//        aMessageContext.getEndpoint().setEndpoint(replyToEndpoint.getEndpoint());
//        aMessageContext.getEndpoint().setServerURI(replyToEndpoint.getServerURI());
//        inputCasReferenceId = String.valueOf(casReferenceId);
//        // Set this to null so that the new CAS gets its own Cas Reference Id below
//        casReferenceId = null;
//      }
//
//      long arrivalTime = System.nanoTime();
//      getController().saveTime(arrivalTime, casReferenceId, getController().getName());// aMessageContext.getEndpointName());
//
//      // To prevent processing multiple messages with the same CasReferenceId, check the CAS cache
//      // to see if the message with a given CasReferenceId is already being processed. It is, the
//      // message contains the same request possibly issued by the caller due to timeout. Also this
//      // mechanism helps with dealing with scenario when this service is not up when the client
//      // sends
//      // request. The client can keep re-sending the same request until its timeout thresholds are
//      // exceeded. By that time, there may be multiple messages in this service queue with the same
//      // CasReferenceId. When the service finally comes back up, it will have multiple messages in
//      // its queue possibly from the same client. Only the first message for any given
//      // CasReferenceId
//      // should be processed.
//      if (casReferenceId == null
//              || !getController().getInProcessCache().entryExists(casReferenceId)) {
//        String xmi = aMessageContext.getStringMessage();
//
//        // *****************************************************************
//        // ***** NO XMI In Message. Kick this back to sender with exception
//        // *****************************************************************
//        if (xmi == null) {
//          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
//            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
//                    "handleProcessRequestWithXCAS", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
//                    "UIMAEE_message_has_no_cargo__INFO",
//                    new Object[] { aMessageContext.getEndpoint().getEndpoint() });
//          }
//          getController().getOutputChannel().sendReply(
//                  new InvalidMessageException("No XMI data in message"), casReferenceId, null,
//                  aMessageContext.getEndpoint(), AsynchAEMessage.Process);
//          // Dont process this empty message
//          return;
//        }
//
//        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
//          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
//                  "handleProcessRequestWithXCAS", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
//                  "UIMAEE_request_cas__FINE",
//                  new Object[] { aMessageContext.getEndpoint().getEndpoint() });
//        }
//        CAS cas = getController().getCasManagerWrapper().getNewCas();
//
//        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
//          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
//                  "handleProcessRequestWithXCAS", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
//                  "UIMAEE_request_cas_granted__FINE",
//                  new Object[] { aMessageContext.getEndpoint().getEndpoint() });
//        }
//        
//        UimaSerializer uimaSerializer = SerializerCache.lookupSerializerByThreadId();
//
//        XmiSerializationSharedData deserSharedData = new XmiSerializationSharedData();
//        uimaSerializer.deserializeCasFromXmi(xmi, cas, deserSharedData, true, -1);
//
//        if (casReferenceId == null) {
//          CacheEntry entry = getController().getInProcessCache().register(cas, aMessageContext,
//                  deserSharedData);
//          casReferenceId = entry.getCasReferenceId();
//        } else {
//          if (getController() instanceof PrimitiveAnalysisEngineController) {
//            getController().getInProcessCache().register(cas, aMessageContext, deserSharedData,
//                    casReferenceId);
//          }
//        }
//        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
//          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
//                  "handleProcessRequestWithXCAS", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
//                  "UIMAEE_deserialized_cas_ready_to_process_FINE",
//                  new Object[] { aMessageContext.getEndpoint().getEndpoint() });
//        }
//        cacheProcessCommandInClientEndpoint();
//        invokeProcess(cas, inputCasReferenceId, casReferenceId, aMessageContext, newCASProducedBy);
//      } else {
//        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
//          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
//                  "handleProcessRequestWithXCAS", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
//                  "UIMAEE_duplicate_request__INFO", new Object[] { casReferenceId });
//        }
//      }
//
//    } catch (AsynchAEException e) {
//      throw e;
//    } catch (Exception e) {
//      throw new AsynchAEException(e);
//    }
//
//  }

  private void cacheProcessCommandInClientEndpoint() {
    Endpoint clientEndpoint = getController().getClientEndpoint();
    if (clientEndpoint != null) {
      clientEndpoint.setCommand(AsynchAEMessage.Process);
    }
  }

  private void handleCollectionProcessCompleteRequest(MessageContext aMessageContext)
          throws AsynchAEException {
    Endpoint replyToEndpoint = aMessageContext.getEndpoint();
    getController().collectionProcessComplete(replyToEndpoint);
  }

  private void handleReleaseCASRequest(MessageContext aMessageContext) throws AsynchAEException {
    String casReferenceId = aMessageContext.getMessageStringProperty(AsynchAEMessage.CasReference);
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
              "handleReleaseCASRequest", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_release_cas_req__FINE",
              new Object[] { getController().getName(), casReferenceId });
    }
    getController().releaseNextCas(casReferenceId);
  }

  private void handlePingRequest(MessageContext aMessageContext) {
    try {
      getController().getOutputChannel().sendReply(AsynchAEMessage.Ping,
              aMessageContext.getEndpoint(), null, false);
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( getController() != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handlePingRequest", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getController().getComponentName());
        }

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "handlePingRequest", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }

  }

  private void handleStopRequest(MessageContext aMessageContext) {
    try {
      String casReferenceId = aMessageContext
              .getMessageStringProperty(AsynchAEMessage.CasReference);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "handleStopRequest", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_received_stop_request__INFO", new Object[] { getController().getComponentName(), casReferenceId });
      }

      if (getController() instanceof PrimitiveAnalysisEngineController) {
        getController().addAbortedCasReferenceId(casReferenceId);
      } else if (getController() instanceof AggregateAnalysisEngineController_impl) {
        try {
          CasStateEntry casStateEntry = getController().getLocalCache().lookupEntry(casReferenceId);
          // Mark the CAS as if it have failed. In this case we dont associate any
          // exceptions with this CAS so its really not a failure of a CAS or any
          // of its children. We simply use the same logic here as if the CAS failed.
          // The Aggregate replyToClient() method will know that this CAS was stopped
          // as opposed to failed by the fact that the CAS has no exceptions associated
          // with it. In such case the replyToClient() method returns an input CAS as if
          // it has been fully processed.
          casStateEntry.setFailed();
          ((AggregateAnalysisEngineController_impl) getController()).stopCasMultipliers();
        } catch (Exception ex) {
        } // CAS may have already been deleted

      }

    } catch (Exception e) {
    }
  }

  /**
   * Main method called by the predecessor handler.
   * 
   * 
   */
  public void handle(Object anObjectToHandle) // throws AsynchAEException
  {
    try {
      super.validate(anObjectToHandle);

      MessageContext messageContext = (MessageContext) anObjectToHandle;
      if (isHandlerForMessage(messageContext, AsynchAEMessage.Request, AsynchAEMessage.Process)
              || isHandlerForMessage(messageContext, AsynchAEMessage.Request,
                      AsynchAEMessage.CollectionProcessComplete)
              || isHandlerForMessage(messageContext, AsynchAEMessage.Request,
                      AsynchAEMessage.ReleaseCAS)
              || isHandlerForMessage(messageContext, AsynchAEMessage.Request, AsynchAEMessage.Stop)) {
        int payload = messageContext.getMessageIntProperty(AsynchAEMessage.Payload);
        int command = messageContext.getMessageIntProperty(AsynchAEMessage.Command);

        getController().getControllerLatch().waitUntilInitialized();

        // If a Process Request, increment number of CASes processed
        if (messageContext.getMessageIntProperty(AsynchAEMessage.MessageType) == AsynchAEMessage.Request
                && command == AsynchAEMessage.Process
                && !messageContext.propertyExists(AsynchAEMessage.CasSequence)) {
          // Increment number of CASes processed by this service
          getController().getServicePerformance().incrementNumberOfCASesProcessed();
        }
        if (getController().isStopped()) {
          return;
        }

        if (AsynchAEMessage.CASRefID == payload) {
          // Fetch id of the CAS from the message.
          if (getCasReferenceId(messageContext) == null) {
            return; // Invalid message. Nothing to do
          }

          handleProcessRequestWithCASReference(messageContext);
        } else if (AsynchAEMessage.XMIPayload == payload
                || AsynchAEMessage.BinaryPayload == payload) {
          // Fetch id of the CAS from the message.
          if (getCasReferenceId(messageContext) == null) {
            return; // Invalid message. Nothing to do
          }
          handleProcessRequestFromRemoteClient(messageContext);
        } 
//          else if (AsynchAEMessage.XCASPayload == payload) {
//          // Fetch id of the CAS from the message.
//          if (getCasReferenceId(messageContext) == null) {
//            return; // Invalid message. Nothing to do
//          }
//          handleProcessRequestWithXCAS(messageContext);
//        } 
          else if (AsynchAEMessage.None == payload
                && AsynchAEMessage.CollectionProcessComplete == command) {
          handleCollectionProcessCompleteRequest(messageContext);
        } else if (AsynchAEMessage.None == payload && AsynchAEMessage.ReleaseCAS == command) {
          handleReleaseCASRequest(messageContext);
        } else if (AsynchAEMessage.None == payload && AsynchAEMessage.Stop == command) {
          handleStopRequest(messageContext);
        } else if (AsynchAEMessage.None == payload && AsynchAEMessage.Ping == command) {
          handlePingRequest(messageContext);
        }
        // Handled Request
        return;
      }
      // Not a Request nor Command. Delegate to the next handler in the chain
      super.delegate(messageContext);
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( getController() != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handle", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getController().getComponentName());
        }

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "handle", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
      getController().getErrorHandlerChain().handle(e,
              HandlerBase.populateErrorContext((MessageContext) anObjectToHandle), getController());
    }
  }

}
