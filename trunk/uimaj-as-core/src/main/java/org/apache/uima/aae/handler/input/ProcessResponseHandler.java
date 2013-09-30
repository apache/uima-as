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
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UIMARuntimeException;
import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.SerializerCache;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaSerializer;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.BaseAnalysisEngineController;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.error.ExpiredMessageException;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.error.UimaAsDelegateException;
import org.apache.uima.aae.error.UimaEEServiceException;
import org.apache.uima.aae.handler.HandlerBase;
import org.apache.uima.aae.jmx.ServicePerformance;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.monitor.Monitor;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.AllowPreexistingFS;
import org.apache.uima.cas.impl.BinaryCasSerDes6;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.MarkerImpl;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.cas.impl.TypeSystemImpl;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.cas.impl.BinaryCasSerDes6.ReuseInfo;
import org.apache.uima.util.Level;

public class ProcessResponseHandler extends HandlerBase {
  private static final Class CLASS_NAME = ProcessResponseHandler.class;

  public ProcessResponseHandler(String aName) {
    super(aName);
  }

  private Endpoint lookupEndpoint(String anEndpointName, String aCasReferenceId) {
    return getController().getInProcessCache().getEndpoint(anEndpointName, aCasReferenceId);
  }

  private void cancelTimer(MessageContext aMessageContext, String aCasReferenceId,
          boolean removeEndpoint) throws AsynchAEException {
    if (aMessageContext != null && aMessageContext.getEndpoint() != null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "cancelTimer",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_cancel_timer__FINE",
                new Object[] { aMessageContext.getEndpoint().getEndpoint(), aCasReferenceId });
      }
      // Retrieve the endpoint from the cache using endpoint name
      // and casRefereceId
      if (aCasReferenceId == null
              && aMessageContext.propertyExists(AsynchAEMessage.Command)
              && aMessageContext.getMessageIntProperty(AsynchAEMessage.Command) == AsynchAEMessage.CollectionProcessComplete) {
        aCasReferenceId = ":CpC";
      }
      if (aMessageContext != null && aMessageContext.getEndpoint() != null) {
        Endpoint endpoint = lookupEndpoint(aMessageContext.getEndpoint().getEndpoint(),
                aCasReferenceId);

        if (endpoint != null) {
          // Received the response within timeout interval so
          // cancel the running timer
          endpoint.cancelTimer();
          if (removeEndpoint) {
            getController().getInProcessCache().removeEndpoint(
                    aMessageContext.getEndpoint().getEndpoint(), aCasReferenceId);
          }
        } else {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                    "cancelTimer", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_endpoint_not_found__INFO",
                    new Object[] { aMessageContext.getEndpoint().getEndpoint(), aCasReferenceId });
          }
        }
      }
    }
  }

  private void cancelTimerAndProcess(MessageContext aMessageContext, String aCasReferenceId,
          CAS aCAS) throws AsynchAEException {
    computeStats(aMessageContext, aCasReferenceId);

    super.invokeProcess(aCAS, aCasReferenceId, null, aMessageContext, null);

  }

  private boolean isMessageExpected(String aCasReferenceId, Endpoint anEndpointWithTimer)

  {
    if (getController().getInProcessCache().entryExists(aCasReferenceId)
            && anEndpointWithTimer.isWaitingForResponse()) {
      return true;
    }
    return false;
  }

  private void handleUnexpectedMessage(String aCasReferenceId, Endpoint anEndpoint) {
    // Cas does not exist in the CAS Cache. This would be possible if the
    // CAS has been dropped due to timeout and the delegate
    // sends the response later. In asynch communication this scenario is
    // possible. The service may not be up when the client sends
    // the message. Messages accumulate in the service queue until the
    // service becomes available. When this happens, the service
    // will pickup messages from the queue, process them and send respones
    // to an appropriate response queue. Most likely such
    // respones should be thrown away. Well perhaps logged first.
    ErrorContext errorContext = new ErrorContext();
    errorContext.add(AsynchAEMessage.CasReference, aCasReferenceId);
    errorContext.add(AsynchAEMessage.Command, AsynchAEMessage.Process);
    errorContext.add(AsynchAEMessage.Endpoint, anEndpoint);
    AnalysisEngineController controller = getController();
    controller.getErrorHandlerChain().handle(new ExpiredMessageException(), errorContext,
            controller);

  }

  private void handleProcessResponseFromRemote(MessageContext aMessageContext, String aDelegateKey) {
    CAS cas = null;
    String casReferenceId = null;
    Endpoint endpointWithTimer = null;
    try {
      final int payload = aMessageContext.getMessageIntProperty(AsynchAEMessage.Payload);
      casReferenceId = aMessageContext.getMessageStringProperty(AsynchAEMessage.CasReference);
      endpointWithTimer = lookupEndpoint(aMessageContext.getEndpoint().getEndpoint(),
              casReferenceId);

      if (endpointWithTimer == null) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handleProcessResponseFromRemote", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_invalid_endpoint__WARNING",
                  new Object[] { aMessageContext.getEndpoint().getEndpoint(), casReferenceId });
        }
        return;
      }
      String delegateKey = ((AggregateAnalysisEngineController) getController())
              .lookUpDelegateKey(aMessageContext.getEndpoint().getEndpoint());
      Delegate delegate = ((AggregateAnalysisEngineController) getController())
              .lookupDelegate(delegateKey);
      boolean casRemovedFromOutstandingList = delegate.removeCasFromOutstandingList(casReferenceId);

      // Check if this process reply message is expected. A message is expected if the Cas Id
      // in the message matches an entry in the delegate's outstanding list. This list stores
      // ids of CASes sent to the remote delegate pending reply.
      if (!casRemovedFromOutstandingList) {
        //handleUnexpectedMessage(casReferenceId, aMessageContext.getEndpoint());
        return;   // out of band reply. Most likely the CAS previously timedout
      }

      // Increment number of CASes processed by this delegate
      if (aDelegateKey != null) {
        ServicePerformance delegateServicePerformance = ((AggregateAnalysisEngineController) getController())
                .getServicePerformance(aDelegateKey);
        if (delegateServicePerformance != null) {
          delegateServicePerformance.incrementNumberOfCASesProcessed();
        }
      }

      String xmi = aMessageContext.getStringMessage();

      // Fetch entry from the cache for a given Cas Id. The entry contains a CAS that will be used
      // during deserialization
      CacheEntry cacheEntry = getController().getInProcessCache().getCacheEntryForCAS(
              casReferenceId);
      if ( aMessageContext.propertyExists(AsynchAEMessage.CASPerComponentMetrics) ) {
        try {
          CacheEntry ancestor = 
                  getController().
                    getInProcessCache().
                      getTopAncestorCasEntry(cacheEntry);
          if ( ancestor != null ) {
            List<AnalysisEnginePerformanceMetrics> metrics = 
                    UimaSerializer.deserializePerformanceMetrics(aMessageContext.getMessageStringProperty(AsynchAEMessage.CASPerComponentMetrics));
            
            List<AnalysisEnginePerformanceMetrics> adjustedMetrics =
                    new ArrayList<AnalysisEnginePerformanceMetrics>();
            for(AnalysisEnginePerformanceMetrics delegateMetric : metrics ) {
              String tmp =
                      delegateMetric.getUniqueName().substring(delegateMetric.getUniqueName().indexOf(","));
              String adjustedUniqueName =
                ((AggregateAnalysisEngineController) getController()).getJMXDomain()+((AggregateAnalysisEngineController) getController()).getJmxContext()+tmp;
              AnalysisEnginePerformanceMetrics metric =
                      new AnalysisEnginePerformanceMetrics(delegateMetric.getName(),adjustedUniqueName,delegateMetric.getAnalysisTime(),delegateMetric.getNumProcessed());
              adjustedMetrics.add(metric);
            }
            ancestor.addDelegateMetrics(delegateKey, adjustedMetrics, true);  // true=remote
          }
        } catch (Exception e) {
          // An exception be be thrown here if the service is being stopped.
          // The top level controller may have already cleaned up the cache
          // and the getCacheEntryForCAS() will throw an exception. Ignore it
          // here, we are shutting down.
        }
        
      }
      CasStateEntry casStateEntry = ((AggregateAnalysisEngineController) getController())
              .getLocalCache().lookupEntry(casReferenceId);
      if (casStateEntry != null) {
        casStateEntry.setReplyReceived();
        // Set the key of the delegate that returned the CAS
        casStateEntry.setLastDelegate(delegate);
      } else {
        return; // Cache Entry Not found
      }

      cas = cacheEntry.getCas();
      int totalNumberOfParallelDelegatesProcessingCas = casStateEntry
              .getNumberOfParallelDelegates();
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                "handleProcessResponseFromRemote", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_number_parallel_delegates_FINE",
                new Object[] { totalNumberOfParallelDelegatesProcessingCas, Thread.currentThread().getId(), Thread.currentThread().getName() });
      }
      if (cas == null) {
        throw new AsynchAEException(Thread.currentThread().getName()
                + "-Cache Does not contain a CAS. Cas Reference Id::" + casReferenceId);
      }
      if (UIMAFramework.getLogger().isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                "handleProcessResponseFromRemote", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_rcvd_reply_FINEST",
                new Object[] { aMessageContext.getEndpoint().getEndpoint(), casReferenceId, xmi });
      }
      long t1 = getController().getCpuTime();
      /* --------------------- */
      /** DESERIALIZE THE CAS. */
      /* --------------------- */
      //all subsequent serialization must be complete CAS.
      if ( !aMessageContext.getMessageBooleanProperty(AsynchAEMessage.SentDeltaCas))  {
    	cacheEntry.setAcceptsDeltaCas(false);
      }

      SerialFormat serialFormat = endpointWithTimer.getSerialFormat();
      // check if the CAS is part of the Parallel Step
      if (totalNumberOfParallelDelegatesProcessingCas > 1) {
        // Synchronized because replies are merged into the same CAS.
        synchronized (cas) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                    "handleProcessResponseFromRemote", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_delegate_responded_count_FINEST",
                    new Object[] { casStateEntry.howManyDelegatesResponded(), casReferenceId });
          }
          // If a delta CAS, merge it while checking that no pre-existing FSs are modified.
          if (aMessageContext.getMessageBooleanProperty(AsynchAEMessage.SentDeltaCas)) {
            switch (serialFormat) {
            case XMI:
              int highWaterMark = cacheEntry.getHighWaterMark();
              deserialize(xmi, cas, casReferenceId, highWaterMark, AllowPreexistingFS.disallow);
              break;
            case COMPRESSED_FILTERED:
              deserialize(aMessageContext.getByteMessage(), cas, cacheEntry, endpointWithTimer.getTypeSystemImpl(), AllowPreexistingFS.disallow);
              break;
            default:
              throw new UIMARuntimeException(new Exception("Internal error"));
            }
          } else {
            // If not a delta CAS (old service), take all of first reply, and merge in the new
            // entries in the later replies. Ignoring pre-existing FS for 2.2.2 compatibility
            // Note: can't be a compressed binary - that would have returned a delta
            if (casStateEntry.howManyDelegatesResponded() == 0) {
              deserialize(xmi, cas, casReferenceId);
            } else { // process secondary reply from a parallel step
              int highWaterMark = cacheEntry.getHighWaterMark();
              deserialize(xmi, cas, casReferenceId, highWaterMark, AllowPreexistingFS.ignore);
            }
          }
          casStateEntry.incrementHowManyDelegatesResponded();
        }
      } else { // Processing a reply from a non-parallel delegate (binary or delta xmi or xmi)
        byte[] binaryData = aMessageContext.getByteMessage();
        ByteArrayInputStream istream = new ByteArrayInputStream(binaryData);
        switch (serialFormat) {
        case BINARY:
          ((CASImpl)cas).reinit(istream);
          break;
        case COMPRESSED_FILTERED:
          BinaryCasSerDes6 bcs = new BinaryCasSerDes6(cas, (MarkerImpl) cacheEntry.getMarker(), endpointWithTimer.getTypeSystemImpl(), cacheEntry.getCompress6ReuseInfo());          
          bcs.deserialize(istream, AllowPreexistingFS.allow);
          break;
        case XMI:
          if (aMessageContext.getMessageBooleanProperty(AsynchAEMessage.SentDeltaCas)) {
            int highWaterMark = cacheEntry.getHighWaterMark();
            deserialize(xmi, cas, casReferenceId, highWaterMark, AllowPreexistingFS.allow);
          } else {
            deserialize(xmi, cas, casReferenceId);
          }
          break;
        default:
          throw new UIMARuntimeException(new Exception("Internal error"));
        }
      }
      long timeToDeserializeCAS = getController().getCpuTime() - t1;

      getController().getServicePerformance().incrementCasDeserializationTime(timeToDeserializeCAS);

      ServicePerformance casStats = getController().getCasStatistics(casReferenceId);
      casStats.incrementCasDeserializationTime(timeToDeserializeCAS);
      LongNumericStatistic statistic;
      if ((statistic = getController().getMonitor().getLongNumericStatistic("",
              Monitor.TotalDeserializeTime)) != null) {
        statistic.increment(timeToDeserializeCAS);
      }

      computeStats(aMessageContext, casReferenceId);

      // Send CAS for processing when all delegates reply
      // totalNumberOfParallelDelegatesProcessingCas indicates how many delegates are processing CAS
      // in parallel. Default is 1, meaning only one delegate processes the CAS at the same.
      // Otherwise, check if all delegates responded before passing CAS on to the Flow Controller.
      // The idea is that all delegates processing one CAS concurrently must respond, before the CAS
      // is allowed to move on to the next step.
      // HowManyDelegatesResponded is incremented every time a parallel delegate sends response.
      if (totalNumberOfParallelDelegatesProcessingCas == 1
              || receivedAllResponsesFromParallelDelegates(casStateEntry,
                      totalNumberOfParallelDelegatesProcessingCas)) {
        super.invokeProcess(cas, casReferenceId, null, aMessageContext, null);
      }

    } catch (Exception e) {
      // Check if the exception was thrown by the Cache while looking up
      // the CAS. It may be the case if in the parallel step one thread
      // drops the CAS in the Error Handling while another thread processes
      // reply from another delegate in the Parallel Step. A race condition
      // may occur here. If one thread drops the CAS due to excessive exceptions
      // and Flow Controller is configured to drop the CAS, the other thread
      // should not be allowed to move the CAS to process()method. The second
      // thread will find the CAS missing in the cache and the logic below
      // just logs the stale CAS and returns and doesnt attempt to handle
      // the missing CAS exception.
      if (e instanceof AsynchAEException && e.getMessage() != null
              && e.getMessage().startsWith("Cas Not Found")) {
        String key = "N/A";
        if (endpointWithTimer != null) {
          key = ((AggregateAnalysisEngineController) getController())
                  .lookUpDelegateKey(endpointWithTimer.getEndpoint());
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "handleProcessResponseFromRemote", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_stale_reply__INFO",
                  new Object[] { getController().getComponentName(), key, casReferenceId });
        }
        // The reply came late. The CAS was removed from the cache.
        return;
      }
      ErrorContext errorContext = new ErrorContext();
      errorContext.add(AsynchAEMessage.Command, AsynchAEMessage.Process);
      errorContext.add(AsynchAEMessage.CasReference, casReferenceId);
      errorContext.add(AsynchAEMessage.Endpoint, aMessageContext.getEndpoint());
      getController().getErrorHandlerChain().handle(e, errorContext, getController());
    } finally {
      incrementDelegateProcessCount(aMessageContext);
    }

  }

  private synchronized boolean receivedAllResponsesFromParallelDelegates(
          CasStateEntry aCasStateEntry, int totalNumberOfParallelDelegatesProcessingCas) {
    if (aCasStateEntry.howManyDelegatesResponded() == totalNumberOfParallelDelegatesProcessingCas) {
      aCasStateEntry.resetDelegateResponded();
      return true;
    }
    return false;
  }

  private void deserialize(String xmi, CAS cas, String casReferenceId, int highWaterMark,
          AllowPreexistingFS allow) throws Exception {
    XmiSerializationSharedData deserSharedData;
    deserSharedData = getController().getInProcessCache().getCacheEntryForCAS(casReferenceId)
            .getDeserSharedData();
    UimaSerializer uimaSerializer = SerializerCache.lookupSerializerByThreadId();
    uimaSerializer.deserializeCasFromXmi(xmi, cas, deserSharedData, true, highWaterMark, allow);
  }
  
  private void deserialize(byte[] bytes, CAS cas, CacheEntry cacheEntry, TypeSystemImpl remoteTs,
          AllowPreexistingFS allow) throws Exception {
    ByteArrayInputStream istream = new ByteArrayInputStream(bytes);
    ReuseInfo reuseInfo = cacheEntry.getCompress6ReuseInfo();

    Serialization.deserializeCAS(cas, istream, remoteTs, reuseInfo, allow);
  }

  private void deserialize(String xmi, CAS cas, String casReferenceId) throws Exception {
    CacheEntry entry = getController().getInProcessCache().getCacheEntryForCAS(casReferenceId);
    // Processing the reply from a standard, non-parallel delegate
    XmiSerializationSharedData deserSharedData;
    deserSharedData = entry.getDeserSharedData();
    if (deserSharedData == null) {
      deserSharedData = new XmiSerializationSharedData();
      entry.setXmiSerializationData(deserSharedData);
    }
    UimaSerializer uimaSerializer = SerializerCache.lookupSerializerByThreadId();
    uimaSerializer.deserializeCasFromXmi(xmi, cas, deserSharedData, true, -1);
  }

  private void handleProcessResponseWithCASReference(MessageContext aMessageContext) {
    String casReferenceId = null;
    CacheEntry cacheEntry = null;

    try {
      casReferenceId = aMessageContext.getMessageStringProperty(AsynchAEMessage.CasReference);
      cacheEntry = getController().getInProcessCache().getCacheEntryForCAS(casReferenceId);
      CasStateEntry casStateEntry = ((AggregateAnalysisEngineController) getController())
              .getLocalCache().lookupEntry(casReferenceId);

      CAS cas = cacheEntry.getCas();
      String delegateKey = ((AggregateAnalysisEngineController) getController())
              .lookUpDelegateKey(aMessageContext.getEndpoint().getEndpoint());
      Delegate delegate = ((AggregateAnalysisEngineController) getController())
              .lookupDelegate(delegateKey);
      if (casStateEntry != null) {
        casStateEntry.setReplyReceived();
        casStateEntry.setLastDelegate(delegate);
      }
      delegate.removeCasFromOutstandingList(casReferenceId);

      if (cas != null) {
        cancelTimerAndProcess(aMessageContext, casReferenceId, cas);
      } else {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.INFO,
                  CLASS_NAME.getName(),
                  "handleProcessResponseWithCASReference",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_cas_not_in_cache__INFO",
                  new Object[] { getController().getName(), casReferenceId,
                      aMessageContext.getEndpoint().getEndpoint() });
        }
        throw new AsynchAEException("CAS with Reference Id:" + casReferenceId
                + " Not Found in CasManager's CAS Cache");
      }
    } catch (Exception e) {

      ErrorContext errorContext = new ErrorContext();
      errorContext.add(AsynchAEMessage.Command, AsynchAEMessage.Process);
      errorContext.add(AsynchAEMessage.CasReference, casReferenceId);
      errorContext.add(AsynchAEMessage.Endpoint, aMessageContext.getEndpoint());
      getController().getErrorHandlerChain().handle(e, errorContext, getController());
    } finally {
      incrementDelegateProcessCount(aMessageContext);
      if (getController() instanceof AggregateAnalysisEngineController) {
        try {
          String endpointName = aMessageContext.getEndpoint().getEndpoint();
          String delegateKey = ((AggregateAnalysisEngineController) getController())
                  .lookUpDelegateKey(endpointName);
          if (delegateKey != null) {
            Endpoint endpoint = ((AggregateAnalysisEngineController) getController())
                    .lookUpEndpoint(delegateKey, false);

            // Check if the multiplier aborted during processing of this input CAS
            if (endpoint != null && endpoint.isCasMultiplier() && cacheEntry.isAborted()) {
              if (!getController().getInProcessCache().isEmpty()) {
                getController().getInProcessCache().registerCallbackWhenCacheEmpty(
                        getController().getEventListener());
              } else {
                // Callback to notify that the cache is empty
                getController().getEventListener().onCacheEmpty();
              }
            }

          }
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            if ( getController() != null ) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                      "handleProcessResponseWithCASReference", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_service_exception_WARNING", getController().getComponentName());
            }

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "handleProcessResponseWithCASReference", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", e);
          }
        }
      }
    }

  }

  private void incrementDelegateProcessCount(MessageContext aMessageContext) {
    Endpoint endpoint = aMessageContext.getEndpoint();
    if (endpoint != null && getController() instanceof AggregateAnalysisEngineController) {
      try {
        String delegateKey = ((AggregateAnalysisEngineController) getController())
                .lookUpDelegateKey(endpoint.getEndpoint());
        LongNumericStatistic stat = getController().getMonitor().getLongNumericStatistic(
                delegateKey, Monitor.ProcessCount);
        stat.increment();
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                    "incrementDelegateProcessCount", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_delegate_key_for_endpoint_not_found__INFO", new Object[] { getController().getComponentName(), endpoint.getEndpoint() });
        }
      }
    }

  }

  private boolean isException(Object object) {
    return (object instanceof Exception || object instanceof Throwable);
  }

  private boolean isShutdownException(Object object) {
    return (object instanceof Exception && object instanceof UimaEEServiceException
            && ((UimaEEServiceException) object).getCause() != null && ((UimaEEServiceException) object)
            .getCause() instanceof ServiceShutdownException);
  }

  private boolean ignoreException(Object object) {
    if (object != null && isException(object) && !isShutdownException(object)) {
      return false;
    }
    return true;
  }

  private synchronized void handleProcessResponseWithException(MessageContext aMessageContext,
          String delegateKey) {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME)
              .logrb(
                      Level.FINE,
                      CLASS_NAME.getName(),
                      "handleProcessResponseWithException",
                      UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_handling_exception_from_delegate_FINE",
                      new Object[] { getController().getName(),
                          aMessageContext.getEndpoint().getEndpoint() });
    }
    boolean isCpCError = false;
    String casReferenceId = null;

    try {
      // If a Process Request, increment number of docs processed
      if (aMessageContext.getMessageIntProperty(AsynchAEMessage.MessageType) == AsynchAEMessage.Response
              && aMessageContext.getMessageIntProperty(AsynchAEMessage.Command) == AsynchAEMessage.Process) {
        // Increment number of CASes processed by a delegate
        incrementDelegateProcessCount(aMessageContext);
      }

      Object object = aMessageContext.getObjectMessage();
      if (object == null) {
        // Could be a C++ exception. In this case the exception is just a String in the message
        // cargo
        if (aMessageContext.getStringMessage() != null) {
          object = new UimaEEServiceException(aMessageContext.getStringMessage());
        }
      }
      if (ignoreException(object)) {
        return;
      }

      if (getController() instanceof AggregateAnalysisEngineController
              && aMessageContext.propertyExists(AsynchAEMessage.Command)
              && aMessageContext.getMessageIntProperty(AsynchAEMessage.Command) == AsynchAEMessage.CollectionProcessComplete) {
        isCpCError = true;
        ((AggregateAnalysisEngineController) getController())
                .processCollectionCompleteReplyFromDelegate(delegateKey, false);
      } else {
        casReferenceId = aMessageContext.getMessageStringProperty(AsynchAEMessage.CasReference);
      }

      if (object != null && (object instanceof Exception || object instanceof Throwable)) {
        String casid_msg = (casReferenceId == null) ? "" : " on CAS:" + casReferenceId;
        String controllerName = "/" + getController().getComponentName();
        if (!getController().isTopLevelComponent()) {
          controllerName += ((BaseAnalysisEngineController) getController().getParentController())
                  .getUimaContextAdmin().getQualifiedContextName();
        }
        Exception remoteException = new UimaAsDelegateException("----> Controller:"
                + controllerName + " Received Exception " + casid_msg + " From Delegate:"
                + delegateKey, (Exception) object);
        ErrorContext errorContext = new ErrorContext();
        errorContext.add(AsynchAEMessage.Command, aMessageContext
                .getMessageIntProperty(AsynchAEMessage.Command));
        errorContext.add(AsynchAEMessage.MessageType, aMessageContext
                .getMessageIntProperty(AsynchAEMessage.MessageType));
        if (!isCpCError) {
          errorContext.add(AsynchAEMessage.CasReference, casReferenceId);
        }
        errorContext.add(AsynchAEMessage.Endpoint, aMessageContext.getEndpoint());
        getController().getErrorHandlerChain().handle(remoteException, errorContext,
                getController());
      }
    } catch (Exception e) {
      ErrorContext errorContext = new ErrorContext();
      errorContext.add(AsynchAEMessage.Command, AsynchAEMessage.Process);
      errorContext.add(AsynchAEMessage.CasReference, casReferenceId);
      errorContext.add(AsynchAEMessage.Endpoint, aMessageContext.getEndpoint());
      getController().getErrorHandlerChain().handle(e, errorContext, getController());
    }

  }

  private void handleCollectionProcessCompleteReply(MessageContext aMessageContext,
          String delegateKey) {
    try {
      if (getController() instanceof AggregateAnalysisEngineController) {
        ((AggregateAnalysisEngineController) getController())
                .processCollectionCompleteReplyFromDelegate(delegateKey, true);
      }
    } catch (Exception e) {
      ErrorContext errorContext = new ErrorContext();
      errorContext.add(AsynchAEMessage.Command, AsynchAEMessage.CollectionProcessComplete);
      errorContext.add(AsynchAEMessage.Endpoint, aMessageContext.getEndpoint());
      getController().getErrorHandlerChain().handle(e, errorContext, getController());
    }

  }

  private void resetErrorCounts(String aDelegate) {
    getController().getMonitor().resetCountingStatistic(aDelegate, Monitor.ProcessErrorCount);
    getController().getMonitor().resetCountingStatistic(aDelegate, Monitor.ProcessErrorRetryCount);
  }

  private void handlePingReply(MessageContext aMessageContext) {
    try {

    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( getController() != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handlePingReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", getController().getComponentName());
        }

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "handlePingReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }
  }

  private void handleServiceInfoReply(MessageContext messageContext) {

    String casReferenceId = null;
    try {
      casReferenceId = messageContext.getMessageStringProperty(AsynchAEMessage.CasReference);
      if ( casReferenceId == null ) {
    	  return;  // nothing to do
      }
      Endpoint freeCasEndpoint = messageContext.getEndpoint();
      CasStateEntry casStateEntry = ((AggregateAnalysisEngineController) getController())
              .getLocalCache().lookupEntry(casReferenceId);
      if (casStateEntry != null) {
        casStateEntry.setFreeCasNotificationEndpoint(freeCasEndpoint);
        //  Fetch host IP where the CAS is being processed. When the UIMA AS service
        //  receives a CAS it immediately sends ServiceInfo Reply message containing 
        //  IP of the host where the service is running.
        String serviceHostIp = messageContext.getMessageStringProperty(AsynchAEMessage.ServerIP);
        if ( serviceHostIp != null ) {
          casStateEntry.setHostIpProcessingCAS(serviceHostIp);
        }
      }
    } catch (Exception e) {
    	UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "handleServiceInfoReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
    	return;
    }

  }

  public void handle(Object anObjectToHandle) throws AsynchAEException {
    super.validate(anObjectToHandle);
    MessageContext messageContext = (MessageContext) anObjectToHandle;

    if (isHandlerForMessage(messageContext, AsynchAEMessage.Response, AsynchAEMessage.Process)
            || isHandlerForMessage(messageContext, AsynchAEMessage.Response, AsynchAEMessage.ACK)
            || isHandlerForMessage(messageContext, AsynchAEMessage.Response,
                    AsynchAEMessage.ServiceInfo)
            || isHandlerForMessage(messageContext, AsynchAEMessage.Response,
                    AsynchAEMessage.CollectionProcessComplete)) {
      int payload = messageContext.getMessageIntProperty(AsynchAEMessage.Payload);
      int command = messageContext.getMessageIntProperty(AsynchAEMessage.Command);
      String delegate = ((Endpoint) messageContext.getEndpoint()).getEndpoint();
      String key = null;
      String fromServer = null;
      if (getController() instanceof AggregateAnalysisEngineController) {
        if (((Endpoint) messageContext.getEndpoint()).isRemote()) {
          if (((MessageContext) anObjectToHandle).propertyExists(AsynchAEMessage.EndpointServer)) {
            fromServer = ((MessageContext) anObjectToHandle)
                    .getMessageStringProperty(AsynchAEMessage.EndpointServer);
          }
          // If old service does not echo back the external broker name then the queue name must be
          // unique.
          // Can't use the ServerURI set by the service as it may be its local name for the broker,
          // e.g. tcp://localhost:61616
        }

        key = ((AggregateAnalysisEngineController) getController()).lookUpDelegateKey(delegate,
                fromServer);
      }
      if (AsynchAEMessage.CASRefID == payload) {

        handleProcessResponseWithCASReference(messageContext);
        if (key != null) {
          resetErrorCounts(key);
        }
      } else if (AsynchAEMessage.XMIPayload == payload || AsynchAEMessage.BinaryPayload == payload) {
        handleProcessResponseFromRemote(messageContext, key);
        if (key != null) {
          resetErrorCounts(key);
        }
      } else if (AsynchAEMessage.Exception == payload) {
        if (key == null) {
          key = ((Endpoint) messageContext.getEndpoint()).getEndpoint();
        }
        handleProcessResponseWithException(messageContext, key);
      } else if (AsynchAEMessage.None == payload
              && AsynchAEMessage.CollectionProcessComplete == command) {
        if (key == null) {
          key = ((Endpoint) messageContext.getEndpoint()).getEndpoint();
        }
        handleCollectionProcessCompleteReply(messageContext, key);
      } else if (AsynchAEMessage.None == payload && AsynchAEMessage.ACK == command) {
        handleACK(messageContext, key);
      } else if (AsynchAEMessage.None == payload && AsynchAEMessage.Ping == command) {
        handlePingReply(messageContext);
      } else if (AsynchAEMessage.None == payload && AsynchAEMessage.ServiceInfo == command) {
        handleServiceInfoReply(messageContext);
      } else {
        throw new AsynchAEException("Invalid Payload. Expected XMI or CasReferenceId Instead Got::"
                + payload);
      }

      // Handled Request to Process with A given Payload
      return;
    }
    // Not a Request nor Command. Delegate to the next handler in the chain
    super.delegate(messageContext);

  }

  private void handleACK(MessageContext aMessageContext, String key) throws AsynchAEException {
  }

}
