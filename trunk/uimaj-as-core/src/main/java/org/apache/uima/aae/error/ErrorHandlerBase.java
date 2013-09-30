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

package org.apache.uima.aae.error;

import java.util.HashMap;
import java.util.Map;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.InputChannel;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.PrimitiveAnalysisEngineController;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.monitor.Monitor;
import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.aae.monitor.statistics.Statistic;
import org.apache.uima.util.Level;

public abstract class ErrorHandlerBase {
  private static final Class CLASS_NAME = ErrorHandlerBase.class;

  protected Map endpointThresholdMap;

  public ErrorHandlerBase(Map anEndpointThreasholdMap) {
    endpointThresholdMap = anEndpointThreasholdMap;
  }

  public ErrorHandlerBase() {
    endpointThresholdMap = new HashMap();
  }

  public Map getEndpointThresholdMap() {
    return endpointThresholdMap;
  }

  protected String getAction(String aThresholdToCheck, String endpoint) {
    Threshold threshold = getThreshold(aThresholdToCheck, endpoint);
    if (threshold != null) {
      return threshold.getAction();
    }
    return null;
  }

  protected String getDelegateKey(Endpoint anEndpoint, AnalysisEngineController aController) {
    String key = null;
    if (aController instanceof PrimitiveAnalysisEngineController
            && aController.isTopLevelComponent()) {
      key = aController.getServiceEndpointName();
    } else if (anEndpoint != null) {
      key = anEndpoint.getEndpoint();

    }
    return key;

  }

  protected boolean isValidActionForController(String anAction, AnalysisEngineController aController) {
    if (aController instanceof PrimitiveAnalysisEngineController) {
      if (ErrorHandler.DISABLE.equalsIgnoreCase(anAction)) {
        return false;
      }
    }
    return true;
  }

  protected Threshold getThreshold(String aThresholdToCheck, String endpoint) {
    if (endpointThresholdMap.containsKey(endpoint)) {
      EndpointThresholds endpointThresholds = (EndpointThresholds) endpointThresholdMap
              .get(endpoint);
      Threshold threshold = endpointThresholds.getThreshold(aThresholdToCheck);
      return threshold;
    }
    return null;
  }

  protected boolean exceedsThreshold(String aThresholdToCheck, String endpoint,
          AnalysisEngineController controller) {
    Threshold threshold = getThreshold(aThresholdToCheck, endpoint);
    if (threshold != null) {
      Monitor monitor = controller.getMonitor();
      Statistic statistic = null;
      if ((statistic = monitor.getStatistic(endpoint, aThresholdToCheck)) == null) {
        statistic = new LongNumericStatistic(aThresholdToCheck);
        monitor.addStatistic(endpoint, statistic);
      }
      if (statistic instanceof LongNumericStatistic) {
        ((LongNumericStatistic) statistic).increment();
        if (threshold.exceeded(((LongNumericStatistic) statistic).getValue())) {
          return true;
        }
      }
    }
    return false;
  }

  protected boolean exceedsThreshold(Threshold aThreshold, String aThresholdToCheck,
          String endpoint, AnalysisEngineController controller) {
    if (aThreshold != null) {
      Monitor monitor = controller.getMonitor();
      Statistic statistic = null;

      if ((statistic = monitor.getStatistic(endpoint, aThresholdToCheck)) == null) {
        statistic = new LongNumericStatistic(aThresholdToCheck);
        monitor.addStatistic(endpoint, statistic);
      }
      if (Monitor.GetMetaErrorRetryCount.equals(aThresholdToCheck)
              || Monitor.ProcessErrorRetryCount.equals(aThresholdToCheck)) {
        return aThreshold.maxRetriesExceeded(((LongNumericStatistic) statistic).getValue());
      } else {
        return aThreshold.exceeded(((LongNumericStatistic) statistic).getValue());
      }
    }
    return false;
  }

  protected String getEndpointName(AnalysisEngineController aController, ErrorContext anErrorContext) {
    String key = null;
    if (aController instanceof PrimitiveAnalysisEngineController) {
      key = aController.getServiceEndpointName();
    } else {
      Endpoint endpoint = (Endpoint) anErrorContext.get(AsynchAEMessage.Endpoint);
      key = endpoint.getEndpoint();
    }

    return key;

  }

  protected boolean isHandlerForError(ErrorContext anErrorContext, int anExpectedCommand) {
    if (anErrorContext != null) {
      int command = (Integer) anErrorContext.get(AsynchAEMessage.Command);
      return (command == anExpectedCommand) ? true : false;
    }
    return false;
  }

  protected boolean shouldRetry(Threshold aThreshold, String aKindOfRetryCount, String aKey,
          AnalysisEngineController aController) {
    return (exceedsThreshold(aThreshold, aKindOfRetryCount, aKey, aController) == true) ? false
            : true;
  }

  protected Threshold getThreshold(Endpoint anEndpoint, Map aDelegateMap,
          AnalysisEngineController aController) {
    Threshold threshold = null;
    if (aController instanceof AggregateAnalysisEngineController && anEndpoint != null) {
      String key = ((AggregateAnalysisEngineController) aController).lookUpDelegateKey(anEndpoint
              .getEndpoint());
      if (aDelegateMap.containsKey(key)) {
        threshold = (Threshold) aDelegateMap.get(key);
      }
    }
    return threshold;
  }

  protected synchronized void incrementStatistic(Monitor aMonitor, String aComponentName,
          String aStatistic) {
    Statistic statistic = aMonitor.getStatistic(aComponentName, aStatistic);
    if (statistic == null) {
      statistic = new LongNumericStatistic(aStatistic);
      aMonitor.addStatistic(aComponentName, statistic);
    }

    if (statistic instanceof LongNumericStatistic) {
      ((LongNumericStatistic) statistic).increment();
    }
  }

  protected boolean retryLastCommand(int aRetryCommand, Endpoint anEndpoint,
          AnalysisEngineController aController, String aKey, Threshold aThreshold,
          ErrorContext anErrorContext) {
    boolean done = false;

    if ( aController == null ) {
    	return false;
    }
    String errorCounterKind = (aRetryCommand == AsynchAEMessage.GetMeta) ? Monitor.GetMetaErrorRetryCount
            : Monitor.ProcessErrorRetryCount;
    // Handle errors in a loop. Retry until retry threshold is reached
    int retryCount = 0;
    while (!done) {
      /*
       * if ( !exceedsThreshold(aThreshold, errorCounterKind, aKey, aController)) { // Increment
       * number of retries incrementStatistic(aController.getMonitor(), aKey, errorCounterKind ); }
       */
      // Check if exceeding threshold
      if (shouldRetry(aThreshold, errorCounterKind, aKey, aController)) {
        incrementStatistic(aController.getMonitor(), aKey, errorCounterKind);
        try {
          switch (aRetryCommand) {
            case AsynchAEMessage.GetMeta:
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                          "retryLastCommand", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_retrying_getmeta__INFO", new Object[] { aController.getComponentName(), anEndpoint.getDelegateKey() });
              }

              
              // Retry GetMeta
              ((AggregateAnalysisEngineController) aController).retryMetadataRequest(anEndpoint);
              break;

            case AsynchAEMessage.Process:
              if ( anErrorContext.containsKey(AsynchAEMessage.Endpoint)) {
                Endpoint masterEndpoint = (Endpoint)anErrorContext.get(AsynchAEMessage.Endpoint);

                if (aController instanceof AggregateAnalysisEngineController && (masterEndpoint != null && masterEndpoint.getStatus() == Endpoint.FAILED)) {
                  // Fetch an InputChannel that handles messages for a given delegate
                  InputChannel iC = aController.getReplyInputChannel(masterEndpoint.getDelegateKey());
                  // Create a new Listener, new Temp Queue and associate the listener with the Input Channel
                  iC.createListener(masterEndpoint.getDelegateKey(), null);
                  iC.removeDelegateFromFailedList(masterEndpoint.getDelegateKey());
                  anEndpoint.setDestination(masterEndpoint.getDestination());
                }
                
              }
              String casReferenceId = (String) anErrorContext.get(AsynchAEMessage.CasReference);
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                          "retryLastCommand", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_retrying_process_cas__INFO", new Object[] { aController.getComponentName(), casReferenceId, anEndpoint.getDelegateKey() });
              }
              ((AggregateAnalysisEngineController) aController).retryProcessCASRequest(
                      casReferenceId, anEndpoint, true);
              break;
          }
          return true;
        } catch (Exception e) {
          anEndpoint.cancelTimer();
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                      "retryLastCommand", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_service_exception_WARNING", aController.getComponentName());

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                    "retryLastCommand", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_exception__WARNING", e);
          }
        }
      } else {
        done = true;
      }
    }
    return false;
  }

  protected synchronized boolean exceedsThresholdWithinWindow(Monitor aMonitor, String aStat,
          String aComponent, Threshold aThreshold) {
    LongNumericStatistic currentErrorCountStat = aMonitor
            .getLongNumericStatistic(aComponent, aStat);
    LongNumericStatistic currentProcessCountStat = aMonitor.getLongNumericStatistic(aComponent,
            Monitor.ProcessCount);
    long numberOfErrors = currentErrorCountStat.getValue();
    // Check if threshold exceeded
    if (numberOfErrors > 0 && aThreshold.getThreshold() > 0
            && numberOfErrors % aThreshold.getThreshold() == 0) {
      return true;
    }
    // Check if reached end of window. If so, begin counting against a new window
    if (aThreshold.getThreshold() > 0 && aThreshold.getWindow() > 0
            && currentProcessCountStat.getValue() % aThreshold.getWindow() == 0) {
      aMonitor.resetCountingStatistic(aComponent, aStat);
    }
    return false;
  }

  protected boolean continueOnError(String aDelegateKey, Threshold aThreshold,
          String aCasReferenceId, Throwable t, AnalysisEngineController aController) {
    try {
      if (aThreshold.getContinueOnRetryFailure() == true
              && ((AggregateAnalysisEngineController) aController).continueOnError(aCasReferenceId,
                      aDelegateKey, (Exception) t)) {
        return true;
      }
    } catch (Exception e) {
      if ( aController != null ) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "continueOnError", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", aController.getComponentName());
      }

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "continueOnError", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e); 
      }

    }
    return false;
  }

}
