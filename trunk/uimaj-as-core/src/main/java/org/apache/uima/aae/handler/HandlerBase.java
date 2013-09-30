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

package org.apache.uima.aae.handler;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaSerializer;
import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.PrimitiveAnalysisEngineController;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.jmx.ServicePerformance;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.message.UIMAMessage;
import org.apache.uima.aae.monitor.Monitor;
import org.apache.uima.cas.CAS;
import org.apache.uima.util.Level;

public abstract class HandlerBase implements Handler {
  private static final Class CLASS_NAME = HandlerBase.class;

  private Handler delegateHandler;

  private Handler parentHandler;

  private String handlerName;

  private AnalysisEngineController controller;

  public AnalysisEngineController getController() {
    return controller;
  }

  public void setController(AnalysisEngineController controller) {
    this.controller = controller;
  }

  public static ErrorContext populateErrorContext(MessageContext aMessageCtx) {
    ErrorContext errorContext = new ErrorContext();
    if (aMessageCtx != null) {
      try {
        if (aMessageCtx.propertyExists(AsynchAEMessage.Command)) {
          errorContext.add(AsynchAEMessage.Command, aMessageCtx
                  .getMessageIntProperty(AsynchAEMessage.Command));
        }

        if (aMessageCtx.propertyExists(AsynchAEMessage.MessageType)) {
          errorContext.add(AsynchAEMessage.MessageType, aMessageCtx
                  .getMessageIntProperty(AsynchAEMessage.MessageType));
        }

        if (aMessageCtx.propertyExists(AsynchAEMessage.CasReference)) {
          errorContext.add(AsynchAEMessage.CasReference, aMessageCtx
                  .getMessageStringProperty(AsynchAEMessage.CasReference));
        }
        errorContext.add(UIMAMessage.RawMsg, aMessageCtx.getRawMessage());
      } catch (Exception e) { /* ignore */
      }
    }
    return errorContext;
  }

  public void validate(Object anObjectToHandle) throws AsynchAEException {
    if (anObjectToHandle == null) {
      throw new AsynchAEException("Nothing to Handle - Input Object is Null");
    }
    if (!(anObjectToHandle instanceof MessageContext)) {
      throw new AsynchAEException(
              "Invalid Object in Handler. Expected MessageContext Instead Got::"
                      + anObjectToHandle.getClass().getName());
    }
  }

  public boolean isHandlerForMessage(MessageContext aMessageContext, int anExpectedMessageType,
          int anExpectedCommand) throws AsynchAEException {
    int messageType = aMessageContext.getMessageIntProperty(AsynchAEMessage.MessageType);
    int command = aMessageContext.getMessageIntProperty(AsynchAEMessage.Command);
    if (anExpectedMessageType == messageType && anExpectedCommand == command) {
      return true;
    }
    return false;
  }

  public void invokeProcess(CAS aCAS, String anInputCasReferenceId, String aNewCasReferenceId,
          MessageContext aMessageContext, String aNewCasProducedBy) throws AsynchAEException {
    try {
      // Use empty string as key. Top level component stats are stored under this key.
      controller.getMonitor().incrementCount("", Monitor.ProcessCount);

      if (controller instanceof AggregateAnalysisEngineController) {
        boolean isNewCAS = aMessageContext.propertyExists(AsynchAEMessage.CasSequence);
        if (isNewCAS) {
          ((AggregateAnalysisEngineController) controller).process(aCAS, anInputCasReferenceId,
                  aNewCasReferenceId, aNewCasProducedBy);
        } else {
          ((AggregateAnalysisEngineController) controller).process(aCAS, anInputCasReferenceId);
        }
      } else if (controller instanceof PrimitiveAnalysisEngineController) {
        ((PrimitiveAnalysisEngineController) controller).process(aCAS, anInputCasReferenceId,
                aMessageContext.getEndpoint());
      } else {
        throw new AsynchAEException(
                "Invalid Controller. Expected AggregateController or PrimitiveController. Got:"
                        + controller.getClass().getName());
      }
    } catch (AsynchAEException e) {
      throw e;
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }

  }

  public void delegate(MessageContext aMessageContext) throws AsynchAEException {
    int messageType = aMessageContext.getMessageIntProperty(AsynchAEMessage.MessageType);
    if (hasDelegateHandler()) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "delegate",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_dispatch_msg_to_handler__FINE",
                new Object[] { messageType, aMessageContext.getEndpoint().getEndpoint(),
                    getDelegate().getName() });
      }
      getDelegate().handle(aMessageContext);
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "delegate",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_no_handler_for_message__FINE",
                new Object[] { messageType, aMessageContext.getEndpoint().getEndpoint() });
      }
    }
  }

  public HandlerBase(String aName) {
    handlerName = aName;
  }

  public Handler getDelegate() {
    return delegateHandler;
  }

  public void setParent(Handler aHandler) {
    parentHandler = aHandler;
  }

  public String getName() {
    return handlerName;
  }

  public void setDelegate(Handler aHandler) {
    delegateHandler = aHandler;
  }

  public boolean hasDelegateHandler() {
    return (delegateHandler != null);
  }

  public void removeDelegate(String aHandlerName) {
    if (handlerName.equals(aHandlerName)) {
      if (hasDelegateHandler()) {
        // connect the delegate with the parent of this handler
        ((HandlerBase) delegateHandler).setParent(parentHandler);
        parentHandler.setDelegate(delegateHandler);
      }
    } else {
      delegateHandler.removeDelegate(aHandlerName);
    }

  }

  public void resequenceDelegateHandler(String aHandlerName, int aNewPositionInHandlerChain) {
    // TODO Auto-generated method stub

  }

  public void handle(Object anObjectToHandle) throws AsynchAEException {
    getController().getControllerLatch().waitUntilInitialized();
  }

  public void handle(Object anObjectToHandle, String expectedOutputType) throws AsynchAEException {
  }

  protected synchronized void aggregateDelegateStats(MessageContext aMessageContext,
          String aCasReferenceId) throws AsynchAEException {
    String delegateKey = "";
    try {

      delegateKey = ((AggregateAnalysisEngineController) getController())
              .lookUpDelegateKey(aMessageContext.getEndpoint().getEndpoint());
      CacheEntry entry = getController().getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
      if (entry == null) {
        throw new AsynchAEException("CasReferenceId:" + aCasReferenceId
                + " Not Found in the Cache.");
      }
      CacheEntry inputCasEntry = null;
      String inputCasReferenceId = entry.getInputCasReferenceId();
      ServicePerformance casStats = ((AggregateAnalysisEngineController) getController())
              .getCasStatistics(aCasReferenceId);
      if (inputCasReferenceId != null
              && getController().getInProcessCache().entryExists(inputCasReferenceId)) {
        String casProducerKey = entry.getCasProducerKey();
        if (casProducerKey != null
                && ((AggregateAnalysisEngineController) getController())
                        .isDelegateKeyValid(casProducerKey)) {
          // Get entry for the input CAS
          inputCasEntry = getController().getInProcessCache().getCacheEntryForCAS(
                  inputCasReferenceId);
        }

      }
      ServicePerformance delegateServicePerformance = ((AggregateAnalysisEngineController) getController())
              .getServicePerformance(delegateKey);

      if (aMessageContext.propertyExists(AsynchAEMessage.TimeToSerializeCAS)) {
        long timeToSerializeCAS = ((Long) aMessageContext
                .getMessageLongProperty(AsynchAEMessage.TimeToSerializeCAS)).longValue();
        if (timeToSerializeCAS > 0) {
          if (delegateServicePerformance != null) {
            delegateServicePerformance.incrementCasSerializationTime(timeToSerializeCAS);
          }
        }
      }
      if (aMessageContext.propertyExists(AsynchAEMessage.TimeToDeserializeCAS)) {
        long timeToDeserializeCAS = ((Long) aMessageContext
                .getMessageLongProperty(AsynchAEMessage.TimeToDeserializeCAS)).longValue();
        if (timeToDeserializeCAS > 0) {
          if (delegateServicePerformance != null) {
            delegateServicePerformance.incrementCasDeserializationTime(timeToDeserializeCAS);
          }
        }
      }

      if (aMessageContext.propertyExists(AsynchAEMessage.IdleTime)) {
        long idleTime = ((Long) aMessageContext.getMessageLongProperty(AsynchAEMessage.IdleTime))
                .longValue();
        if (idleTime > 0 && delegateServicePerformance != null) {
          Endpoint endp = aMessageContext.getEndpoint();
          if (endp != null && endp.isRemote()) {
            delegateServicePerformance.incrementIdleTime(idleTime);
          }
        }
      }

      if (aMessageContext.propertyExists(AsynchAEMessage.TimeWaitingForCAS)) {
        long timeWaitingForCAS = ((Long) aMessageContext
                .getMessageLongProperty(AsynchAEMessage.TimeWaitingForCAS)).longValue();
        if (timeWaitingForCAS > 0 && aMessageContext.getEndpoint().isRemote()) {
          entry.incrementTimeWaitingForCAS(timeWaitingForCAS);
          delegateServicePerformance.incrementCasPoolWaitTime(timeWaitingForCAS
                  - delegateServicePerformance.getRawCasPoolWaitTime());
          if (inputCasEntry != null) {
            inputCasEntry.incrementTimeWaitingForCAS(timeWaitingForCAS);
          }
        }
      }
      if (aMessageContext.propertyExists(AsynchAEMessage.TimeInProcessCAS)) {
        long timeInProcessCAS = ((Long) aMessageContext
                .getMessageLongProperty(AsynchAEMessage.TimeInProcessCAS)).longValue();
        Endpoint endp = aMessageContext.getEndpoint();
        if (endp != null && endp.isRemote()) {
          if (delegateServicePerformance != null) {
            // calculate the time spent in analysis. The remote service returns total time
            // spent in the analysis. Compute the delta.
            long dt = timeInProcessCAS - delegateServicePerformance.getRawAnalysisTime();
            // increment total time in analysis
            delegateServicePerformance.incrementAnalysisTime(dt);
            getController().getServicePerformance().incrementAnalysisTime(dt);
          }
        } else {
          getController().getServicePerformance().incrementAnalysisTime(timeInProcessCAS);
        }
        casStats.incrementAnalysisTime(timeInProcessCAS);

        if (inputCasReferenceId != null) {
          ServicePerformance inputCasStats = ((AggregateAnalysisEngineController) getController())
                  .getCasStatistics(inputCasReferenceId);
          // Update processing time for this CAS
          if (inputCasStats != null) {
            inputCasStats.incrementAnalysisTime(timeInProcessCAS);
          }
        }

      }
    } catch (AsynchAEException e) {
      throw e;
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  protected void computeStats(MessageContext aMessageContext, String aCasReferenceId)
          throws AsynchAEException {
    if (aMessageContext.propertyExists(AsynchAEMessage.TimeInService)) {
      long departureTime = getController().getTime(aCasReferenceId,
              aMessageContext.getEndpoint().getEndpoint());
      long currentTime = System.nanoTime();
      long roundTrip = currentTime - departureTime;
      long timeInService = aMessageContext.getMessageLongProperty(AsynchAEMessage.TimeInService);
      long totalTimeInComms = currentTime - (departureTime - timeInService);

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "computeStats",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_show_roundtrip_time__FINE",
                new Object[] { aCasReferenceId, aMessageContext.getEndpoint(),
                    (double) roundTrip / (double) 1000000 });

        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "computeStats",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_show_time_spent_in_delegate__FINE",
                new Object[] { aCasReferenceId, (double) timeInService / (double) 1000000,
                    aMessageContext.getEndpoint() });

        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "computeStats",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_show_time_spent_in_comms__FINE",
                new Object[] { aCasReferenceId, (double) totalTimeInComms / (double) 1000000,
                    aMessageContext.getEndpoint() });
      }
    }

    if (getController() instanceof AggregateAnalysisEngineController) {
      aggregateDelegateStats(aMessageContext, aCasReferenceId);
    }
  }

}
