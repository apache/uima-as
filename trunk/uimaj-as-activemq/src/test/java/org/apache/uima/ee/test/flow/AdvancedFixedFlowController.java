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

package org.apache.uima.ee.test.flow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.uima.aae.error.MessageTimeoutException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.flow.CasFlowController_ImplBase;
import org.apache.uima.flow.CasFlow_ImplBase;
import org.apache.uima.flow.FinalStep;
import org.apache.uima.flow.Flow;
import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.ParallelStep;
import org.apache.uima.flow.SimpleStep;
import org.apache.uima.flow.Step;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

/**
 * Simple FlowController that invokes components in a fixed sequence.
 */
public class AdvancedFixedFlowController extends CasFlowController_ImplBase {
  public static final String PARAM_ACTION_AFTER_CAS_MULTIPLIER = "ActionAfterCasMultiplier";

  public static final String PARAM_ALLOW_CONTINUE_ON_FAILURE = "AllowContinueOnFailure";

  public static final String PARAM_ALLOW_DROP_ON_FAILURE = "AllowDropOnFailure";

  public static final String PARAM_FLOW = "Flow";

  private static final int ACTION_CONTINUE = 0;

  private static final int ACTION_STOP = 1;

  private static final int ACTION_DROP = 2;

  private static final int ACTION_DROP_IF_NEW_CAS_PRODUCED = 3;

  public static final String EXCEPTIONS_TO_IGNORE = "ExceptionsToIgnore";

  private ArrayList mSequence;

  private int mActionAfterCasMultiplier;

  private Set mAEsAllowingContinueOnFailure = new HashSet();

  private Set mAEsAllowingDropOnFailure = new HashSet();

  private boolean flowError;

  private String[] exceptionsToIgnore;

  private Logger mLogger;

  private int nCreated;

  public void initialize(FlowControllerContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);
    mLogger = aContext.getLogger();

    String[] flow = (String[]) aContext.getConfigParameterValue(PARAM_FLOW);
    mSequence = new ArrayList();
    for (int i = 0; i < flow.length; i++) {
      String[] aes = flow[i].split(",");
      if (aes.length == 1) {
        mSequence.add(new SimpleStep(aes[0]));
      } else {
        Collection keys = new ArrayList();
        keys.addAll(Arrays.asList(aes));
        mSequence.add(new ParallelStep(keys));
      }
    }

    String actionAfterCasMultiplier = (String) aContext
            .getConfigParameterValue(PARAM_ACTION_AFTER_CAS_MULTIPLIER);
    if ("continue".equalsIgnoreCase(actionAfterCasMultiplier)) {
      mActionAfterCasMultiplier = ACTION_CONTINUE;
    } else if ("stop".equalsIgnoreCase(actionAfterCasMultiplier)) {
      mActionAfterCasMultiplier = ACTION_STOP;
    } else if ("drop".equalsIgnoreCase(actionAfterCasMultiplier)) {
      mActionAfterCasMultiplier = ACTION_DROP;
    } else if ("dropIfNewCasProduced".equalsIgnoreCase(actionAfterCasMultiplier)) {
      mActionAfterCasMultiplier = ACTION_DROP_IF_NEW_CAS_PRODUCED;
    } else if (actionAfterCasMultiplier == null) {
      mActionAfterCasMultiplier = ACTION_DROP_IF_NEW_CAS_PRODUCED; // default
    } else {
      throw new ResourceInitializationException(); // TODO
    }

    exceptionsToIgnore = (String[]) aContext.getConfigParameterValue(EXCEPTIONS_TO_IGNORE);

    // Some delegates allow the CAS to continue in the flow after an error
    String[] aeKeysAllowingContinue = (String[]) aContext
            .getConfigParameterValue(PARAM_ALLOW_CONTINUE_ON_FAILURE);
    if (aeKeysAllowingContinue != null) {
      mAEsAllowingContinueOnFailure.addAll(Arrays.asList(aeKeysAllowingContinue));
    }

    // Some delegates want the CAS quietly dropped after an error
    String[] aeKeysAllowingDrop = (String[]) aContext
            .getConfigParameterValue(PARAM_ALLOW_DROP_ON_FAILURE);
    if (aeKeysAllowingDrop != null) {
      mAEsAllowingDropOnFailure.addAll(Arrays.asList(aeKeysAllowingDrop));
    }

    flowError = false;
    nCreated = 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.flow.CasFlowController_ImplBase#computeFlow(org.apache.uima.cas.CAS)
   */
  public Flow computeFlow(CAS aCAS) throws AnalysisEngineProcessException {
    ++nCreated;
    return new FixedFlowObject(0, false, Integer.toString(nCreated));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.flow.FlowController_ImplBase#addAnalysisEngines(java.util.Collection)
   */
  public void addAnalysisEngines(Collection aKeys) {
    // Append new keys as a ParallelStep at end of Sequence
    mSequence.add(new ParallelStep(new ArrayList(aKeys)));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.flow.FlowController_ImplBase#removeAnalysisEngines(java.util.Collection)
   */
  public void removeAnalysisEngines(Collection aKeys) throws AnalysisEngineProcessException {
    // Remove keys from Sequence
    for (int i = 0; i < mSequence.size(); ++i) {
      Step step = (Step) mSequence.get(i);
      if (step instanceof SimpleStep && aKeys.contains(((SimpleStep) step).getAnalysisEngineKey())) {
        mSequence.set(i, null);
      } else if (step instanceof ParallelStep) {
        Collection keys = new ArrayList(((ParallelStep) step).getAnalysisEngineKeys());
        keys.removeAll(aKeys);
        if (keys.isEmpty()) {
          mSequence.set(i, null);
        } else {
          mSequence.set(i, new ParallelStep(keys));
        }
      }
    }
  }

  class FixedFlowObject extends CasFlow_ImplBase {
    private int currentStep;

    private boolean wasPassedToCasMultiplier = false;

    private boolean casMultiplierProducedNewCas = false;

    private boolean internallyCreatedCas = false;

    private boolean dropOnFailure = false;

    private String flowId;

    private int nCreated;

    /**
     * Create a new fixed flow starting at step <code>startStep</code> of the fixed sequence.
     * 
     * @param startStep
     *          index of mSequence to start at
     * @param internallyCreatedCas
     *          true to indicate that this Flow object is for a CAS that was produced by a
     *          CasMultiplier within this aggregate. Such CASes area allowed to be dropped and not
     *          output from the aggregate.
     * @param id
     *          id of CAS used in logging messages
     * 
     */
    public FixedFlowObject(int startStep, boolean internallyCreatedCas, String id) {
      if (internallyCreatedCas) {
        mLogger.log(Level.FINE, "CAS " + id + " created by "
                + ((SimpleStep) mSequence.get(startStep - 1)).getAnalysisEngineKey());
      } else {
        mLogger.log(Level.FINE, "CAS " + id + " at start of flow");
      }
      flowId = id;
      nCreated = 0;
      currentStep = startStep;
      this.internallyCreatedCas = internallyCreatedCas;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.Flow#next()
     */
    public Step next() throws AnalysisEngineProcessException {
      // Terminate flow if continueOnFailure was unhappy
      if (flowError) {
        throw new RuntimeException("Flow error found in continueOnFailure");
      }

      if (dropOnFailure) {
        mLogger
                .log(Level.FINE, "CAS " + flowId
                        + " skips rest of flow after failing previous step");
        return new FinalStep(internallyCreatedCas);
      }

      // if CAS was passed to a CAS multiplier on the last step, special processing
      // is needed according to the value of the ActionAfterCasMultiplier config parameter
      if (wasPassedToCasMultiplier) {
        switch (mActionAfterCasMultiplier) {
          case ACTION_STOP:
            mLogger.log(Level.FINE, "CAS " + flowId + " stops after CasMultiplier ");
            return new FinalStep();
          case ACTION_DROP:
            mLogger.log(Level.FINE, "CAS " + flowId + " discarded after CasMultiplier");
            return new FinalStep(internallyCreatedCas);
          case ACTION_DROP_IF_NEW_CAS_PRODUCED:
            if (casMultiplierProducedNewCas) {
              mLogger.log(Level.FINE, "CAS " + flowId
                      + " discarded after CasMultiplier (as new CAS produced)");
              return new FinalStep(internallyCreatedCas);
            }
            // else, continue with flow
            break;
          // if action is ACTION_CONTINUE, just continue with flow
        }
        wasPassedToCasMultiplier = false;
        casMultiplierProducedNewCas = false;
      }

      // Get next in sequence, skipping any disabled ones
      Step nextStep;
      do {
        if (currentStep >= mSequence.size()) {
          mLogger.log(Level.FINE, "CAS " + flowId + " at end of flow");
          return new FinalStep(); // this CAS has finished the sequence
        }
        nextStep = (Step) mSequence.get(currentStep++);
        if (nextStep == null) {
          mLogger.log(Level.FINE, "CAS " + flowId + " skipping disabled step " + currentStep);
        }
      } while (nextStep == null);

      // if next step is a CasMultiplier, set wasPassedToCasMultiplier to true for next time
      if (stepContainsCasMultiplier(nextStep))
        wasPassedToCasMultiplier = true;

      String stepName = "?";
      if (nextStep instanceof SimpleStep) {
        stepName = ((SimpleStep) nextStep).getAnalysisEngineKey();
        mLogger.log(Level.FINE, "CAS " + flowId + " sent to " + stepName);
      } else if (nextStep instanceof FinalStep) {
        stepName = "Final-" + currentStep;
        mLogger.log(Level.FINE, "CAS " + flowId + " at final step - will"
                + (((FinalStep) nextStep).getForceCasToBeDropped() ? "" : " not") + " be dropped");
      } else if (nextStep instanceof ParallelStep) {
        ((ParallelStep) nextStep).getAnalysisEngineKeys().toArray(new String[0]);
        stepName = ((ParallelStep) nextStep).getAnalysisEngineKeys().toString();
        mLogger.log(Level.FINE, "CAS " + flowId + " sent in parallel to: " + stepName);
      }

      // now send the CAS to the next AE(s) in sequence.
      return nextStep;
    }

    /**
     * @param nextStep
     * @return
     */
    private boolean stepContainsCasMultiplier(Step nextStep) {
      if (nextStep instanceof SimpleStep) {
        AnalysisEngineMetaData md = (AnalysisEngineMetaData) getContext()
                .getAnalysisEngineMetaDataMap().get(((SimpleStep) nextStep).getAnalysisEngineKey());
        return md != null && md.getOperationalProperties() != null
                && md.getOperationalProperties().getOutputsNewCASes();
      } else if (nextStep instanceof ParallelStep) {
        Iterator iter = ((ParallelStep) nextStep).getAnalysisEngineKeys().iterator();
        while (iter.hasNext()) {
          String key = (String) iter.next();
          AnalysisEngineMetaData md = (AnalysisEngineMetaData) getContext()
                  .getAnalysisEngineMetaDataMap().get(key);
          if (md != null && md.getOperationalProperties() != null
                  && md.getOperationalProperties().getOutputsNewCASes())
            return true;
        }
        return false;
      } else
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.CasFlow_ImplBase#newCasProduced(CAS, String)
     */
    public Flow newCasProduced(CAS newCas, String producedBy) throws AnalysisEngineProcessException {
      // record that the input CAS has been segmented (affects its subsequent flow)
      casMultiplierProducedNewCas = true;
      // start the new output CAS from the next node after the CasMultiplier that produced it
      int i = 0;
      while (!stepContains((Step) mSequence.get(i), producedBy))
        i++;
      ++nCreated;
      return new FixedFlowObject(i + 1, true, flowId + "." + nCreated);
    }

    /**
     * @param object
     * @param producedBy
     * @return
     */
    private boolean stepContains(Step step, String producedBy) {
      if (step instanceof SimpleStep) {
        return ((SimpleStep) step).getAnalysisEngineKey().equals(producedBy);
      } else if (step instanceof ParallelStep) {
        Iterator iter = ((ParallelStep) step).getAnalysisEngineKeys().iterator();
        while (iter.hasNext()) {
          String key = (String) iter.next();
          if (key.equals(producedBy))
            return true;
        }
        return false;
      } else
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.CasFlow_ImplBase#continueOnFailure(java.lang.String,
     * java.lang.Exception)
     */
    public boolean continueOnFailure(String failedAeKey, Exception failure) {
      // Check that root cause is expected
      Throwable cause = failure;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      mLogger.log(Level.WARNING, failedAeKey + " ERROR = " + cause.toString());
      // Usually expect IndexOutOfBoundsException or MessageTimeoutException
      // (forced when a delegate with outstanding CASes is disabled) but these
      // must now be explicitly specified in the descriptor.
      if (!expectedException(cause)) {
        System.out
                .println("FlowController.continueOnFailure - Unexpected cause for delegate failure: "
                        + cause.getClass());
        // Throwing an exception here doesn't stop flow!
        // Will be thrown in "next" method so should let CAS continue !?
        flowError = true;
        return false;
      }
      Boolean carryOn = mAEsAllowingContinueOnFailure.contains(failedAeKey);
      dropOnFailure = internallyCreatedCas && mAEsAllowingDropOnFailure.contains(failedAeKey);
      if (dropOnFailure) {
        mLogger.log(Level.FINE, "CAS " + flowId + " will be quietly dropped after failure of "
                + failedAeKey);
      } else {
        mLogger.log(Level.FINE, "CAS " + flowId + " CAN" + (carryOn ? "" : "NOT")
                + " continue in flow after failure of " + failedAeKey);
      }
      return carryOn || dropOnFailure;
    }

    private boolean expectedException(Throwable failure) {
      if (exceptionsToIgnore != null) {
        for (String exception : exceptionsToIgnore) {
          if (failure.getClass().getName().equals(exception)) {
            return true;
          }
        }
      }
      return false;
    }
  }
}
