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

/**
 * This FlowController tests robustness of the uima ee error handling. !!!! It is NOT meant to be
 * used for processing!!!! It throws a ResourceInitializationException in the initialize() method
 */
public class AdvancedFixedFlowControllerWithInitiFailure extends CasFlowController_ImplBase {
  public static final String PARAM_ACTION_AFTER_CAS_MULTIPLIER = "ActionAfterCasMultiplier";

  public static final String PARAM_ALLOW_CONTINUE_ON_FAILURE = "AllowContinueOnFailure";

  public static final String PARAM_FLOW = "Flow";

  private static final int ACTION_CONTINUE = 0;

  private static final int ACTION_STOP = 1;

  private static final int ACTION_DROP = 2;

  private static final int ACTION_DROP_IF_NEW_CAS_PRODUCED = 3;

  private ArrayList mSequence;

  private int mActionAfterCasMultiplier;

  private Set mAEsAllowingContinueOnFailure = new HashSet();

  private boolean flowError;

  public void initialize(FlowControllerContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);

    throw new ResourceInitializationException();

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.flow.CasFlowController_ImplBase#computeFlow(org.apache.uima.cas.CAS)
   */
  public Flow computeFlow(CAS aCAS) throws AnalysisEngineProcessException {
    return new FixedFlowObject(0);
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
    int i = 0;
    while (i < mSequence.size()) {
      Step step = (Step) mSequence.get(i);
      if (step instanceof SimpleStep && aKeys.contains(((SimpleStep) step).getAnalysisEngineKey())) {
        mSequence.remove(i);
      } else if (step instanceof ParallelStep) {
        Collection keys = new ArrayList(((ParallelStep) step).getAnalysisEngineKeys());
        keys.removeAll(aKeys);
        if (keys.isEmpty()) {
          mSequence.remove(i);
        } else {
          mSequence.set(i++, new ParallelStep(keys));
        }
      } else
        i++;
    }
  }

  class FixedFlowObject extends CasFlow_ImplBase {
    private int currentStep;

    private boolean wasPassedToCasMultiplier = false;

    private boolean casMultiplierProducedNewCas = false;

    private boolean internallyCreatedCas = false;

    /**
     * Create a new fixed flow starting at step <code>startStep</code> of the fixed sequence.
     * 
     * @param startStep
     *          index of mSequence to start at
     */
    public FixedFlowObject(int startStep) {
      this(startStep, false);
    }

    /**
     * Create a new fixed flow starting at step <code>startStep</code> of the fixed sequence.
     * 
     * @param startStep
     *          index of mSequence to start at
     * @param internallyCreatedCas
     *          true to indicate that this Flow object is for a CAS that was produced by a
     *          CasMultiplier within this aggregate. Such CASes area allowed to be dropped and not
     *          output from the aggregate.
     * 
     */
    public FixedFlowObject(int startStep, boolean internallyCreatedCas) {
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

      // if CAS was passed to a CAS multiplier on the last step, special processing
      // is needed according to the value of the ActionAfterCasMultiplier config parameter
      if (wasPassedToCasMultiplier) {
        switch (mActionAfterCasMultiplier) {
          case ACTION_STOP:
            return new FinalStep();
          case ACTION_DROP:
            return new FinalStep(internallyCreatedCas);
          case ACTION_DROP_IF_NEW_CAS_PRODUCED:
            if (casMultiplierProducedNewCas) {
              return new FinalStep(internallyCreatedCas);
            }
            // else, continue with flow
            break;
          // if action is ACTION_CONTINUE, just continue with flow
        }
        wasPassedToCasMultiplier = false;
        casMultiplierProducedNewCas = false;
      }

      if (currentStep >= mSequence.size()) {
        return new FinalStep(); // this CAS has finished the sequence
      }

      // if next step is a CasMultiplier, set wasPassedToCasMultiplier to true for next time
      Step nextStep = (Step) mSequence.get(currentStep++);
      if (stepContainsCasMultiplier(nextStep))
        wasPassedToCasMultiplier = true;

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
      return new FixedFlowObject(i + 1, true);
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
      // Check that root cause is an IndexOutOfBounds exception
      Throwable cause = failure.getCause();
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      if (cause.getClass() != IndexOutOfBoundsException.class) {
        System.out
                .println("FlowController.continueOnFailure - Invalid cause for delegate failure - expected "
                        + IndexOutOfBoundsException.class + " - received " + cause.getClass());
        // Throwing an exception here doesn't stop flow!
        flowError = true;
      }
      return mAEsAllowingContinueOnFailure.contains(failedAeKey);
    }
  }
}
