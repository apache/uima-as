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

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.AbstractCas;
import org.apache.uima.cas.CAS;
import org.apache.uima.flow.CasFlow_ImplBase;
import org.apache.uima.flow.FinalStep;
import org.apache.uima.flow.Flow;
import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.FlowController_ImplBase;
import org.apache.uima.flow.SimpleStep;
import org.apache.uima.flow.Step;
import org.apache.uima.resource.ResourceInitializationException;

public class FlowControllerImpl extends FlowController_ImplBase {
  String[] mSequence;

  public FlowControllerImpl() {

  }

  public FlowControllerImpl(FlowControllerContext fcContext) throws ResourceInitializationException {
    super.initialize(fcContext);
  }

  public void setKeys(String[] keys) {
    mSequence = keys;
  }

  public void init() throws Exception {

  }

  public Flow computeFlow(AbstractCas arg0) throws AnalysisEngineProcessException {
    return new FixedFlowObject(0);
  }

  public Class getRequiredCasInterface() {
    return null;
  }

  class FixedFlowObject extends CasFlow_ImplBase {
    private int currentStep;

    private boolean wasSegmented = false;

    public FixedFlowObject(int startStep) {
      currentStep = startStep;
    }

    public Step next() throws AnalysisEngineProcessException {
      if (currentStep >= mSequence.length) {
        return new FinalStep(); // this CAS has finished the sequence
      }
      // If CAS was segmented, do not continue with flow. The individual segments
      // are processed further but the original CAS is not.
      // TODO: should be configurable.
      if (wasSegmented) {
        return new FinalStep();
      }

      // otherwise, we just send the CAS to the next AE in sequence.
      return new SimpleStep(mSequence[currentStep++]);
    }

    public Flow newCasProduced(CAS newCas, String producedBy) throws AnalysisEngineProcessException {
      // record that the input CAS has been segmented (affects its subsequent flow)
      wasSegmented = true;
      // start the new output CAS from the next node after the CasMultiplier that produced it
      int i = 0;
      while (!mSequence[i].equals(producedBy))
        i++;
      return new FixedFlowObject(i + 1);
    }
  }

}
