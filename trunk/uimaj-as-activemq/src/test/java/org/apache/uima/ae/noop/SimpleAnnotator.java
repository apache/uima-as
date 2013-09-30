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

package org.apache.uima.ae.noop;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.examples.SourceDocumentInformation;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class SimpleAnnotator extends CasAnnotator_ImplBase {
  private long counter = 0;

  int processDelay = 0;

  int finalCount = 0;

  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);

    if (getContext().getConfigParameterValue("ProcessDelay") != null) {
      processDelay = ((Integer) getContext().getConfigParameterValue("ProcessDelay")).intValue();
      System.out.println("SimpleAnnotator.initialize() Initializing With Process Delay of "
              + processDelay + " millis");
    }

    if (getContext().getConfigParameterValue("FinalCount") != null) {
      finalCount = ((Integer) getContext().getConfigParameterValue("FinalCount")).intValue();
    }

    // write log messages
    Logger logger = getContext().getLogger();
    logger.log(Level.CONFIG, "SimpleAnnotator initialized");
  }

  public void typeSystemInit(TypeSystem aTypeSystem) throws AnalysisEngineProcessException {
  }

  public void collectionProcessComplete() throws AnalysisEngineProcessException {
    System.out
            .println("SimpleAnnotator.collectionProcessComplete() Called -------------------------------------");
    if (finalCount > 0 && finalCount != counter) {
      String msg = "SimpleAnnotator expected " + finalCount + " CASes but was given " + counter;
      System.out.println(msg);
      throw new AnalysisEngineProcessException(new Exception(msg));
    }
    counter = 0;
  }

  public void process(CAS aCAS) throws AnalysisEngineProcessException {
    ++counter;
    if (processDelay == 0) {
      if (UIMAFramework.getLogger().isLoggable(Level.FINE))
        System.out.println("SimpleAnnotator.process() called for the " + counter
                + "th time. Hashcode:" + hashCode());
    } else {
      if (UIMAFramework.getLogger().isLoggable(Level.FINE))
        System.out.println("SimpleAnnotator.process() called for the " + counter
                + "th time, delaying Response For:" + processDelay + " millis");
      synchronized (this) {
        try {
          wait(processDelay);
        } catch (InterruptedException e) {
        }
      }
    }
    JCas jcas;
    try {
      jcas = aCAS.getJCas();
    } catch (CASException e) {
      throw new AnalysisEngineProcessException(e);
    }
    SourceDocumentInformation sda = new SourceDocumentInformation(jcas, 0, jcas.getDocumentText()
            .length());
    sda.setOffsetInSource(0);
    sda.addToIndexes();
    if (UIMAFramework.getLogger().isLoggable(Level.FINE))
      System.out.println("SimpleAnnotator.process() added a SourceDocumentInformation annotation");
  }

}
