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

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.text.AnnotationIndex;
import org.apache.uima.examples.SourceDocumentInformation;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

/**
 * Checks that the sofa data of every N-th CAS starts with the correct value. Validates output of a
 * CasMultiplier that merges its input CASes
 */

public class CheckTextAnnotator extends CasAnnotator_ImplBase {
  String name = "CheckText";

  int counter;

  int checkInterval = 0;

  int finalCount = 0;

  String textPrefix;

  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);
    counter = 0;

    textPrefix = (String) getContext().getConfigParameterValue("TextPrefix");
    checkInterval = ((Integer) getContext().getConfigParameterValue("CheckInterval")).intValue();
    if (getContext().getConfigParameterValue("FinalCount") != null) {
      finalCount = ((Integer) getContext().getConfigParameterValue("FinalCount")).intValue();
    }

    // write log messages
    Logger logger = getContext().getLogger();
    logger.log(Level.CONFIG, name + " initialized");
  }

  public void typeSystemInit(TypeSystem aTypeSystem) throws AnalysisEngineProcessException {
  }

  public void collectionProcessComplete() throws AnalysisEngineProcessException {
    System.out.println(name
            + ".collectionProcessComplete() Called -------------------------------------");
    if (finalCount > 0 && finalCount != counter) {
      String msg = name + " expected " + finalCount + " CASes but was given " + counter;
      System.out.println(msg);
      throw new AnalysisEngineProcessException(new Exception(msg));
    }
    if (UIMAFramework.getLogger().isLoggable(Level.INFO)) {
      System.out.println(name + " processed " + counter + " CASes");
    }
    counter = 0;
  }

  public void process(CAS aCAS) throws AnalysisEngineProcessException {
    ++counter;
    String line = aCAS.getDocumentText();
    if (UIMAFramework.getLogger().isLoggable(Level.FINE)) {
      if (line.length() > 60)
        line = line.substring(0, 30) + " ...";
      System.out.println(name + ".process() called for the " + counter + "th time: \"" + line
              + "\"");
    }
    if (checkInterval > 0 && counter % checkInterval == 0) {
      if (!line.startsWith(textPrefix)) {
        String msg = name + " expected " + counter + "-th CAS to start with: " + textPrefix;
        System.out.println(msg);
        throw new AnalysisEngineProcessException(new Exception(msg));
      }
    }

    // If not checking the document text check if a SourceDocumentInformation annotation has been
    // added
    if (checkInterval == 0) {
      JCas jcas;
      try {
        jcas = aCAS.getJCas();
      } catch (CASException e) {
        throw new AnalysisEngineProcessException(e);
      }
      AnnotationIndex aIndx = jcas.getAnnotationIndex(SourceDocumentInformation.type);
      FSIterator aIter = aIndx.iterator();
      if (!aIter.isValid()) {
        String msg = name + " didn't find a SourceDocumentInformation annotation";
        System.out.println(msg);
        throw new AnalysisEngineProcessException(new Exception(msg));
      }
      System.out.println(name + " found a SourceDocumentInformation");
    }
  }

}
