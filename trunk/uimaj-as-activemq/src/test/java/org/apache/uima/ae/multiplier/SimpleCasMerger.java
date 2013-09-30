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

package org.apache.uima.ae.multiplier;

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

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasMultiplier_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.AbstractCas;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;

/**
 * An example CasMultiplier, which simulates merging of the input CASes. Generates 1 output CAS for
 * every N input CASes.
 */
public class SimpleCasMerger extends CasMultiplier_ImplBase {
  private int docCount = 0;

  private int genCount = 0;

  private int nToMerge;

  private String casMultName;

  /*
   * (non-Javadoc)
   * 
   * @seeorg.apache.uima.analysis_component.AnalysisComponent_ImplBase#initialize(org.apache.uima.
   * UimaContext)
   */
  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);
    this.nToMerge = ((Integer) aContext.getConfigParameterValue("NumberToMerge")).intValue();
    this.casMultName = (String) aContext.getConfigParameterValue("AnnotatorName");
    if (this.casMultName == null) {
      this.casMultName = "CasMerger";
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see JCasMultiplier_ImplBase#process(JCas)
   */
  public void process(CAS aCas) throws AnalysisEngineProcessException {
    this.docCount++;
    if (UIMAFramework.getLogger().isLoggable(Level.FINE))
      System.out.println(casMultName + ".process() received document " + this.docCount);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.analysis_component.AnalysisComponent#hasNext()
   */
  public boolean hasNext() throws AnalysisEngineProcessException {
    // Generate N-th when receive M * N-th input
    return (docCount >= (genCount + 1) * nToMerge);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.analysis_component.AnalysisComponent#next()
   */
  public AbstractCas next() throws AnalysisEngineProcessException {
    CAS cas = getEmptyCAS();

    this.genCount++;
    String text = casMultName + " created #" + this.genCount + " from #" + this.docCount;
    if (UIMAFramework.getLogger().isLoggable(Level.FINE))
      System.out.println(text);
    cas.setDocumentText(text);

    return cas;
  }

}
