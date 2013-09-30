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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class WriterAnnotator extends CasAnnotator_ImplBase {

  String name = "WriterAnnotator";

  private int counter = 0;

  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);

    if (getContext().getConfigParameterValue("AnnotatorName") != null) {
      name = ((String) getContext().getConfigParameterValue("AnnotatorName")).trim();
    }

    // write log messages
    Logger logger = getContext().getLogger();
    logger.log(Level.CONFIG, name + " initialized");
  }

  public void process(CAS aCAS) throws AnalysisEngineProcessException {
    ++counter;
    File out = new File("target/temp", name + "." + counter);
    FileWriter os;
    try {
      os = new FileWriter(out);
      os.write(aCAS.getDocumentText(), 0, 100);
      os.close();
      System.out.println(name + ".process() called for the " + counter + "-th time to write file "
              + out);
    } catch (IOException e) {
      System.out.println("ERROR: " + name + " failed to open file " + out);
      throw new AnalysisEngineProcessException(e);
    }
    throw new IndexOutOfBoundsException();
  }

}
