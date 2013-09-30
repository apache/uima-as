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

package org.apache.uima.dde.internal.wizards;

import java.text.MessageFormat;

import org.apache.uima.taeconfigurator.wizards.AbstractNewWizard;

public class DDENewWizard extends AbstractNewWizard  {

  static public final String EDITOR_ID="com.ibm.apache.uima.dde.internal.MultiPageEditor";
  static protected String editorId = EDITOR_ID;
  
  public DDENewWizard() {
    super("New Analysis Engine Descriptor File");
  }

  public void addPages() {
    page = new DDENewWizardPage(selection);
    addPage(page);
  }

  public String getPrototypeDescriptor(String name) {
    // UIMA-3308
    return MessageFormat.format(COMMON_PARTIAL_DESCRIPTOR, 
        name,
        
        "    <deployment protocol=\"jms\" provider=\"activemq\">\n"
      + "        <casPool numberOfCASes=\"1\"/>\n" // since Top AE is a primitive (not Async)
      + "        <service>\n"
      + "            <inputQueue endpoint=\"myQueueName\" brokerURL=\"${defaultBrokerURL}\"/>\n"
      + "        </service>\n"
      + "    </deployment>\n",
      
        "analysisEngineDeploymentDescription");
  }

}