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

package org.apache.uima.aae.deploymentDescriptor;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.UimacppServiceController;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.impl.Import_impl;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.Level;

public class XsltImportByName {
  private static final Class CLASS_NAME = XsltImportByName.class;

  public static String resolveByName(String input) {
    ResourceManager resourceManager = UIMAFramework.newDefaultResourceManager();
    Import theImport = new Import_impl();
    theImport.setName(input);
    try {
      return theImport.findAbsoluteUrl(resourceManager).toExternalForm();
    } catch (InvalidXMLException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, XsltImportByName.class.getName(),
                "resolveByName", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
      return "ERROR converting import by name to absolute path";
    }
  }

}
