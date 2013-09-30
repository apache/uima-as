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

package org.apache.uima.aae;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContextAdmin;
import org.apache.uima.aae.jmx.JmxManagement;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.asb.impl.FlowControllerContainer;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.analysis_engine.metadata.FlowControllerDeclaration;
import org.apache.uima.analysis_engine.metadata.SofaMapping;
import org.apache.uima.cas.CAS;
import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.FlowControllerDescription;
import org.apache.uima.flow.impl.FlowControllerContext_impl;
import org.apache.uima.resource.Resource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.impl.Import_impl;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;

public class UimaClassFactory {
  public static final String IMPORT_BY_NAME_PREFIX = "*importByName:";

  /**
   * Creates a resource specifier from a given file
   * 
   * @param aFileResource
   *          - resource filename
   * @return ResourceSpecifier - new instance
   * 
   * @throws InvalidXMLException
   * @throws ResourceInitializationException
   * @throws IOException
   */
  public static ResourceSpecifier produceResourceSpecifier(String aFileResource)
          throws InvalidXMLException, ResourceInitializationException, IOException {
    XMLInputSource input = resolveImportByName(aFileResource, UIMAFramework
            .newDefaultResourceManager());
    return UIMAFramework.getXMLParser().parseResourceSpecifier(input);
  }

  /**
   * Creates and returns a new default Resource Manager
   * 
   * @return ResourceManager - new instance of a default ResourceManager
   */
  public static ResourceManager produceResourceManager() {
    return UIMAFramework.newDefaultResourceManager();
  }

  private static XMLInputSource resolveImportByName(String aFileResource,
          ResourceManager aResourceManager) throws InvalidXMLException,
          ResourceInitializationException, IOException {
    XMLInputSource input;

    if (aFileResource.startsWith(IMPORT_BY_NAME_PREFIX)) {
      Import theImport = new Import_impl();
      theImport.setName(aFileResource.substring(IMPORT_BY_NAME_PREFIX.length()));
      input = new XMLInputSource(theImport.findAbsoluteUrl(aResourceManager));
    } else {
      input = new XMLInputSource(aFileResource);
    }
    return input;

  }

  /**
   * Produces and initializes new FlowController
   * 
   * @param aeSpecifier
   *          -
   * @param aFlowControllerDescriptor
   * @param anAggregateMergedTypeSystem
   *          - Merged type system from all delegates
   * @param aParentContext
   *          - reference to parent context
   * @param aSofaMappings
   *          -
   * @param aJmxManagementInterface
   * @return
   * @throws InvalidXMLException
   * @throws ResourceInitializationException
   * @throws IOException
   */
  public static FlowControllerContainer produceAggregateFlowControllerContainer(
          AnalysisEngineDescription aeSpecifier, String aFlowControllerDescriptor,
          Map anAggregateMergedTypeSystem, UimaContextAdmin aParentContext,
          SofaMapping[] aSofaMappings, JmxManagement aJmxManagementInterface)
          throws InvalidXMLException, ResourceInitializationException, IOException {
    {

      FlowControllerDeclaration fcd = aeSpecifier.getFlowControllerDeclaration();
      String key = "_FlowController"; // default
      if (fcd != null && fcd.getKey() != null && fcd.getKey().trim().length() > 0) {
        key = fcd.getKey();
      }

      ResourceManager resourceManager = aParentContext.getRootContext().getResourceManager();
      XMLInputSource input = resolveImportByName(aFlowControllerDescriptor, resourceManager);

      FlowControllerDescription specifier = (FlowControllerDescription) UIMAFramework
              .getXMLParser().parseResourceSpecifier(input);
      AnalysisEngineMetaData anAggregateMetadata = aeSpecifier.getAnalysisEngineMetaData();

      Map sofamap = new TreeMap();
      if (aSofaMappings != null && aSofaMappings.length > 0) {
        for (int s = 0; s < aSofaMappings.length; s++) {
          // the mapping is for this analysis engine
          if (aSofaMappings[s].getComponentKey().equals(key)) {
            // if component sofa name is null, replace it with the default for TCAS sofa name
            // This is to support old style TCAS
            if (aSofaMappings[s].getComponentSofaName() == null)
              aSofaMappings[s].setComponentSofaName(CAS.NAME_DEFAULT_SOFA);
            sofamap.put(aSofaMappings[s].getComponentSofaName(), aSofaMappings[s]
                    .getAggregateSofaName());
          }
        }
      }
      Map flowControllerParams = new HashMap();
      FlowControllerContext fctx = new FlowControllerContext_impl(aParentContext, key, sofamap,
              anAggregateMergedTypeSystem, anAggregateMetadata);

      flowControllerParams.put(Resource.PARAM_UIMA_CONTEXT, fctx);
      flowControllerParams.put(Resource.PARAM_RESOURCE_MANAGER, resourceManager);
      flowControllerParams.put(AnalysisEngine.PARAM_MBEAN_NAME_PREFIX, aJmxManagementInterface
              .getJmxDomain());
      if (aJmxManagementInterface.getMBeanServer() != null) {
        flowControllerParams.put(AnalysisEngine.PARAM_MBEAN_SERVER, aJmxManagementInterface
                .getMBeanServer());
      }
      FlowControllerContainer flowControllerContainer = new FlowControllerContainer();
      flowControllerContainer.initialize(specifier, flowControllerParams);
      return flowControllerContainer;
    }
  }

}
