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

package org.apache.uima.tools.internal.uima.util;

import org.apache.uima.ResourceSpecifierFactory;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.aae.deployment.impl.AsyncAggregateErrorConfiguration_Impl;
import org.apache.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration_Impl;
import org.apache.uima.aae.deployment.impl.DeploymentMetaData_Impl;
import org.apache.uima.aae.deployment.impl.GetMetadataErrors_Impl;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.OperationalProperties;
import org.apache.uima.util.InvalidXMLException;


public class AETreeBuilder {
  
  static public Import createImport(String descriptor, boolean byLocation) {
    Import imp = (Import) UIMAFramework.getResourceSpecifierFactory().createObject(Import.class);
    if (byLocation) {
      imp.setLocation(descriptor);
    } else {
      imp.setName(descriptor);
    }

    return imp;
  }

  /**
   * Create AEDeploymentMetaData from RemoteAEDeploymentMetaData
   * 
   * @param remoteMetaData
   * @return
   * @return AEDeploymentMetaData
   */
  static public AEDeploymentMetaData createAEDeploymentMetaData(
          RemoteAEDeploymentMetaData remoteMetaData) {
    ResourceSpecifierFactory factory = UIMAFramework.getResourceSpecifierFactory();
    AEDeploymentMetaData metaData = (AEDeploymentMetaData) factory
            .createObject(AEDeploymentMetaData.class);
    metaData.setParent(remoteMetaData.getParent());
    metaData.setKey(remoteMetaData.getKey());
    try {
      metaData.setResourceSpecifier(remoteMetaData.getResourceSpecifier(), null, false);
    } catch (InvalidXMLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // Need to call AFTER metaData.setResourceSpecifier
    metaData.setCasMultiplierPoolSize(remoteMetaData.getCasMultiplierPoolSize());

      // Clone AsyncAggregateErrorConfiguration
      metaData.setAsyncAEErrorConfiguration((AsyncAEErrorConfiguration) remoteMetaData
              .getAsyncAEErrorConfiguration().clone());
      // Set TimeOut for Delegate
      metaData.getAsyncAEErrorConfiguration().getGetMetadataErrors().setTimeout(AEDeploymentConstants.DEFAULT_GETMETADATA_NO_TIMEOUT);

    return metaData;
  }

  /**
   * Create RemoteAEDeploymentMetaData from AEDeploymentMetaData
   * 
   * @param metaData
   * @return
   * @return RemoteAEDeploymentMetaData
   */
  static public RemoteAEDeploymentMetaData createRemoteAEDeploymentMetaData(
          AEDeploymentMetaData metaData) {
    ResourceSpecifierFactory factory = UIMAFramework.getResourceSpecifierFactory();
    RemoteAEDeploymentMetaData remoteMetaData = (RemoteAEDeploymentMetaData) factory
            .createObject(RemoteAEDeploymentMetaData.class);
    remoteMetaData.setParent(metaData.getParent());
    remoteMetaData.setKey(metaData.getKey());
    remoteMetaData.setCasMultiplierPoolSize(metaData.getCasMultiplierPoolSize());
    remoteMetaData.setRemoteReplyQueueScaleout(-1);
    try {
      remoteMetaData.setResourceSpecifier(metaData.getResourceSpecifier(), null, false);
    } catch (InvalidXMLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // Clone OR Create a new AsyncAggregateErrorConfiguration
    AsyncAEErrorConfiguration aggErrorConfig = null;
    AsyncAEErrorConfiguration errorConfig = metaData.getAsyncAEErrorConfiguration();
    if (errorConfig instanceof AsyncAggregateErrorConfiguration_Impl) {
      // Clone
      aggErrorConfig = errorConfig.clone();
    } else {
      // Create a new AsyncAggregateErrorConfiguration
      aggErrorConfig = new AsyncAggregateErrorConfiguration_Impl();
      aggErrorConfig.setGetMetadataErrors(new GetMetadataErrors_Impl(errorConfig));
      aggErrorConfig.setProcessCasErrors(errorConfig.getProcessCasErrors().clone(aggErrorConfig));
      aggErrorConfig.setCollectionProcessCompleteErrors(errorConfig
              .getCollectionProcessCompleteErrors().clone(aggErrorConfig));
    }
    remoteMetaData.setErrorConfiguration(aggErrorConfig);
    errorConfig.sParentObject((DeploymentMetaData_Impl)remoteMetaData);
    // Set TimeOut for Remote
    aggErrorConfig.getGetMetadataErrors().setTimeout(AEDeploymentConstants.DEFAULT_GETMETADATA_TIMEOUT);
    return remoteMetaData;
  }

//  static public DDObject createDeploymentTree(AEDeploymentDescription dd) {
//    try {
//      ResourceSpecifier root = dd.getAeService().getTopAnalysisEngineDescription();
//    } catch (InvalidXMLException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//
//    return null;
//  }

/*  static public DDObject createDeploymentTree(String key, AnalysisEngineDescription root) {
    if (root == null) {
      return null;
    }

    if (root.isPrimitive()) {
      return new DDObject(new DDModel(key, root));
    }

    DDParent tree = new DDParent(new DDModel(key, root));
    try {
      Map map = root.getDelegateAnalysisEngineSpecifiers();
      for (Object obj : map.entrySet()) {
        Map.Entry entry = (Map.Entry) obj;
        // Trace.err("key: " + entry.getKey() + " ; " + entry.getValue().getClass().getName());
        DDObject node = buildAnalysisEngineTree((String) entry.getKey(), (ResourceSpecifier) entry
                .getValue());
        tree.add(node);
      }
    } catch (InvalidXMLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return tree;
  }

  static public DDObject buildAnalysisEngineTree(String key, ResourceSpecifier root) {
    DDObject rootNode = null;

    if (root instanceof AnalysisEngineDescription) {
      AnalysisEngineDescription ae = (AnalysisEngineDescription) root;
      if (!ae.isPrimitive()) {
        rootNode = new DDParent(new DDModel(key, root));
        try {
          Map map = ae.getDelegateAnalysisEngineSpecifiers();
          for (Object obj : map.entrySet()) {
            Map.Entry entry = (Map.Entry) obj;
            // Trace.err("key: " + entry.getKey() + " ; " + entry.getValue().getClass().getName());
            DDObject node = buildAnalysisEngineTree((String) entry.getKey(),
                    (ResourceSpecifier) entry.getValue());
            ((DDParent) rootNode).add(node);
          }
        } catch (InvalidXMLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else {
        rootNode = new DDObject(new DDModel(key, root));
      }
    } else {
      Trace.err("root: " + root.getClass().getName());
    }

    return rootNode;
  }
*/
//  static public void dumpDeploymentTree(DDObject root, int level) {
//    for (int i = 0; i < level; ++i) {
//      System.out.print("---- ");
//    }
//    System.out.println(root.getModel().getLabel());
//
//    // Has chidren ?
//    if (root instanceof DDParent && ((DDParent) root).getChildCount() > 0) {
//      List<DDObject> list = ((DDParent) root).getChildren();
//      for (DDObject ddo : list) {
//        dumpDeploymentTree(ddo, level + 1);
//      }
//    }
//  }

  /** ********************************************************************** */
/*
  static public BaseTNode createAETree(IFile file, String topDescriptor) {
    String dir = FileUtil.getDirectoryFromIFile(file);
    Trace.err("topDescriptor: " + topDescriptor);
    XMLizable xmlizable = null;
    try {
      xmlizable = UimaXmlParsingUtil.parseUimaXmlDescriptor(dir + topDescriptor);
    } catch (InvalidXMLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }

    if (xmlizable instanceof AnalysisEngineDescription) {
      AnalysisEngineDescription ae = (AnalysisEngineDescription) xmlizable;
      String key = ae.getAnalysisEngineMetaData().getName().trim();
      BaseTNode rootNode = buildAETree(key.length() == 0 ? "root" : key, ae);
      return rootNode;
    } else {
      Trace.err("xmlizable: " + xmlizable.getClass().getName());
    }
    return null;
  }

  static public BaseTNode buildAETree(String key, ResourceSpecifier root) {
    BaseTNode rootNode = new BaseTNode(key, root);

    if (root instanceof AnalysisEngineDescription) {
      AnalysisEngineDescription ae = (AnalysisEngineDescription) root;
      if (!ae.isPrimitive()) {
        try {
          Map map = ae.getDelegateAnalysisEngineSpecifiers();
          for (Object obj : map.entrySet()) {
            Map.Entry entry = (Map.Entry) obj;
            // Trace.err("key: " + entry.getKey() + " ; " + entry.getValue().getClass().getName());
            BaseTNode node = buildAETree((String) entry.getKey(), (ResourceSpecifier) entry
                    .getValue());
            rootNode.addChild(node);
          }
        } catch (InvalidXMLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    } else {
      Trace.err("root: " + root.getClass().getName());
    }

    return rootNode;
  }

  static public void dumpAETree(BaseTNode rootNode, int level) {
    for (int i = 0; i < level; ++i) {
      System.out.print("---- ");
    }
    System.out.println(rootNode.getLabel());

    // Has chidren ?
    if (rootNode.getChildrenList() == null) {
      return;
    }
    for (BaseTNode node : (List<BaseTNode>) rootNode.getChildrenList()) {
      dumpAETree(node, level + 1);
    }
  }

  static public void dumpUimaAETree(AnalysisEngineDescription root, int level) {
    for (int i = 0; i < level; ++i) {
      System.out.print("---- ");
    }
    System.out.println(root.getAnalysisEngineMetaData().getName());

    // Has chidren ?
    if (root.isPrimitive()) {
      return;
    }

    // for (AnalysisEngineDescription aed: root.getDelegateAnalysisEngineSpecifiers()) {
    // dumpAETree(node, level+1);
    // }
  }
*/
  /** ********************************************************************** */

  static public boolean isCASMultiplier(AnalysisEngineDescription ae) {
    return isCASMultiplier(ae.getAnalysisEngineMetaData());
  }

  static public boolean isCASMultiplier(AnalysisEngineMetaData meta) {
    OperationalProperties op = meta.getOperationalProperties();
    if (op != null) {
      return op.getOutputsNewCASes();
    }
    return false;
  }
}
