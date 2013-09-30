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

package org.apache.uima.dde.internal.provider;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.aae.deployment.impl.AEDelegates_Impl;
import org.apache.uima.aae.deployment.impl.AsyncAggregateErrorConfiguration_Impl;
import org.apache.uima.aae.deployment.impl.GetMetadataErrors_Impl;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.util.InvalidXMLException;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.ITreeContentProvider;
import org.eclipse.jface.viewers.Viewer;


public class DDTreeContentProvider implements IStructuredContentProvider, ITreeContentProvider {
  private static final Object[] EMPTY_ARRAY = new Object[0];

  public Object[] getChildren(Object parent) {
    Object[] objs = null;
    int count = 0;
    if (parent instanceof AEDeploymentMetaData) {
      AEDeploymentMetaData metaData = (AEDeploymentMetaData) parent;

//      if (metaData.getAsyncAEErrorConfiguration() == null) {
//        // Create a new Error Config
//        if (metaData.getResourceSpecifier() != null) {
//          ResourceSpecifier rs = metaData.getResourceSpecifier();
//          if (rs instanceof AnalysisEngineDescription) {
//            Class cls;
//            if (((AnalysisEngineDescription) rs).isPrimitive()) {
//              // Prmitive
//              cls = AsyncPrimitiveErrorConfiguration.class;
//            } else {
//              // Aggregate
//              cls = AsyncAggregateErrorConfiguration.class;
//            }
//
//            Object obj = UIMAFramework.getResourceSpecifierFactory().createObject(cls);
//            if (obj != null) {
//              Trace.err("OK to create " + cls.getName() + " for " + metaData.getKey());
//              metaData.setAsyncAEErrorConfiguration((AsyncAEErrorConfiguration) obj);
//            } else {
//              Trace.err("CANNOT create " + cls.getName() + " for " + metaData.getKey());
//            }
//          } else {
//            Trace.bug("Should be AnalysisEngineDescription: " + rs.getClass().getName());
//          }
//        } else {
//          Trace.bug("ResourceSpecifier == null for " + metaData.getKey());
//        }
//      }

      if (metaData.isAsync()) {
        if (metaData.getDelegates() != null) {
          List list = metaData.getDelegates().getDelegates();
          return list.toArray();
        }
      }
    } else if (parent instanceof RemoteAEDeploymentMetaData) {
      RemoteAEDeploymentMetaData metaData = (RemoteAEDeploymentMetaData) parent;

    } else if (parent instanceof AEDelegates_Impl) {
      return ((AEDelegates_Impl) parent).getDelegates().toArray();

    } else if (parent instanceof AsyncAggregateErrorConfiguration) {
      AsyncAggregateErrorConfiguration errorConfig = (AsyncAggregateErrorConfiguration) parent;

    } else if (parent instanceof GetMetadataErrors) {
      GetMetadataErrors getMetadataErrors = (GetMetadataErrors) parent;
      objs = new Object[3];
      objs[0] = "MaxRetries: " + getMetadataErrors.getMaxRetries();
      objs[1] = "Timeout: " + getMetadataErrors.getTimeout();
      objs[2] = "Error Action: " + getMetadataErrors.getErrorAction();
      return objs;

    } else if (parent instanceof ProcessCasErrors) {
      ProcessCasErrors processCasErrors = (ProcessCasErrors) parent;
      objs = new Object[6];
      objs[0] = "MaxRetries: " + processCasErrors.getMaxRetries();
      objs[1] = "Timeout: " + processCasErrors.getTimeout();
      objs[2] = "ContinueOnRetryFailure: " + processCasErrors.isContinueOnRetryFailure();
      objs[3] = "Threshold Count: " + processCasErrors.getThresholdCount();
      objs[4] = "Threshold Window: " + processCasErrors.getThresholdWindow();
      objs[5] = "Threshold Action: " + processCasErrors.getThresholdAction();
      return objs;

    } else if (parent instanceof CollectionProcessCompleteErrors) {
      CollectionProcessCompleteErrors completeErrors = (CollectionProcessCompleteErrors) parent;
      objs = new Object[2];
      objs[0] = "Timeout: " + completeErrors.getTimeout();
      objs[1] = "Additional Error Action: " + completeErrors.getAdditionalErrorAction();
      return objs;

    } else if (parent instanceof AsyncPrimitiveErrorConfiguration) {
      AsyncPrimitiveErrorConfiguration errorConfig = (AsyncPrimitiveErrorConfiguration) parent;

      if (errorConfig.hasImport()) {
        String importBy = "import ";
        if (errorConfig.isImportByLocation()) {
          importBy += " location=" + errorConfig.getImportedDescriptor();
        } else {
          importBy += " name=" + errorConfig.getImportedDescriptor();
        }
        objs = new Object[1];
        objs[0] = importBy;
        return objs;
      }

      if (errorConfig.getGetMetadataErrors() != null) {
        ++count;
      }

      if (errorConfig.getProcessCasErrors() != null) {
        ++count;
      }

      if (errorConfig.getCollectionProcessCompleteErrors() != null) {
        ++count;
      }

      objs = new Object[count];
      int index = 0;
      if (errorConfig.getGetMetadataErrors() != null) {
        objs[index++] = errorConfig.getGetMetadataErrors();
      }

      if (errorConfig.getProcessCasErrors() != null) {
        objs[index++] = errorConfig.getProcessCasErrors();
      }

      if (errorConfig.getCollectionProcessCompleteErrors() != null) {
        objs[index++] = errorConfig.getCollectionProcessCompleteErrors();
      }
      return objs;
    }

    return EMPTY_ARRAY;
  }

  public Object getParent(Object element) {
    return null;
  }

  public boolean hasChildren(Object parent) {
    return getChildren(parent).length > 0;
  }

  public Object[] getElements(Object inputElement) {
    if (inputElement instanceof AEDeploymentDescription) {
      AEDeploymentMetaData metaData = null;
      try {
        metaData = ((AEDeploymentDescription) inputElement).getAeService()
                .getAnalysisEngineDeploymentMetaData();
//        if (metaData != null && metaData.getAsyncAEErrorConfiguration() == null) {
//          // Create a new Error Config
//          Object obj = UIMAFramework.getResourceSpecifierFactory().createObject(
//                  AsyncAggregateErrorConfiguration.class);
//          if (obj != null) {
//            Trace.err("OK to create AsyncAEErrorConfiguration for " + metaData.getKey());
//            metaData.setAsyncAEErrorConfiguration((AsyncAggregateErrorConfiguration) obj);
//          } else {
//            Trace.err("Cannot create AsyncAggregateErrorConfiguration");
//          }
//        } else {
//          Trace.err("NO AEDeploymentMetaData");
//        }
      } catch (InvalidXMLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      if (metaData != null) {
        Object[] objs = new Object[1];
        objs[0] = metaData;
        return objs;
      }
    }
    Trace.err("inputElement: " + inputElement.getClass().getName());
    return EMPTY_ARRAY; // Should NOT return "null"
  }

  public void dispose() {
    // TODO Auto-generated method stub
  }

  public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
//    if (newInput != null) {
//      Trace.err("newInput: " + newInput.getClass().getName());
//    }
  }

}
