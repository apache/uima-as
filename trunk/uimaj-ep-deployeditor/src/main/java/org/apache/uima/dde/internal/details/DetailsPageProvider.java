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

package org.apache.uima.dde.internal.details;

import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.aae.deployment.impl.RemoteAEDeploymentMetaData_Impl;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.dde.internal.DeploymentDescriptorEditor;
import org.apache.uima.dde.internal.page.MasterDetails;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.tools.internal.uima.util.FormMessage;
import org.eclipse.jface.dialogs.IMessageProvider;
import org.eclipse.ui.forms.DetailsPart;
import org.eclipse.ui.forms.IDetailsPage;
import org.eclipse.ui.forms.IDetailsPageProvider;
import org.eclipse.ui.forms.IManagedForm;


/**
 * 
 * 
 */
public class DetailsPageProvider implements IDetailsPageProvider {
  private DeploymentDescriptorEditor multiPageEditor;

  private IManagedForm managedForm = null;
  
  private MasterDetails masterPart;

//  private DetailsPart detailsPart;

  public DetailsPageProvider(DeploymentDescriptorEditor editor, IManagedForm managedForm,
          MasterDetails master, DetailsPart detailsPart) {
    this.multiPageEditor = editor;
    this.managedForm = managedForm;
    this.masterPart = master;
//    this.detailsPart = detailsPart;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.forms.IDetailsPageProvider#getPage(java.lang.Object)
   */
  public IDetailsPage getPage(Object key) {
    if (key.equals(AsyncPrimitiveErrorConfiguration.class)) {
      return new AEMetaDataDetailsPage(multiPageEditor, managedForm, masterPart, false);

    } else if (key.equals(AsyncAggregateErrorConfiguration.class)) {
      return new AEMetaDataDetailsPage(multiPageEditor, managedForm, masterPart, true);

      // } else if ( key.equals(RemoteAEDeploymentMetaData.class) ) {
      // return new RemoteAEMetaDataDetails_V1(managedForm);
    }

    // Note: Cannot overcome blank page issue for unknown selection
    // if (detailsPart != null) {
    // return detailsPart.getCurrentPage();
    // }
    return new EmptyDetailsPage(managedForm);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.forms.IDetailsPageProvider#getPageKey(java.lang.Object)
   */
  public Object getPageKey(Object object) {
    updateStatus(null, IMessageProvider.ERROR);
    if (object instanceof AEDeploymentMetaData) {
      AEDeploymentMetaData metaData = (AEDeploymentMetaData) object;

      Class cls = null;
      if ( metaData.isTopAnalysisEngine() ) {
        // Top AE
        // Trace.err("Top AE: " + metaData.getKey());
        cls = AsyncPrimitiveErrorConfiguration.class;
      } else {
        // Delegate
        // Trace.err("NOT Top AE: " + metaData.getKey());
        cls = AsyncAggregateErrorConfiguration.class;
      }
          
      return cls;

    } else if (object instanceof RemoteAEDeploymentMetaData) {
      return AsyncAggregateErrorConfiguration.class;
    } else {

    }

    Trace.trace("Unkown class: " + object.getClass().getName());
    return EmptyDetailsPage.class;
  }
  public Object getPageKey_OLD(Object object) {
    updateStatus(null, IMessageProvider.ERROR);
    if (object instanceof AEDeploymentMetaData) {
      AEDeploymentMetaData metaData = (AEDeploymentMetaData) object;

      Class cls = null;
      ResourceSpecifier rs = metaData.getResourceSpecifier();
      if (rs != null) {
        if (rs instanceof AnalysisEngineDescription) {
          if ( metaData.isTopAnalysisEngine() ) {
            // Prmitive
            cls = AsyncPrimitiveErrorConfiguration.class;
          } else {
            // Aggregate
            cls = AsyncAggregateErrorConfiguration.class;
          }
        }
      } else {
        String parentKey = null;
        if (metaData.getParent() != null) {
          parentKey = metaData.getParent().getKey();          
        } else {
          if (metaData.isTopAnalysisEngine()) {
            updateStatus("The top descriptor is not specified.", IMessageProvider.ERROR);
            return null;
          }
        }
        if (parentKey == null) {
          parentKey = "Top Analysis Engine";
        }
        updateStatus("The analysis engine's key=\"" + metaData.getKey()
                + "\" is not valid for the aggregate \"" + parentKey + "\"", IMessageProvider.ERROR);
      }
      return cls;

    } else if (object instanceof RemoteAEDeploymentMetaData) {
      RemoteAEDeploymentMetaData metaData = (RemoteAEDeploymentMetaData) object;

      Class cls = null;
      ResourceSpecifier rs = metaData.getResourceSpecifier();
      if (rs != null) {
        if (rs instanceof AnalysisEngineDescription) {
          cls = AsyncAggregateErrorConfiguration.class;
        }
      } else {
        String parentKey = metaData.getParent().getKey();
        if (parentKey == null) {
          parentKey = "Top Analysis Engine";
        }
        updateStatus("The analysis engine's key=\"" + metaData.getKey()
                + "\" is not valid for the aggregate \"" + parentKey + "\"", IMessageProvider.ERROR);
      }
      return cls;
      // return AsyncPrimitiveErrorConfiguration.class;
    } else {

    }

    Trace.trace("Unkown class: " + object.getClass().getName());
    return EmptyDetailsPage.class;
  }

  /** ********************************************************************** */

  protected void updateStatus(String msg, int msgType) {
    FormMessage.setMessage(managedForm.getForm().getForm(), msg, msgType);
  }

}
