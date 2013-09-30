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
package org.apache.uima.resourceSpecifier.factory.impl;

import java.util.List;

import org.apache.uima.resourceSpecifier.AnalysisEngineDeploymentDescriptionDocument;
import org.apache.uima.resourceSpecifier.AnalysisEngineDeploymentDescriptionType;
import org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.Delegates;
import org.apache.uima.resourceSpecifier.factory.Deployment;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASAggregateDeploymentDescriptor;
import org.apache.uima.resourceSpecifier.factory.impl.DeploymentDescriptorImpl;
import org.apache.uima.resourceSpecifier.factory.impl.DeploymentImpl;
import org.apache.uima.resourceSpecifier.factory.impl.UimaASDeploymentDescriptorImpl;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class UimaASAggregateDeploymentDescriptorImpl.
 */
public class UimaASAggregateDeploymentDescriptorImpl extends DeploymentDescriptorImpl
implements UimaASAggregateDeploymentDescriptor {
  //private AnalysisEngineDeploymentDescriptionDocument dd;
  //private AnalysisEngineDeploymentDescriptionType dt;
  //private Deployment deployment;

  /**
   * Instantiates a new uima as aggregate deployment descriptor impl.
   *
   * @param dd the dd
   * @param context the context
   * @param delegateConfigurations the delegate configurations
   */
  public UimaASAggregateDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument dd,
          ServiceContext context, DelegateConfiguration ... delegateConfigurations) {
    super(new UimaASDeploymentDescriptorImpl(dd,context,delegateConfigurations));
    //this.dd = dd;
    //Assert.notNull(dd);
//    dt = dd.addNewAnalysisEngineDeploymentDescription();
    //setName(context.getName());
    //setDescription(context.getDescription());
    //deployment = new DeploymentImpl(dt.addNewDeployment(),context,delegateConfigurations);
  }
/*
  public List<DelegateConfiguration> getDelegates() {
    List<DelegateConfiguration> delegateList = new ArrayList<DelegateConfiguration>();
    Delegates delegates =
      super.deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().getDelegates();
    DelegateConfiguration dc = null;
    for( DelegateAnalysisEngine delegate : delegates.getDelegates()) {
      if ( delegate instanceof RemoteDelegateEngine ) {
        RemoteDelegateEngine rde = (RemoteDelegateEngine)delegate;
        dc = new RemoteDelegateConfigurationImpl(rde.getKey(), 
                rde.getInputQueue().getBroker(), 
                rde.getInputQueue().getEndpoint(), 
                rde.getSerializationStrategy(), 
                rde.getErrorConfiguration().getGetMetaErrorHandlingSettings(),
                rde.getErrorConfiguration().getProcessErrorHandlingSettings());
      } else if ( delegate instanceof ColocatedDelegateEngine) {
        ColocatedDelegateEngine cdc = (ColocatedDelegateEngine)delegate;
        DelegateConfiguration[] delegateConfigurations = new DelegateConfiguration[cdc.getDelegates().size()];
        int indx=0;
        for( DelegateAnalysisEngine nestedDelegate : cdc.getDelegates() ) {
          delegateConfigurations[indx++] = nestedDelegate.
        }
        dc = new ColocatedDelegateConfigurationImpl(cdc.getKey(), delegateConfigurations);
      } else {
        // ????????
        // Log
        //throw new Exception("Invalid Delegate Type");
        continue;
      }
      delegateList.add(dc);
    }
    return delegateList;
  }
*/
  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.UimaASAggregateDeploymentDescriptor#getDelegates()
 */
public List<DelegateAnalysisEngine> getDelegates() {
//    List<DelegateConfiguration> delegateList = new ArrayList<DelegateConfiguration>();
    Delegates delegates = super.getDeploymentDescriptor().getDeployment().getService().getTopLevelAnalysisEngine().getDelegates();
//      super.deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().getDelegates();
    return delegates.getDelegates();
  }
  
}
