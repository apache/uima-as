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

import org.apache.uima.resourceSpecifier.AnalysisEngineDeploymentDescriptionDocument;
import org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrors;
import org.apache.uima.resourceSpecifier.factory.GetMetaErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.ProcessCasErrors;
import org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASPrimitiveDeploymentDescriptor;
import org.apache.uima.resourceSpecifier.factory.impl.DeploymentDescriptorImpl;
import org.apache.uima.resourceSpecifier.factory.impl.UimaASDeploymentDescriptorImpl;

// TODO: Auto-generated Javadoc
/**
 * The Class UimaASPrimitiveDeploymentDescriptorImpl.
 */
public class UimaASPrimitiveDeploymentDescriptorImpl extends DeploymentDescriptorImpl
implements UimaASPrimitiveDeploymentDescriptor {
  
  /**
   * Instantiates a new uima as primitive deployment descriptor impl.
   *
   * @param dd the dd
   * @param context the context
   */
  public UimaASPrimitiveDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument dd,
          ServiceContext context ) {
    super(new UimaASDeploymentDescriptorImpl(dd,context));
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASPrimitiveDeploymentDescriptor#setScaleup(int)
   */
  public void setScaleup(int scaleup) {
    super.deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().getInputQueueScaleout().setNumberOfInstances(scaleup);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASPrimitiveDeploymentDescriptor#getScaleup()
   */
  public int getScaleup() {
    return super.deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().getInputQueueScaleout().getNumberOfInstances();
  }

  public ProcessCasErrors getProcessErrorHandlingSettings() {
    return super.deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().getPrimitiveErrorConfiguration().getProcessCasErrors();
  }

  public GetMetaErrorHandlingSettings getGetMetaErrorHandlingSettings() {
    // TODO Auto-generated method stub
    return null;
  }

  public CollectionProcessCompleteErrors getCollectionProcessCompleteErrorHandlingSettings() {
    return null;
    //return super.deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().getPrimitiveErrorConfiguration().getCollectionProcessCompleteErrors();
  }

}
