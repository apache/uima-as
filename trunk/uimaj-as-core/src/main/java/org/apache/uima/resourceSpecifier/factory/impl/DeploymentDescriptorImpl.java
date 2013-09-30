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

import java.io.File;

import org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor;
import org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor;

// TODO: Auto-generated Javadoc
/**
 * The Class DeploymentDescriptorImpl.
 */
public abstract class DeploymentDescriptorImpl implements DeploymentDescriptor {
  
  /** The deployment descriptor. */
  protected UimaASDeploymentDescriptor deploymentDescriptor;

  /**
   * Instantiates a new deployment descriptor impl.
   *
   * @param deploymentDescriptor the deployment descriptor
   */
  protected DeploymentDescriptorImpl(UimaASDeploymentDescriptor deploymentDescriptor) {
    this.deploymentDescriptor = deploymentDescriptor;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getDeploymentDescriptor()
   */
  protected UimaASDeploymentDescriptor getDeploymentDescriptor() {
    return deploymentDescriptor;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setName(java.lang.String)
   */
  public void setName(String name) {
    deploymentDescriptor.setName(name);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getName()
   */
  public String getName() {
    return deploymentDescriptor.getName();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setDescription(java.lang.String)
   */
  public void setDescription(String description) {
    deploymentDescriptor.setDescription(description);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getDescription()
   */
  public String getDescription() {
    return deploymentDescriptor.getDescription();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setServiceAnalysisEngineDescriptor(java.lang.String)
   */
  public void setServiceAnalysisEngineDescriptor(String aeDescriptorPath) {
    deploymentDescriptor.getDeployment().getService().getTopDescriptor().getImport().setLocation(aeDescriptorPath);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getServiceAnalysisEngineDescriptor()
   */
  public String getServiceAnalysisEngineDescriptor() {
    return deploymentDescriptor.getDeployment().getService().getTopDescriptor().getImport().getLocation();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setEndoint(java.lang.String)
   */
  public void setEndoint(String endpoint) {
    deploymentDescriptor.getDeployment().getService().getInputQueue().setEndpoint(endpoint);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getEndpoint()
   */
  public String getEndpoint() {
    return deploymentDescriptor.getDeployment().getService().getInputQueue().getEndpoint();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setBroker(java.lang.String)
   */
  public void setBroker(String broker) {
    deploymentDescriptor.getDeployment().getService().getInputQueue().setBroker(broker);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getBroker()
   */
  public String getBroker() {
    return deploymentDescriptor.getDeployment().getService().getInputQueue().getBroker();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setPrefetch(int)
   */
  public void setPrefetch(int prefetch) {
    deploymentDescriptor.getDeployment().getService().getInputQueue().setPrefetch(prefetch);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getPrefetch()
   */
  public int getPrefetch() {
    return deploymentDescriptor.getDeployment().getService().getInputQueue().getPrefetch();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setCasPoolSize(int)
   */
  public void setCasPoolSize(int casPoolSize) {
    deploymentDescriptor.getDeployment().getCasPool().setNumberOfCases(casPoolSize);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getCasPoolSize()
   */
  public int getCasPoolSize() {
    return deploymentDescriptor.getDeployment().getCasPool().getNumberOfCases();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setInitialHeapSize(int)
   */
  public void setInitialHeapSize(int initialHeapSize) {
    deploymentDescriptor.getDeployment().getCasPool().setInitialFsHeapSize(initialHeapSize);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#getInitialHeapSize()
   */
  public int getInitialHeapSize() {
    return deploymentDescriptor.getDeployment().getCasPool().getInitialFsHeapSize();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#toXML()
   */
  public String toXML() {
    return deploymentDescriptor.toXML();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#save(java.io.File)
   */
  public void save(File file) throws Exception {
    deploymentDescriptor.save(file);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#isAsync()
   */
  public boolean isAsync() {
    return deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().isAsync();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor#setAsync(boolean)
   */
  public void setAsync(boolean async) {
    deploymentDescriptor.getDeployment().getService().getTopLevelAnalysisEngine().setIsAsync();
  }
  public void setProtocol(String protocol) {
    deploymentDescriptor.getDeployment().setProtocol(protocol);
  }
  
  public String getProtocol() {
    return deploymentDescriptor.getDeployment().getProtocol();
  }
  
  public void setProvider(String provider) {
    deploymentDescriptor.getDeployment().setProvider(provider);
  }
  
  public String getProvider() {
    return deploymentDescriptor.getDeployment().getProvider();
  }

}
