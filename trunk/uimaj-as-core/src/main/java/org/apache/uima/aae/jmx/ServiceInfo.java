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

package org.apache.uima.aae.jmx;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController_impl;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.BaseAnalysisEngineController.ServiceState;
import org.apache.uima.util.Level;

public class ServiceInfo implements ServiceInfoMBean {
  private static final Class CLASS_NAME = ServiceInfoMBean.class;

  /**
	 * 
	 */
  // private static final long serialVersionUID = -4662094240276750977L;
  private static final String label = "Service Info";

  private String brokerURL = "";

  private String inputQueueName = "";

  private String replyQueueName = "";

  private String state="";

  private String deploymentDescriptorPath = "";

  private boolean casMultiplier;

  private boolean topLevel;

  private String serviceKey;

  private boolean aggregate;

  private String cmUniqueName;
  
  private AnalysisEngineController controller;
  
  public ServiceInfo(boolean isaCasMultiplier, AnalysisEngineController controller) {
    casMultiplier = isaCasMultiplier;
    this.controller = controller;
  }
  
  public void setCmRegisteredName(String uniqueName ) {
    cmUniqueName = uniqueName;
  }
  
  public String getCmRegisteredName() {
    return cmUniqueName;
  }
  public String getLabel() {
    return label;
  }

  public String getBrokerURL() {
    return brokerURL;
  }

  public String getDeploymentDescriptorPath() {
    return deploymentDescriptorPath;
  }

  public void setDeploymentDescriptorPath(String deploymentDescriptorPath) {
    this.deploymentDescriptorPath = deploymentDescriptorPath;
  }

  public void setBrokerURL(String aBrokerURL) {
    brokerURL = aBrokerURL;
  }

  public String getInputQueueName() {
    return inputQueueName;
  }

  public void setInputQueueName(String anInputQueueName) {
    inputQueueName = anInputQueueName;
  }

  public String getState() {
    if ( controller != null ) {
      return controller.getState().name();
    } else {
      return state;
    }
  }

  public String dumpState() {
    if ( controller != null ) {
      StringBuffer buffer = new StringBuffer();
      controller.dumpState(buffer,"  ");
      return buffer.toString();
    }    
    return "";
  }
  public void setState(String aState) {
    state = aState;
  }

  public boolean isCASMultiplier() {
    return casMultiplier;
  }

  public void setCASMultiplier() {
    casMultiplier = true;
  }

  public void setTopLevel() {
    topLevel = true;
  }

  public boolean isTopLevel() {
    return topLevel;
  }

  public String getServiceKey() {
    return serviceKey;
  }

  public void setServiceKey(String serviceKey) {
    this.serviceKey = serviceKey;
  }

  public String getReplyQueueName() {
    return replyQueueName;
  }

  public void setReplyQueueName(String aReplyQueueName) {
    replyQueueName = aReplyQueueName;
  }

  public boolean isAggregate() {
    return aggregate;
  }

  public void setAggregate(boolean aggregate) {
    this.aggregate = aggregate;
  }

}
