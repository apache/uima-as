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

import org.apache.uima.resourceSpecifier.factory.Action;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class ServiceContextImpl.
 */
public class ServiceContextImpl implements ServiceContext {
  
  /** The name. */
  private String name="";
  
  /** The async. */
  private boolean async;
 
  /** The description. */
  private String description="";
  
  /** The descriptor. */
  private String descriptor="";
  
  /** The protocol. */
  private String protocol="jms";
  
  /** The provider. */
  private String provider="activemq";
  
  /** The endpoint. */
  private String endpoint="";
  
  /** The broker url. */
  private String brokerURL="${DefaultBrokerURL}";
  
  /** The scale up. */
  private int scaleUp=1;
  
  /** The cas multiplier. */
  private boolean casMultiplier=false;
  
  /** The aggregate. */
  private boolean aggregate=false;
  
  /** The prefetch. */
  private int prefetch=0;
  
  /** The cas pool size. */
  private int casPoolSize=1;
  
  /** The initial heap size. */
  private int initialHeapSize=2000000;
  
  /** The process parent last. */
  private boolean processParentLast=false;
  
  /** The process error threshold count. */
  private int processErrorThresholdCount=0;
  
  /** The process error threshold window. */
  private int processErrorThresholdWindow=0;
  
  /** The process error threshold action. */
  private Action processErrorThresholdAction = Action.Terminate;
  
  /** The cpc additional action. */
  private Action cpcAdditionalAction = Action.Terminate;
  /**
   * This class describes UIMA AS service. The minimal information needed is the service name, description, the AnalysisEngine
   * descriptor and the queue name. If not provided, the class uses defaults to fully describe the service.
   * 
   * @param name - service name
   * @param description - service description
   * @param descriptor - analysis engine descriptor
   * @param endpoint - the name of of the service input queue
   */
  public ServiceContextImpl(String name, String description, String descriptor, String endpoint) {
    this(name,description, descriptor,endpoint,null);
  }
  
  public ServiceContextImpl(String name, String description, String descriptor, String endpoint, String brokerURL) {
    setName(name);
    setDescription(description);
    Assert.notNull(descriptor);
    setDescriptor(descriptor);
    Assert.notNull(endpoint);
    setEndpoint(endpoint);
    if ( brokerURL != null ) {
      setBrokerURL(brokerURL);
    }
  }
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setName(java.lang.String)
   */
  public void setName(String name) {
    this.name = name;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getName()
   */
  public String getName() {
    return name;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setDescription(java.lang.String)
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getDescription()
   */
  public String getDescription() {
    return description;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setDescriptor(java.lang.String)
   */
  public void setDescriptor(String descriptor) {
    this.descriptor = descriptor;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getDescriptor()
   */
  public String getDescriptor() {
    return descriptor;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setProtocol(java.lang.String)
   */
  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getProtocol()
   */
  public String getProtocol() {
    return protocol;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setProvider(java.lang.String)
   */
  public void setProvider(String provider) {
    this.provider = provider;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getProvider()
   */
  public String getProvider() {
    return provider;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setEndpoint(java.lang.String)
   */
  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getEndpoint()
   */
  public String getEndpoint() {
    return endpoint;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setBrokerURL(java.lang.String)
   */
  public void setBrokerURL(String brokerURL) {
    this.brokerURL = brokerURL;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getBrokerURL()
   */
  public String getBrokerURL() {
    return brokerURL;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setPrefetch(int)
   */
  public void setPrefetch(int prefetch) {
    this.prefetch = prefetch;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getPrefetch()
   */
  public int getPrefetch() {
    return prefetch;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setScaleup(int)
   */
  public void setScaleup(int scaleUp) {
    this.scaleUp = scaleUp;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getScaleup()
   */
  public int getScaleup() {
    return scaleUp;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setCasMultiplier(boolean)
   */
  public void setCasMultiplier(boolean casMultiplier) {
      this.casMultiplier = casMultiplier;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#isCasMultiplier()
   */
  public boolean isCasMultiplier() {
    return casMultiplier;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setAggregate(boolean)
   */
  public void setAggregate(boolean aggregate) {
    this.aggregate = aggregate;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#isAggregate()
   */
  public boolean isAggregate() {
    return aggregate;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setCasPoolSize(int)
   */
  public void setCasPoolSize(int casPoolSize) {
    this.casPoolSize = casPoolSize;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getCasPoolSize()
   */
  public int getCasPoolSize() {
    return casPoolSize;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setInitialHeapSize(int)
   */
  public void setInitialHeapSize(int initialHeapSize) {
    this.initialHeapSize = initialHeapSize;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getInitialHeapSize()
   */
  public int getInitialHeapSize() {
    return initialHeapSize;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setProcessParentLast(boolean)
   */
  public void setProcessParentLast(boolean processParentLast) {
    this.processParentLast = processParentLast;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#processParentLast()
   */
  public boolean processParentLast() {
    return processParentLast;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setProcessErrorThresholdCount(int)
   */
  public void setProcessErrorThresholdCount(int thresholdCount) {
    this.processErrorThresholdCount = thresholdCount;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getProcessErrorThresholdCount()
   */
  public int getProcessErrorThresholdCount() {
    return processErrorThresholdCount;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setProcessErrorThresholdWindow(int)
   */
  public void setProcessErrorThresholdWindow(int thresholdWindow) {
    this.processErrorThresholdWindow = thresholdWindow;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getProcessErrorThresholdWindow()
   */
  public int getProcessErrorThresholdWindow() {
    return processErrorThresholdWindow;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setProcessErrorThresholdAction(org.apache.uima.resourceSpecifier.factory.Action)
   */
  public void setProcessErrorThresholdAction(Action action) {
    this.processErrorThresholdAction = action;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getProcessErrorThresholdAction()
   */
  public Action getProcessErrorThresholdAction() {
    return processErrorThresholdAction;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setCpCAdditionalAction(org.apache.uima.resourceSpecifier.factory.Action)
   */
  public void setCpCAdditionalAction(Action action) {
    this.cpcAdditionalAction = action;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#getCpCAdditionalAction()
   */
  public Action getCpCAdditionalAction() {
    return cpcAdditionalAction;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#setAsync(boolean)
   */
  public void setAsync(boolean async) {
    this.async = async;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ServiceContext#isAsync()
   */
  public boolean isAsync() {
    return async;
  }

}
