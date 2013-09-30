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

import java.util.Properties;

import org.apache.uima.resourceSpecifier.DeploymentType;
import org.apache.uima.resourceSpecifier.factory.CasPool;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.Deployment;
import org.apache.uima.resourceSpecifier.factory.Service;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class DeploymentImpl.
 */
public class DeploymentImpl implements Deployment {
  
  /** The dt. */
  private DeploymentType dt = null;
  
  /** The service. */
  private Service service=null;
  
  /** The cas pool. */
  private CasPool casPool = null;
  
  /**
   * Instantiates a new deployment impl.
   *
   * @param dt the dt
   * @param context the context
   * @param delegateConfigurations the delegate configurations
   */
  protected DeploymentImpl(DeploymentType dt,ServiceContext context, DelegateConfiguration ... delegateConfigurations) {
    Assert.notNull(dt);
    this.dt = dt;
    setProtocol(context.getProtocol());
    setProvider(context.getProvider());
    
    service = new ServiceImpl(dt.addNewService(),context, delegateConfigurations);
    casPool = new CasPoolImpl(dt.addNewCasPool(),context);
  }
  
  /**
   * Sets the protocol.
   *
   * @param protocol the new protocol
   */
  public void setProtocol(String protocol) {
    Assert.notNull(dt);
    dt.setProtocol(VALID_PROTOCOLS.jms.name());
  }
  
  /**
   * Gets the protocol.
   *
   * @return the protocol
   */
  public String getProtocol() {
    Assert.notNull(dt);
    return dt.getProtocol();
  }
  
  /**
   * Sets the provider.
   *
   * @param provider the new provider
   */
  public void setProvider(String provider) {
    Assert.notNull(dt);
    dt.setProvider(VALID_PROVIDERS.activemq.name());
  }
  
  /**
   * Gets the provider.
   *
   * @return the provider
   */
  public String getProvider() {
    Assert.notNull(dt);
    return dt.getProvider();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Deployment#getService()
   */
  public Service getService() {
    Assert.notNull(service);
    return service;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Deployment#getCasPool()
   */
  public CasPool getCasPool() {
    Assert.notNull(casPool);
    return casPool;
  }

}
