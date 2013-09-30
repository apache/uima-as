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
import java.util.Properties;

import org.apache.uima.resourceSpecifier.AnalysisEngineDeploymentDescriptionDocument;
import org.apache.uima.resourceSpecifier.AnalysisEngineDeploymentDescriptionType;
import org.apache.uima.resourceSpecifier.DeploymentType;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.Deployment;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor;
import org.apache.xmlbeans.XmlOptions;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class UimaASDeploymentDescriptorImpl.
 */
public class UimaASDeploymentDescriptorImpl implements UimaASDeploymentDescriptor {
  
  /** The dd. */
  protected AnalysisEngineDeploymentDescriptionDocument dd;
  
  /** The dt. */
  protected AnalysisEngineDeploymentDescriptionType dt;
  
  /** The deployment. */
  protected Deployment deployment;
  
  /**
   * Instantiates a new uima as deployment descriptor impl.
   *
   * @param dd the dd
   * @param context the context
   * @param delegateConfigurations the delegate configurations
   */
  public UimaASDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument dd, ServiceContext context, DelegateConfiguration ... delegateConfigurations) {
    this.dd = dd;
    Assert.notNull(dd);
    dt = dd.addNewAnalysisEngineDeploymentDescription();
    setName(context.getName());
    setDescription(context.getDescription());
    deployment = new DeploymentImpl(dt.addNewDeployment(),context,delegateConfigurations);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor#setName(java.lang.String)
   */
  public void setName(String name) {
    Assert.notNull(dt);
    dt.setName(name);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor#getName()
   */
  public String getName() {
    Assert.notNull(dt);
    return dt.getName();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor#setDescription(java.lang.String)
   */
  public void setDescription(String description) {
    Assert.notNull(dt);
    dt.setDescription(description);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor#getDescription()
   */
  public String getDescription() {
    Assert.notNull(dt);
    return dt.getDescription();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor#getDeployment()
   */
  public Deployment getDeployment() {
    Assert.notNull(deployment);
    return deployment;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor#save(java.io.File)
   */
  public void save(File file) throws Exception {
    Assert.notNull(dd);
    XmlOptions opts = new XmlOptions();
    opts.setSavePrettyPrint();
    //opts.s
    opts.setSavePrettyPrintIndent(4);
    opts.setUseDefaultNamespace();
    dd.save(file,opts);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor#toXML()
   */
  public String toXML() {
    Assert.notNull(dd);
    XmlOptions opts = new XmlOptions();
    opts.setSavePrettyPrint();
    //opts.s
    opts.setSavePrettyPrintIndent(4);
    opts.setUseDefaultNamespace();
    return dd.xmlText(opts);
    //return dd.toString();
  }
  
  /**
   * The Class DeploymentContext.
   */
  protected static class DeploymentContext {
    
    /** The context properties. */
    private static Properties contextProperties=null;
    
    /**
     * Gets the context.
     *
     * @return the context
     */
    protected static Properties getContext() {
      if ( contextProperties == null ) {
        contextProperties = new Properties();
      }
      return contextProperties;
    }
    
    /**
     * Sets the context.
     *
     * @param props the new context
     */
    protected static void setContext(Properties props) {
      contextProperties = props;
    }
  }
  
  
  
}
