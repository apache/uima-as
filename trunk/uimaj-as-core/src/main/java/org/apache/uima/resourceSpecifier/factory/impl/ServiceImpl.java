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

import org.apache.uima.resourceSpecifier.ServiceType;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.InputQueue;
import org.apache.uima.resourceSpecifier.factory.Service;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.TopDescriptor;
import org.apache.uima.resourceSpecifier.factory.TopLevelAnalysisEngine;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class ServiceImpl.
 */
public class ServiceImpl implements Service {
  
  /** The iq. */
  private InputQueue iq = null;
  
  /** The td. */
  private TopDescriptor td = null;
  
  /** The tlae. */
  private TopLevelAnalysisEngine tlae = null;
  
  /** The st. */
  private ServiceType st;
  
  /**
   * Instantiates a new service impl.
   *
   * @param st the st
   * @param context the context
   * @param delegateConfigurations the delegate configurations
   */
  protected ServiceImpl(ServiceType st, ServiceContext context, DelegateConfiguration ... delegateConfigurations) {
    Assert.notNull(st);
    this.st = st;
    iq = new InputQueueImpl(st.addNewInputQueue(), context);
    td = new TopDescriptorImpl(st.addNewTopDescriptor(), context);
    tlae = new TopLevelAnalysisEngineImpl(st.addNewAnalysisEngine(), context, delegateConfigurations);
   
  }

  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Service#getInputQueue()
   */
  public InputQueue getInputQueue() {
    Assert.notNull(iq);
    return iq;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Service#getTopDescriptor()
   */
  public TopDescriptor getTopDescriptor() {
    Assert.notNull(td);
    return td;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Service#getTopLevelAnalysisEngine()
   */
  public TopLevelAnalysisEngine getTopLevelAnalysisEngine() {
    Assert.notNull(tlae);
    return tlae;
  }

}
