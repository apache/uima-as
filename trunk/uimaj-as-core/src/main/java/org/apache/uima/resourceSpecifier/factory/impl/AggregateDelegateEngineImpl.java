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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.uima.resourceSpecifier.AnalysisEngineType;
import org.apache.uima.resourceSpecifier.factory.AggregateDelegateEngine;
import org.apache.uima.resourceSpecifier.factory.CasMultiplier;
import org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;

// TODO: Auto-generated Javadoc
/**
 * The Class AggregateDelegateEngineImpl.
 */
public class AggregateDelegateEngineImpl implements AggregateDelegateEngine {

  /** The delegates. */
  private List<DelegateAnalysisEngine> delegates = new ArrayList<DelegateAnalysisEngine>();
  
  /** The dcaet. */
  private AnalysisEngineType dcaet;
  
  /** The delegate configuration list. */
  private List<DelegateConfiguration> delegateConfigurationList;
  
  /** The configuration. */
  private DelegateConfiguration configuration;
  
  /** The key. */
  private String key;
  
  /** The input queue scaleout. */
  private int inputQueueScaleout=1;
  
  /** The reply queue scaleup. */
  private int replyQueueScaleup=1;
  
  /**
   * Instantiates a new aggregate delegate engine impl.
   *
   * @param dcaet the dcaet
   * @param key the key
   * @param context the context
   * @param delegateConfigurations the delegate configurations
   */
  public AggregateDelegateEngineImpl(AnalysisEngineType dcaet, String key, ServiceContext context, DelegateConfiguration ... delegateConfigurations) {
    this.dcaet = dcaet;
    setKey(key);
    delegateConfigurationList = Arrays.asList(delegateConfigurations);
    dcaet.setAsync("true");
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#isRemote()
   */
  public boolean isRemote() {
    return false;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#isAggregate()
   */
  public boolean isAggregate() {
    return true;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine#isAsync()
   */
  public boolean isAsync() {
    return Boolean.getBoolean(dcaet.getAsync());
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine#setAsync()
   */
  public void setAsync() {
    dcaet.setAsync("true");
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine#getInputQueueScaleout()
   */
  public int getInputQueueScaleout() {
    return inputQueueScaleout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine#setInputQueueScaleout(int)
   */
  public void setInputQueueScaleout(int scaleup) {
    inputQueueScaleout = scaleup;

  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getKey()
   */
  public String getKey() {
    return key;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setKey(java.lang.String)
   */
  public void setKey(String key) {
    this.key = key;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getReplyQueueScaleup()
   */
  public int getReplyQueueScaleup() {
    return replyQueueScaleup;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setReplyQueueScaleup(int)
   */
  public void setReplyQueueScaleup(int scaleup) {
    replyQueueScaleup = scaleup;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getCasMultiplier()
   */
  public CasMultiplier getCasMultiplier() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setCasMultiplier(org.apache.uima.resourceSpecifier.factory.CasMultiplier)
   */
  public void setCasMultiplier(CasMultiplier cm) {
    // TODO Auto-generated method stub

  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateDelegateEngine#addDelegate(org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine)
   */
  public void addDelegate(DelegateAnalysisEngine delegate) {
    delegates.add(delegate);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AggregateDelegateEngine#getDelegates()
   */
  public List<DelegateAnalysisEngine> getDelegates() {
    return delegates;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getConfiguration()
   */
  public DelegateConfiguration getConfiguration() {
  return null;
}

}
