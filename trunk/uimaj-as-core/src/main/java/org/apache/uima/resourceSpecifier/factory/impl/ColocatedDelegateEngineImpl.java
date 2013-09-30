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

import org.apache.uima.resourceSpecifier.AnalysisEngineType;
import org.apache.uima.resourceSpecifier.factory.CasMultiplier;
import org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;

// TODO: Auto-generated Javadoc
/**
 * The Class ColocatedDelegateEngineImpl.
 */
public class ColocatedDelegateEngineImpl implements ColocatedDelegateEngine {
  
  /** The dcaet. */
  private AnalysisEngineType dcaet;
  
  /** The cm. */
  private CasMultiplier cm=null;
  
  /** The configuration. */
  private DelegateConfiguration configuration;
  
  /**
   * Instantiates a new colocated delegate engine impl.
   *
   * @param dcaet the dcaet
   * @param cdc the cdc
   * @param context the context
   */
  public ColocatedDelegateEngineImpl(AnalysisEngineType dcaet, ColocatedDelegateConfiguration cdc, ServiceContext context) {
    this.dcaet = dcaet;
    setKey(cdc.getKey());
    setReplyQueueScaleup(cdc.getInternalReplyQueueScaleout());
    if ( cdc.isCasMultiplier()) {
      cm = new CasMultiplierImpl(dcaet.addNewCasMultiplier(), cdc.getCasPoolSize(), cdc.getInitialHeapSize(), cdc.processParentLast());
    }
    configuration= cdc;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getKey()
   */
  public String getKey() {
    return dcaet.getKey();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setKey(java.lang.String)
   */
  public void setKey(String key) {
    dcaet.setKey(key);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getReplyQueueScaleup()
   */
  public int getReplyQueueScaleup() {
    return Integer.parseInt(dcaet.getInternalReplyQueueScaleout());
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setReplyQueueScaleup(int)
   */
  public void setReplyQueueScaleup(int scaleup) {
    dcaet.setInputQueueScaleout(String.valueOf(scaleup));
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getCasMultiplier()
   */
  public CasMultiplier getCasMultiplier() {
    return cm;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setCasMultiplier(org.apache.uima.resourceSpecifier.factory.CasMultiplier)
   */
  public void setCasMultiplier(CasMultiplier cm) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine#isAsync()
   */
  public boolean isAsync() {
    return dcaet.isSetAsync();
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
    return Integer.valueOf(dcaet.getInputQueueScaleout());
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine#setInputQueueScaleout(int)
   */
  public void setInputQueueScaleout(int scaleup) {
    dcaet.setInputQueueScaleout(String.valueOf(scaleup));
  }
/*
  public List<DelegateAnalysisEngine> getDelegates() {
    return null;
  }
*/
  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getConfiguration()
 */
public DelegateConfiguration getConfiguration() {
    // TODO Auto-generated method stub
    return configuration;
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
    return false;
  }
}
