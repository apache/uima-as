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

import org.apache.uima.resourceSpecifier.TopLevelAnalysisEngineType;
import org.apache.uima.resourceSpecifier.factory.AggregateDelegateEngine;
import org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.resourceSpecifier.factory.CasMultiplier;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.Delegates;
import org.apache.uima.resourceSpecifier.factory.Scaleout;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.TopLevelAnalysisEngine;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class TopLevelAnalysisEngineImpl.
 */
public class TopLevelAnalysisEngineImpl implements TopLevelAnalysisEngine {
  
  /** The tlea. */
  private TopLevelAnalysisEngineType tlea;
  
  /** The input queue scaleout. */
  private Scaleout inputQueueScaleout = null;
  
  /** The reply queue scaleout. */
  private Scaleout replyQueueScaleout = null;
  
  /** The delegates. */
  private Delegates delegates = null;
  
  /** The ec. */
  private AsyncPrimitiveErrorConfiguration ec = null;
  
  /** The cm. */
  private CasMultiplier cm = null;
  
  /**
   * Instantiates a new top level analysis engine impl.
   *
   * @param tlea the tlea
   * @param context the context
   * @param delegateConfigurations the delegate configurations
   */
  protected TopLevelAnalysisEngineImpl( TopLevelAnalysisEngineType tlea, ServiceContext context, DelegateConfiguration ... delegateConfigurations) {
    this.tlea = tlea;
    if ( context.isCasMultiplier()) {
      cm = new CasMultiplierImpl(tlea.addNewCasMultiplier(),context);
    }
    if ( delegateConfigurations != null && delegateConfigurations.length > 0) {
      tlea.setAsync("true");
      //DelegateColocatedAnalysisEngineType dcaet = dt.addNewAnalysisEngine();
      AggregateDelegateEngine aggregateDelegateEngine = new AggregateDelegateEngineImpl( tlea, "", context, delegateConfigurations);

      delegates = new DelegatesImpl(tlea.addNewDelegates(),aggregateDelegateEngine, context, delegateConfigurations);
    } else {
      tlea.setAsync(String.valueOf(context.isAsync()));
      ec = new AsyncPrimitiveErrorConfigurationImpl(tlea.addNewAsyncPrimitiveErrorConfiguration(),context,true);
      inputQueueScaleout = new ScaleoutImpl(tlea.addNewScaleout(), context);
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#getInputQueueScaleout()
   */
  public Scaleout getInputQueueScaleout() {
    Assert.notNull(inputQueueScaleout);
    return inputQueueScaleout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#setInputQueueScaleout(org.apache.uima.resourceSpecifier.factory.Scaleout)
   */
  public void setInputQueueScaleout(Scaleout scaleup) {
    Assert.notNull(tlea);
    if ( inputQueueScaleout == null ) {
      getInputQueueScaleout().setNumberOfInstances(scaleup.getNumberOfInstances());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#getReplyQueueScaleout()
   */
  public Scaleout getReplyQueueScaleout() {
    Assert.notNull(tlea);
    if ( replyQueueScaleout == null ) {
      //replyQueueScaleout = new ScaleoutImpl(tlea.addNewScaleout());
    }
    return replyQueueScaleout;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#setReplyQueueScaleout(org.apache.uima.resourceSpecifier.factory.Scaleout)
   */
  public void setReplyQueueScaleout(Scaleout replyQueueScaleout) {
    Assert.notNull(tlea);
    tlea.setInternalReplyQueueScaleout(String.valueOf(replyQueueScaleout));
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#getDelegates()
   */
  public Delegates getDelegates() {
    Assert.notNull(delegates);
    return delegates;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#setDelegates(org.apache.uima.resourceSpecifier.factory.Delegates)
   */
  public void setDelegates(Delegates delegates) {
    Assert.notNull(tlea);

  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#getCasMultiplier()
   */
  public CasMultiplier getCasMultiplier() {
    Assert.notNull(tlea);
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.TopLevelAnalysisEngine#getPrimitiveErrorConfiguration()
   */
  public AsyncPrimitiveErrorConfiguration getPrimitiveErrorConfiguration() {
    Assert.notNull(ec);
    return ec;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.TopLevelAnalysisEngine#setPrimitiveErrorConfiguration(org.apache.uima.resourceSpecifier.factory.AsyncPrimitiveErrorConfiguration)
   */
  public void setPrimitiveErrorConfiguration(AsyncPrimitiveErrorConfiguration ec) {
    Assert.notNull(tlea);

  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#getKey()
   */
  public String getKey() {
    Assert.notNull(tlea);
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#setKey(java.lang.String)
   */
  public void setKey(String key) {
    Assert.notNull(tlea);

  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#isAsync()
   */
  public boolean isAsync() {
    Assert.notNull(tlea);
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.AnalysisEngine#setIsAsync()
   */
  public void setIsAsync() {
    Assert.notNull(tlea);

  }
  
  /**
   * Sets the reply queue scaleout.
   */
  public void setReplyQueueScaleout() {
    // TODO Auto-generated method stub
    
  }

}
