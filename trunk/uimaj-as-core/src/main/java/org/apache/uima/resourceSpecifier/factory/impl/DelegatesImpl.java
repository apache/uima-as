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
import org.apache.uima.resourceSpecifier.CasMultiplierType;
import org.apache.uima.resourceSpecifier.DelegatesType;
import org.apache.uima.resourceSpecifier.RemoteAnalysisEngineType;
import org.apache.uima.resourceSpecifier.factory.AggregateDelegateEngine;
import org.apache.uima.resourceSpecifier.factory.ColocatedDelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.ColocatedDelegateEngine;
import org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.Delegates;
import org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class DelegatesImpl.
 */
public class DelegatesImpl implements Delegates {
  
  /** The dt. */
  private DelegatesType dt;
  
  /** The aggregate engine. */
  private AggregateDelegateEngine aggregateEngine;
  
  /**
   * Instantiates a new delegates impl.
   *
   * @param dt the dt
   * @param aggregateEngine the aggregate engine
   * @param context the context
   * @param delegateConfigurations the delegate configurations
   */
  public DelegatesImpl(DelegatesType dt, AggregateDelegateEngine aggregateEngine, ServiceContext context, DelegateConfiguration ... delegateConfigurations) {
    this.dt = dt;
    if ( delegateConfigurations != null ) {
      this.aggregateEngine = aggregateEngine;
      List<DelegateConfiguration> delegateConfigurationList = Arrays.asList(delegateConfigurations);
      addDelegateEngine(aggregateEngine, delegateConfigurationList, dt, context);
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Delegates#getDelegates()
   */
  public List<DelegateAnalysisEngine> getDelegates() {
    return aggregateEngine.getDelegates();
  }
  /*
  private void addGetMetaErrorHandlingSettings(AsyncAggregateErrorConfigurationType aarct,RemoteDelegateConfiguration rdc ) {
    GetMetadataErrorsType gmet = aarct.addNewGetMetadataErrors();
    if ( rdc.getGetMetaErrorHandlingSettings() == null ){
      // add default values
      gmet.setMaxRetries(0);
      gmet.setErrorAction(Action.Terminate.toString());
      gmet.setTimeout(0);  // No Timeout
    } else {
      gmet.setMaxRetries(rdc.getGetMetaErrorHandlingSettings().getMaxRetries());
      gmet.setErrorAction(rdc.getGetMetaErrorHandlingSettings().getAction().toString().toLowerCase());
      gmet.setTimeout(rdc.getGetMetaErrorHandlingSettings().getTimeout());
    }
  }

  private void addProcessErrorHandlingSettings(AsyncAggregateErrorConfigurationType aarct,RemoteDelegateConfiguration rdc ) {
    ProcessCasErrorsType pcet = aarct.addNewProcessCasErrors();
    if ( rdc.getProcessErrorHandlingSettings() == null ){
      // add default values
      pcet.setMaxRetries(0);
      pcet.setThresholdAction(Action.Terminate.toString().toLowerCase());
      pcet.setTimeout(0);  // No Timeout
      pcet.setThresholdCount(0);
      pcet.setThresholdWindow(0);
      pcet.setContinueOnRetryFailure("false");
      
    } else {
      pcet.setMaxRetries(rdc.getProcessErrorHandlingSettings().getMaxRetries());
      pcet.setThresholdAction(rdc.getProcessErrorHandlingSettings().getAction().toString().toLowerCase());
      pcet.setTimeout(rdc.getProcessErrorHandlingSettings().getTimeout());
      pcet.setThresholdCount(rdc.getProcessErrorHandlingSettings().getThresholdCount());
      pcet.setThresholdWindow(rdc.getProcessErrorHandlingSettings().getThresholdWindow());
      pcet.setContinueOnRetryFailure(Boolean.toString(rdc.getProcessErrorHandlingSettings().continueOnRetryFailure()));
    }
  }
  */
  /**
   * Adds the cas multiplier.
   *
   * @param cmt the cmt
   * @param cdc the cdc
   */
  private void addCasMultiplier(CasMultiplierType cmt, DelegateConfiguration cdc) {
    cmt.setInitialFsHeapSize(String.valueOf(cdc.getInitialHeapSize()));
    cmt.setPoolSize(cdc.getCasPoolSize());
    cmt.setProcessParentLast(String.valueOf(cdc.processParentLast()));
  }
  
  /**
   * Adds the delegate engine.
   *
   * @param containingAggregate the containing aggregate
   * @param delegates the delegates
   * @param dt the dt
   * @param context the context
   */
  private void addDelegateEngine(AggregateDelegateEngine containingAggregate, List<DelegateConfiguration> delegates, DelegatesType dt, ServiceContext context) {
      for( DelegateConfiguration delegate : delegates ) {
        if ( delegate.isRemote() ) {
          RemoteAnalysisEngineType draet = dt.addNewRemoteAnalysisEngine();
          draet.setKey(delegate.getKey());
          containingAggregate.addDelegate( new RemoteDelegateEngineImpl( draet, (RemoteDelegateConfiguration)delegate, context) );
        } else {
          AnalysisEngineType dcaet = dt.addNewAnalysisEngine();
          dcaet.setKey(delegate.getKey());
          //  colocated delegate, either nested aggregate or primitive
          if ( ((ColocatedDelegateConfiguration)delegate).isAggregate() ) {
            AggregateDelegateEngine a1 = new AggregateDelegateEngineImpl( dcaet, delegate.getKey(), context, (ColocatedDelegateConfiguration)delegate);
            containingAggregate.addDelegate( a1 );
            //  recursive call to handle nested aggregate delegates
            addDelegateEngine(a1, ((ColocatedDelegateConfiguration)delegate).getDelegates(), dcaet.addNewDelegates(), context);
          } else {
            // primitive
            containingAggregate.addDelegate( new ColocatedDelegateEngineImpl( dcaet, (ColocatedDelegateConfiguration)delegate, context) );
          }
        }
      }
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Delegates#addDelegate(org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine)
   */
  public void addDelegate(DelegateAnalysisEngine dae) {
    Assert.notNull(dae);
    Assert.notNull(dt);
    //  Make sure we are adding expected DelegateEngine type. Its either Remote or Colocated (Primitive or Aggregate)
    Assert.isTrue(dae instanceof RemoteDelegateEngine || dae instanceof ColocatedDelegateEngine, "Invalid DelegateEngine Type. Expected either RemoteDelegateEngine or ColocatedDelegateEngine. Provided type is invalid:"+dae.getClass().getName());
    aggregateEngine.addDelegate(dae);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Delegates#getRemoteDelegates()
   */
  public List<RemoteDelegateEngine> getRemoteDelegates() {
    List<RemoteDelegateEngine> remoteDelegates = new 
      ArrayList<RemoteDelegateEngine>();
    for( DelegateAnalysisEngine dea : aggregateEngine.getDelegates() ) {
      if ( dea instanceof RemoteDelegateEngine ) {
        remoteDelegates.add((RemoteDelegateEngine)dea);
      }
    }
    return remoteDelegates;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Delegates#getColocatedDelegateEngine()
   */
  public List<ColocatedDelegateEngine> getColocatedDelegateEngine() {
    List<ColocatedDelegateEngine> colocatedDelegates = new 
    ArrayList<ColocatedDelegateEngine>();
  for( DelegateAnalysisEngine dea : aggregateEngine.getDelegates() ) {
    if ( dea instanceof ColocatedDelegateEngine ) {
      colocatedDelegates.add((ColocatedDelegateEngine)dea);
    }
  }
  return colocatedDelegates;
  }

}
