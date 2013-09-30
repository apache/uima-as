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

import org.apache.uima.resourceSpecifier.AsyncAggregateErrorConfigurationType;
import org.apache.uima.resourceSpecifier.CasMultiplierType;
import org.apache.uima.resourceSpecifier.CollectionProcessCompleteErrorsType;
import org.apache.uima.resourceSpecifier.GetMetadataErrorsType;
import org.apache.uima.resourceSpecifier.ProcessCasErrorsType;
import org.apache.uima.resourceSpecifier.RemoteAnalysisEngineType;
import org.apache.uima.resourceSpecifier.SerializerType;
import org.apache.uima.resourceSpecifier.factory.Action;
import org.apache.uima.resourceSpecifier.factory.CasMultiplier;
import org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.GetMetaErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.InputQueue;
import org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine;
import org.apache.uima.resourceSpecifier.factory.RemoteDelegateErrorConfiguration;
import org.apache.uima.resourceSpecifier.factory.SerializationStrategy;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;

// TODO: Auto-generated Javadoc
/**
 * The Class RemoteDelegateEngineImpl.
 */
public class RemoteDelegateEngineImpl implements RemoteDelegateEngine {
  
  /** The draet. */
  private RemoteAnalysisEngineType draet;
  
  /** The cm. */
  private CasMultiplier cm=null;
  
  /** The pcet. */
  private ProcessCasErrorsType pcet;
  
  /** The gmet. */
  private GetMetadataErrorsType gmet;
  
  /** The cpcet. */
  private CollectionProcessCompleteErrorsType cpcet;
  
  /** The input queue. */
  private InputQueue inputQueue;
  
  /** The st. */
  private SerializerType st;
  
  /** The configuration. */
  private DelegateConfiguration configuration;
  
  /**
   * Instantiates a new remote delegate engine impl.
   *
   * @param draet the draet
   * @param rdc the rdc
   * @param context the context
   */
  public RemoteDelegateEngineImpl( RemoteAnalysisEngineType draet, RemoteDelegateConfiguration rdc, ServiceContext context ) {
    this.draet = draet;
    AsyncAggregateErrorConfigurationType aarct = draet.addNewAsyncAggregateErrorConfiguration();
    pcet = aarct.addNewProcessCasErrors();
    gmet = aarct.addNewGetMetadataErrors();
    cpcet = aarct.addNewCollectionProcessCompleteErrors();
    setKey(rdc.getKey());
    setReplyQueueScaleup(rdc.getRemoteReplyQueueScaleout());

    inputQueue = new InputQueueImpl( draet.addNewInputQueue(),((RemoteDelegateConfiguration) rdc).getEndpoint(), ((RemoteDelegateConfiguration) rdc).getBroker(), 1);

    st = draet.addNewSerializer();
    setSerializationStrategy(((RemoteDelegateConfiguration) rdc).getSerialization());
    if ( rdc.isCasMultiplier()) {
      addCasMultiplier(draet.addNewCasMultiplier(), rdc, context);
    }

    addGetMetaErrorHandlingSettings(draet.getAsyncAggregateErrorConfiguration(),rdc.getGetMetaErrorHandlingSettings());
    addProcessErrorHandlingSettings(draet.getAsyncAggregateErrorConfiguration(),rdc.getProcessErrorHandlingSettings());
    addCPCErrorHandlingSettings(draet.getAsyncAggregateErrorConfiguration(), rdc.getCollectionProcessCompleteErrorHandlingSettings());
    configuration = rdc;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getConfiguration()
   */
  public DelegateConfiguration getConfiguration() {
    return configuration;
  }
  
  /**
   * Adds the cas multiplier.
   *
   * @param cmt the cmt
   * @param cdc the cdc
   * @param context the context
   */
  private void addCasMultiplier(CasMultiplierType cmt, DelegateConfiguration cdc, ServiceContext context) {
    cm = new CasMultiplierImpl(cmt, cdc.getCasPoolSize(), cdc.getInitialHeapSize(), cdc.processParentLast());
  }

  /**
   * Adds the get meta error handling settings.
   *
   * @param aarct the aarct
   * @param rdc the rdc
   */
  private void addGetMetaErrorHandlingSettings(AsyncAggregateErrorConfigurationType aarct,GetMetaErrorHandlingSettings rdc ) {
    if ( rdc == null ){
      // add default values
      gmet.setMaxRetries(0);
      gmet.setErrorAction(Action.Terminate.toString());
      gmet.setTimeout(0);  // No Timeout
    } else {
      gmet.setMaxRetries(rdc.getMaxRetries());
      gmet.setErrorAction(rdc.getAction().toString().toLowerCase());
      gmet.setTimeout(rdc.getTimeout());
    }
  }
  
  /**
   * Adds the cpc error handling settings.
   *
   * @param aarct the aarct
   * @param rdc the rdc
   */
  private void addCPCErrorHandlingSettings(AsyncAggregateErrorConfigurationType aarct, CollectionProcessCompleteErrorHandlingSettings rdc ) {
    if ( rdc == null ){
      // add default values
      cpcet.setTimeout(0);
      cpcet.setAdditionalErrorAction(Action.Terminate.toString());
    } else {
      cpcet.setAdditionalErrorAction(rdc.getAction().toString());
      cpcet.setTimeout(rdc.getTimeout());
    }
  }

  /**
   * Adds the process error handling settings.
   *
   * @param aarct the aarct
   * @param rdc the rdc
   */
  private void addProcessErrorHandlingSettings(AsyncAggregateErrorConfigurationType aarct,ProcessErrorHandlingSettings rdc ) {
    if ( rdc == null ){
      // add default values
      pcet.setMaxRetries(0);
      pcet.setThresholdAction(Action.Terminate.toString().toLowerCase());
      pcet.setTimeout(0);  // No Timeout
      pcet.setThresholdCount(0);
      pcet.setThresholdWindow(0);
      pcet.setContinueOnRetryFailure("false");
      
    } else {
      pcet.setMaxRetries(rdc.getMaxRetries());
      pcet.setThresholdAction(rdc.getAction().toString().toLowerCase());
      pcet.setTimeout(rdc.getTimeout());
      pcet.setThresholdCount(rdc.getThresholdCount());
      pcet.setThresholdWindow(rdc.getThresholdWindow());
      pcet.setContinueOnRetryFailure(Boolean.toString(rdc.continueOnRetryFailure()));
    }
  }

  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getKey()
   */
  public String getKey() {
    return draet.getKey();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setKey(java.lang.String)
   */
  public void setKey(String key) {
    draet.setKey(key);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#getReplyQueueScaleup()
   */
  public int getReplyQueueScaleup() {
    return draet.getRemoteReplyQueueScaleout();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#setReplyQueueScaleup(int)
   */
  public void setReplyQueueScaleup(int remoteReplyQueueScaleout) {
    draet.setRemoteReplyQueueScaleout(remoteReplyQueueScaleout);
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
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine#getInputQueue()
   */
  public InputQueue getInputQueue() {
    return inputQueue;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine#setInputQueue(org.apache.uima.resourceSpecifier.factory.InputQueue)
   */
  public void setInputQueue(InputQueue iq) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine#getSerializationStrategy()
   */
  public SerializationStrategy getSerializationStrategy() {
    return SerializationStrategy.valueOf(st.getMethod().toString());
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine#setSerializationStrategy(org.apache.uima.resourceSpecifier.factory.SerializationStrategy)
   */
  public void setSerializationStrategy(SerializationStrategy ss) {
    SerializerType.Method.Enum method = 
      (ss == SerializationStrategy.binary) ? SerializerType.Method.BINARY : SerializerType.Method.XMI;
    st.setMethod(method);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine#getErrorConfiguration()
   */
  public RemoteDelegateErrorConfiguration getErrorConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateEngine#setRemoteDelegateErrorConfiguration(org.apache.uima.resourceSpecifier.factory.RemoteDelegateErrorConfiguration)
   */
  public void setRemoteDelegateErrorConfiguration(RemoteDelegateErrorConfiguration er) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#isRemote()
   */
  public boolean isRemote() {
    return true;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateAnalysisEngine#isAggregate()
   */
  public boolean isAggregate() {
    return false;
  }

}
