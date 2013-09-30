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

import org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.SerializationStrategy;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class RemoteDelegateConfigurationImpl.
 */
public class RemoteDelegateConfigurationImpl extends DelegateConfigurationImpl implements
        RemoteDelegateConfiguration {
  
  /** The broker url. */
  private String brokerURL;
  
  /** The endpoint. */
  private String endpoint;
  
  /** The remote reply queue scaleout. */
  private int remoteReplyQueueScaleout;
  
  /** The prefetch. */
  private int prefetch=1;
  
  /** The serialization. */
  private SerializationStrategy serialization;
//  public RemoteDelegateConfigurationImpl() {
//  }
  /**
 * Instantiates a new remote delegate configuration impl.
 *
 * @param key the key
 * @param brokerURL the broker url
 * @param endpoint the endpoint
 * @param ser the ser
 * @param errorHandlingSettings the error handling settings
 */
public RemoteDelegateConfigurationImpl(String key, String brokerURL, String endpoint, SerializationStrategy ser,ErrorHandlingSettings ...errorHandlingSettings) {
    super( key, errorHandlingSettings);
    super.setRemote(true);
//    super.setKey(key);
    setBroker(brokerURL);
    setEndpoint(endpoint);
    setSerialization(ser);
//    for( ErrorHandlingSettings setting: errorHandlingSettings ) {
//      if ( setting instanceof GetMetaErrorHandlingSettings ) {
//        setGetMetaErrorHandlingSettings((GetMetaErrorHandlingSettings)setting);
//      } else if( setting instanceof ProcessErrorHandlingSettings ) {
//        setProcessErrorHandlingSettings((ProcessErrorHandlingSettings)setting);
//      }
//    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#setBroker(java.lang.String)
   */
  public void setBroker(String brokerURL) {
    this.brokerURL = brokerURL;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#getBroker()
   */
  public String getBroker() {
    return brokerURL;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#setEndpoint(java.lang.String)
   */
  public void setEndpoint(String endpoint) {
    Assert.notNull(endpoint);
    this.endpoint = endpoint;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#getEndpoint()
   */
  public String getEndpoint() {
    return endpoint;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#setSerialization(org.apache.uima.resourceSpecifier.factory.SerializationStrategy)
   */
  public void setSerialization(SerializationStrategy ser) {
    Assert.notNull(ser);
    this.serialization = ser;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#getSerialization()
   */
  public SerializationStrategy getSerialization() {
    return serialization;
  }
//  public void setGetMetaErrorHandlingSettings(GetMetaErrorHandlingSettings es) {
//    getMetaErrorHandlingSettings = es;
//    
//  }
//  public GetMetaErrorHandlingSettings getGetMetaErrorHandlingSettings() {
//    return getMetaErrorHandlingSettings;
//  }
//  public void setProcessErrorHandlingSettings(ProcessErrorHandlingSettings settings) {
//    processErrorHandlingSettings = settings;
//  }
//  public ProcessErrorHandlingSettings getProcessErrorHandlingSettings() {
//    return processErrorHandlingSettings;
//  }
  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#setRemoteReplyQueueScaleout(int)
 */
public void setRemoteReplyQueueScaleout(int scaleup) {
    remoteReplyQueueScaleout = scaleup;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#getRemoteReplyQueueScaleout()
   */
  public int getRemoteReplyQueueScaleout() {
    if ( remoteReplyQueueScaleout == 0 ) {
      remoteReplyQueueScaleout = 1;
    }
    return remoteReplyQueueScaleout;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#setPrefetch(int)
   */
  public void setPrefetch(int prefetch) {
    this.prefetch = prefetch;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateConfiguration#getPrefetch()
   */
  public int getPrefetch() {
    return prefetch;
  }
}
