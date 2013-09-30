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

import org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.DelegateConfiguration;
import org.apache.uima.resourceSpecifier.factory.ErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.GetMetaErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class DelegateConfigurationImpl.
 */
public abstract class DelegateConfigurationImpl implements DelegateConfiguration {
  
  /** The key. */
  private String key;
  
  /** The remote. */
  private boolean remote;
  
  /** The cas pool size. */
  private int casPoolSize=1;
  
  /** The initial heap size. */
  private int initialHeapSize=2000000;
  
  /** The process parent last. */
  private boolean processParentLast=false;
  
  /** The cas multiplier. */
  private boolean casMultiplier = false;
  
  /** The get meta error handling settings. */
  private GetMetaErrorHandlingSettings getMetaErrorHandlingSettings;
  
  /** The process error handling settings. */
  private ProcessErrorHandlingSettings processErrorHandlingSettings;
  
  /** The collection process complete error handling settings. */
  private CollectionProcessCompleteErrorHandlingSettings collectionProcessCompleteErrorHandlingSettings;
  
  /**
   * Instantiates a new delegate configuration impl.
   *
   * @param key the key
   * @param errorHandlingSettings the error handling settings
   */
  protected DelegateConfigurationImpl(String key, ErrorHandlingSettings ...errorHandlingSettings) {
    this.key = key;
    for( ErrorHandlingSettings setting: errorHandlingSettings ) {
      if ( setting instanceof GetMetaErrorHandlingSettings ) {
        setGetMetaErrorHandlingSettings((GetMetaErrorHandlingSettings)setting);
      } else if( setting instanceof ProcessErrorHandlingSettings ) {
        setProcessErrorHandlingSettings((ProcessErrorHandlingSettings)setting);
      } else if ( setting instanceof CollectionProcessCompleteErrorHandlingSettings ) {
        setCollectionProcessCompleteErrorHandlingSettings((CollectionProcessCompleteErrorHandlingSettings)setting);
      }
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setKey(java.lang.String)
   */
  public void setKey(String key) {
    Assert.notNull(key);
    this.key = key;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#getKey()
   */
  public String getKey() {
    return key;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setGetMetaErrorHandlingSettings(org.apache.uima.resourceSpecifier.factory.GetMetaErrorHandlingSettings)
   */
  public void setGetMetaErrorHandlingSettings(GetMetaErrorHandlingSettings es) {
    getMetaErrorHandlingSettings = es;
    
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#getGetMetaErrorHandlingSettings()
   */
  public GetMetaErrorHandlingSettings getGetMetaErrorHandlingSettings() {
    return getMetaErrorHandlingSettings;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setProcessErrorHandlingSettings(org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings)
   */
  public void setProcessErrorHandlingSettings(ProcessErrorHandlingSettings settings) {
    processErrorHandlingSettings = settings;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#getProcessErrorHandlingSettings()
   */
  public ProcessErrorHandlingSettings getProcessErrorHandlingSettings() {
    return processErrorHandlingSettings;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setCollectionProcessCompleteErrorHandlingSettings(org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrorHandlingSettings)
   */
  public void setCollectionProcessCompleteErrorHandlingSettings(CollectionProcessCompleteErrorHandlingSettings settings) {
    collectionProcessCompleteErrorHandlingSettings = settings;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#getCollectionProcessCompleteErrorHandlingSettings()
   */
  public CollectionProcessCompleteErrorHandlingSettings getCollectionProcessCompleteErrorHandlingSettings() {
    return collectionProcessCompleteErrorHandlingSettings;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#isCasMultiplier()
   */
  public boolean isCasMultiplier() {
    return casMultiplier;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setCasMultiplier(boolean)
   */
  public void setCasMultiplier(boolean cm) {
    casMultiplier = cm;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setRemote(boolean)
   */
  public void setRemote(boolean remote) {
    this.remote = remote;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#isRemote()
   */
  public boolean isRemote() {
    return remote;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setCasPoolSize(int)
   */
  public void setCasPoolSize(int casPoolSize) {
    this.casPoolSize = casPoolSize;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#getCasPoolSize()
   */
  public int getCasPoolSize() {
    return casPoolSize;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setInitialHeapSize(int)
   */
  public void setInitialHeapSize(int size) {
    this.initialHeapSize = size;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#getInitialHeapSize()
   */
  public int getInitialHeapSize() {
    return initialHeapSize;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#processParentLast()
   */
  public boolean processParentLast() {
    return processParentLast;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.DelegateConfiguration#setProcessParentLast(boolean)
   */
  public void setProcessParentLast(boolean processParentLast) {
    this.processParentLast = processParentLast;
  }
}
