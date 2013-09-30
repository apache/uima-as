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
package org.apache.uima.resourceSpecifier.factory;

// TODO: Auto-generated Javadoc
/**
 * The Interface DelegateConfiguration.
 */
public interface DelegateConfiguration {
  
  /**
   * Sets the key.
   *
   * @param key the new key
   */
  public void setKey(String key);
  
  /**
   * Gets the key.
   *
   * @return the key
   */
  public String getKey();
  
  /**
   * Sets the remote.
   *
   * @param remote the new remote
   */
  public void setRemote(boolean remote);
  
  /**
   * Checks if is remote.
   *
   * @return true, if is remote
   */
  public boolean isRemote();
  
  /**
   * Checks if is cas multiplier.
   *
   * @return true, if is cas multiplier
   */
  public boolean isCasMultiplier();
  
  /**
   * Sets the cas multiplier.
   *
   * @param cm the new cas multiplier
   */
  public void setCasMultiplier(boolean cm);

  /**
   * Sets the cas pool size.
   *
   * @param casPoolSize the new cas pool size
   */
  public void setCasPoolSize(int casPoolSize);
  
  /**
   * Gets the cas pool size.
   *
   * @return the cas pool size
   */
  public int getCasPoolSize();
  
  /**
   * Sets the initial heap size.
   *
   * @param size the new initial heap size
   */
  public void setInitialHeapSize(int size);
  
  /**
   * Gets the initial heap size.
   *
   * @return the initial heap size
   */
  public int getInitialHeapSize();

  /**
   * Process parent last.
   *
   * @return true, if successful
   */
  public boolean processParentLast();
  
  /**
   * Sets the process parent last.
   *
   * @param processParentLast the new process parent last
   */
  public void setProcessParentLast(boolean processParentLast);
  
  /**
   * Sets the gets the meta error handling settings.
   *
   * @param es the new gets the meta error handling settings
   */
  public void setGetMetaErrorHandlingSettings(GetMetaErrorHandlingSettings es);
  
  /**
   * Gets the gets the meta error handling settings.
   *
   * @return the gets the meta error handling settings
   */
  public GetMetaErrorHandlingSettings getGetMetaErrorHandlingSettings();
  
  /**
   * Sets the process error handling settings.
   *
   * @param settings the new process error handling settings
   */
  public void setProcessErrorHandlingSettings(ProcessErrorHandlingSettings settings);
  
  /**
   * Gets the process error handling settings.
   *
   * @return the process error handling settings
   */
  public ProcessErrorHandlingSettings getProcessErrorHandlingSettings();
  
  /**
   * Sets the collection process complete error handling settings.
   *
   * @param settings the new collection process complete error handling settings
   */
  public void setCollectionProcessCompleteErrorHandlingSettings(CollectionProcessCompleteErrorHandlingSettings settings);
  
  /**
   * Gets the collection process complete error handling settings.
   *
   * @return the collection process complete error handling settings
   */
  public CollectionProcessCompleteErrorHandlingSettings getCollectionProcessCompleteErrorHandlingSettings();
  
}
