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
import org.apache.uima.resourceSpecifier.factory.GetMetaErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.ProcessErrorHandlingSettings;
import org.apache.uima.resourceSpecifier.factory.RemoteDelegateErrorConfiguration;

// TODO: Auto-generated Javadoc
/**
 * The Class RemoteDelegateErrorConfigurationImpl.
 */
public class RemoteDelegateErrorConfigurationImpl implements RemoteDelegateErrorConfiguration {
  
  /** The get meta error settings. */
  GetMetaErrorHandlingSettings getMetaErrorSettings = null;
  
  /** The process error handling settings. */
  ProcessErrorHandlingSettings processErrorHandlingSettings = null;
  
  /**
   * Instantiates a new remote delegate error configuration impl.
   *
   * @param settings the settings
   */
  public RemoteDelegateErrorConfigurationImpl(ErrorHandlingSettings ...settings ) {
    for( ErrorHandlingSettings errorHandlerSettingType : settings ) {
      if ( errorHandlerSettingType instanceof GetMetaErrorHandlingSettings ) {
        getMetaErrorSettings = (GetMetaErrorHandlingSettings)errorHandlerSettingType;
      } else if ( errorHandlerSettingType instanceof ProcessErrorHandlingSettings ) {
        processErrorHandlingSettings = (ProcessErrorHandlingSettings)errorHandlerSettingType;
      }
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateErrorConfiguration#getGetMetaErrorHandlingSettings()
   */
  public GetMetaErrorHandlingSettings getGetMetaErrorHandlingSettings() {
    return getMetaErrorSettings;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.RemoteDelegateErrorConfiguration#getProcessErrorHandlingSettings()
   */
  public ProcessErrorHandlingSettings getProcessErrorHandlingSettings() {
    return processErrorHandlingSettings;
  }

}
