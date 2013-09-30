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

import org.apache.uima.resourceSpecifier.factory.DeploymentDescriptor;

// TODO: Auto-generated Javadoc
/**
 * The Interface UimaASPrimitiveDeploymentDescriptor.
 */
public interface UimaASPrimitiveDeploymentDescriptor extends DeploymentDescriptor {
  
  /**
   * Sets the scaleup.
   *
   * @param scaleup the new scaleup
   */
  public void setScaleup(int scaleup);
  
  /**
   * Gets the scaleup.
   *
   * @return the scaleup
   */
  public int getScaleup();
  
  /**
   * Gets the process error handling settings.
   *
   * @return the process error handling settings
   */
  public ProcessCasErrors getProcessErrorHandlingSettings();
  
  /**
   * Gets the gets the meta error handling settings.
   *
   * @return the gets the meta error handling settings
   */
  public GetMetaErrorHandlingSettings getGetMetaErrorHandlingSettings();
  
  /**
   * Gets the collection process complete error handling settings.
   *
   * @return the collection process complete error handling settings
   */
  public CollectionProcessCompleteErrors getCollectionProcessCompleteErrorHandlingSettings();
  
}
