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

import java.io.File;

// TODO: Auto-generated Javadoc
/**
 * The Interface UimaASDeploymentDescriptor.
 */
public interface UimaASDeploymentDescriptor {
  
  /** The Constant DEFAULT_BROKER_URL. */
  public static final String DEFAULT_BROKER_URL="tcp://localhost:61616";
  
  /** The Constant TOP_CAS_MULTIPLIER. */
  public static final String TOP_CAS_MULTIPLIER="casMultiplier";
  
  /** The Constant AGGREGATE. */
  public static final String AGGREGATE="aggregate";
  
  /** The Constant NAME. */
  public static final String NAME="name";
  
  /** The Constant DESCRIPTION. */
  public static final String DESCRIPTION="description";
  
  /** The Constant PROTOCOL. */
  public static final String PROTOCOL="protocol";
  
  /** The Constant PROVIDER. */
  public static final String PROVIDER="provider";
  
  /** The Constant ENDPOINT. */
  public static final String ENDPOINT="endpoint";
  
  /** The Constant BROKERURL. */
  public static final String BROKERURL="brokerURL";
  
  /** The Constant PREFETCH. */
  public static final String PREFETCH="prefetch";
  
  /** The Constant INPUTQSCALEOUT. */
  public static final String INPUTQSCALEOUT="scaleout";
  
  /** The Constant ERROR_PROCESS_CAS_THRESHOLDCOUNT. */
  public static final String ERROR_PROCESS_CAS_THRESHOLDCOUNT="thresholdCount";
  
  /** The Constant ERROR_PROCESS_CAS_THRESHOLDWINDOW. */
  public static final String ERROR_PROCESS_CAS_THRESHOLDWINDOW="thresholdWindow";
  
  /** The Constant ERROR_PROCESS_CAS_THRESHOLDACTION. */
  public static final String ERROR_PROCESS_CAS_THRESHOLDACTION="action";
  
  /** The Constant ERROR_CPC_ADDITIONAL_ERROR_ACTION. */
  public static final String ERROR_CPC_ADDITIONAL_ERROR_ACTION="additionalErrorAction";
  
  /** The Constant CASPOOL_CAS_COUNT. */
  public static final String CASPOOL_CAS_COUNT="numberOfCASes";
  
  /** The Constant CASPOOL_INITIAL_FD_HEAP_SIZE. */
  public static final String CASPOOL_INITIAL_FD_HEAP_SIZE="initialFsHeapSize";
  
  /** The Constant PROCESS_PARENT_LAST. */
  public static final String PROCESS_PARENT_LAST="processParentLast";
  
  
  /**
   * Sets the name.
   *
   * @param name the new name
   */
  public void setName(String name);
  
  /**
   * Gets the name.
   *
   * @return the name
   */
  public String getName();
  
  /**
   * Sets the description.
   *
   * @param description the new description
   */
  public void setDescription(String description);
  
  /**
   * Gets the description.
   *
   * @return the description
   */
  public String getDescription();
  
  /**
   * Gets the deployment.
   *
   * @return the deployment
   */
  public Deployment getDeployment();
  
  /**
   * To xml.
   *
   * @return the string
   */
  public String toXML();
  
  /**
   * Save.
   *
   * @param file the file
   * @throws Exception the exception
   */
  public void save(File file) throws Exception;
  
}
