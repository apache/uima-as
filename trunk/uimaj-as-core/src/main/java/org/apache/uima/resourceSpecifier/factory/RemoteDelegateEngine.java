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
 * The Interface RemoteDelegateEngine.
 */
public interface RemoteDelegateEngine extends DelegateAnalysisEngine {
  
  /**
   * Gets the input queue.
   *
   * @return the input queue
   */
  public InputQueue getInputQueue();
  
  /**
   * Sets the input queue.
   *
   * @param iq the new input queue
   */
  public void setInputQueue(InputQueue iq);
  
  /**
   * Gets the serialization strategy.
   *
   * @return the serialization strategy
   */
  public SerializationStrategy getSerializationStrategy();
  
  /**
   * Sets the serialization strategy.
   *
   * @param ss the new serialization strategy
   */
  public void setSerializationStrategy(SerializationStrategy ss);
  
  /**
   * Gets the error configuration.
   *
   * @return the error configuration
   */
  public RemoteDelegateErrorConfiguration getErrorConfiguration();
  
  /**
   * Sets the remote delegate error configuration.
   *
   * @param er the new remote delegate error configuration
   */
  public void setRemoteDelegateErrorConfiguration(RemoteDelegateErrorConfiguration er);
  
}
