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
 * The Interface RemoteDelegateConfiguration.
 */
public interface RemoteDelegateConfiguration extends DelegateConfiguration {
  
  /**
   * Sets the broker.
   *
   * @param brokerURL the new broker
   */
  public void setBroker(String brokerURL);
  
  /**
   * Gets the broker.
   *
   * @return the broker
   */
  public String getBroker();
  
  /**
   * Sets the endpoint.
   *
   * @param endpoint the new endpoint
   */
  public void setEndpoint(String endpoint);
  
  /**
   * Gets the endpoint.
   *
   * @return the endpoint
   */
  public String getEndpoint();
  
  /**
   * Sets the remote reply queue scaleout.
   *
   * @param scaleup the new remote reply queue scaleout
   */
  public void setRemoteReplyQueueScaleout( int scaleup );
  
  /**
   * Gets the remote reply queue scaleout.
   *
   * @return the remote reply queue scaleout
   */
  public int getRemoteReplyQueueScaleout();
  
  /**
   * Sets the serialization.
   *
   * @param ser the new serialization
   */
  public void setSerialization(SerializationStrategy ser);
  
  /**
   * Gets the serialization.
   *
   * @return the serialization
   */
  public SerializationStrategy getSerialization();
  
  /**
   * Sets the prefetch.
   *
   * @param prefetch the new prefetch
   */
  public void setPrefetch( int prefetch);
  
  /**
   * Gets the prefetch.
   *
   * @return the prefetch
   */
  public int getPrefetch();
}
