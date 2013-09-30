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
 * The Interface ServiceContext.
 */
public interface ServiceContext {
  
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
   * Sets the descriptor.
   *
   * @param descriptor the new descriptor
   */
  public void setDescriptor(String descriptor);
  
  /**
   * Gets the descriptor.
   *
   * @return the descriptor
   */
  public String getDescriptor();
  
  /**
   * Sets the async.
   *
   * @param async the new async
   */
  public void setAsync(boolean async);
  
  /**
   * Checks if is async.
   *
   * @return true, if is async
   */
  public boolean isAsync();
  
  /**
   * Sets the protocol.
   *
   * @param protocol the new protocol
   */
  public void setProtocol(String protocol);
  
  /**
   * Gets the protocol.
   *
   * @return the protocol
   */
  public String getProtocol();
  
  /**
   * Sets the provider.
   *
   * @param provider the new provider
   */
  public void setProvider(String provider);
  
  /**
   * Gets the provider.
   *
   * @return the provider
   */
  public String getProvider();
  
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
   * Sets the broker url.
   *
   * @param brokerURL the new broker url
   */
  public void setBrokerURL(String brokerURL);
  
  /**
   * Gets the broker url.
   *
   * @return the broker url
   */
  public String getBrokerURL();
  
  /**
   * Sets the prefetch.
   *
   * @param prefetch the new prefetch
   */
  public void setPrefetch(int prefetch);
  
  /**
   * Gets the prefetch.
   *
   * @return the prefetch
   */
  public int getPrefetch();
  
  /**
   * Sets the scaleup.
   *
   * @param scaleUp the new scaleup
   */
  public void setScaleup(int scaleUp);
  
  /**
   * Gets the scaleup.
   *
   * @return the scaleup
   */
  public int getScaleup();
  
  /**
   * Sets the cas multiplier.
   *
   * @param casMultiplier the new cas multiplier
   */
  public void setCasMultiplier(boolean casMultiplier);
  
  /**
   * Checks if is cas multiplier.
   *
   * @return true, if is cas multiplier
   */
  public boolean isCasMultiplier();
  
  /**
   * Sets the aggregate.
   *
   * @param aggregate the new aggregate
   */
  public void setAggregate(boolean aggregate);
  
  /**
   * Checks if is aggregate.
   *
   * @return true, if is aggregate
   */
  public boolean isAggregate();
  
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
   * @param initialHeapSize the new initial heap size
   */
  public void setInitialHeapSize( int initialHeapSize );
  
  /**
   * Gets the initial heap size.
   *
   * @return the initial heap size
   */
  public int getInitialHeapSize();
  
  /**
   * Sets the process parent last.
   *
   * @param processParentLast the new process parent last
   */
  public void setProcessParentLast( boolean processParentLast);
  
  /**
   * Process parent last.
   *
   * @return true, if successful
   */
  public boolean processParentLast();
  
  /**
   * Sets the process error threshold count.
   *
   * @param thresholdCount the new process error threshold count
   */
  public void setProcessErrorThresholdCount(int thresholdCount);
  
  /**
   * Gets the process error threshold count.
   *
   * @return the process error threshold count
   */
  public int getProcessErrorThresholdCount();
  
  /**
   * Sets the process error threshold window.
   *
   * @param thresholdWindow the new process error threshold window
   */
  public void setProcessErrorThresholdWindow(int thresholdWindow);
  
  /**
   * Gets the process error threshold window.
   *
   * @return the process error threshold window
   */
  public int getProcessErrorThresholdWindow();
  
  /**
   * Sets the process error threshold action.
   *
   * @param action the new process error threshold action
   */
  public void setProcessErrorThresholdAction(Action action);
  
  /**
   * Gets the process error threshold action.
   *
   * @return the process error threshold action
   */
  public Action getProcessErrorThresholdAction();
  
  /**
   * Sets the cp c additional action.
   *
   * @param action the new cp c additional action
   */
  public void setCpCAdditionalAction( Action action);
  
  /**
   * Gets the cp c additional action.
   *
   * @return the cp c additional action
   */
  public Action getCpCAdditionalAction();
  
}
