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
 * The Interface DeploymentDescriptor.
 */
public interface DeploymentDescriptor {
  
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
   * Gets the deployment descriptor.
   *
   * @return the deployment descriptor
   */
  //public UimaASDeploymentDescriptor getDeploymentDescriptor();

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
   * Sets the service analysis engine descriptor.
   *
   * @param aeDescriptorPath the new service analysis engine descriptor
   */
  public void setServiceAnalysisEngineDescriptor(String aeDescriptorPath);
  
  /**
   * Gets the service analysis engine descriptor.
   *
   * @return the service analysis engine descriptor
   */
  public String getServiceAnalysisEngineDescriptor();
  
  /**
   * Sets the endoint.
   *
   * @param endpoint the new endoint
   */
  public void setEndoint(String endpoint);
  
  /**
   * Gets the endpoint.
   *
   * @return the endpoint
   */
  public String getEndpoint();
  
  /**
   * Sets the broker.
   *
   * @param broker the new broker
   */
  public void setBroker(String broker);
  
  /**
   * Gets the broker.
   *
   * @return the broker
   */
  public String getBroker();
  
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
  public void setInitialHeapSize(int initialHeapSize);
  
  /**
   * Gets the initial heap size.
   *
   * @return the initial heap size
   */
  public int getInitialHeapSize();
  
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
  
  public void setProtocol(String protocol);
  
  public String getProtocol();
  
  public void setProvider(String provider);
  
  public String getProvider();
  
  
  
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
