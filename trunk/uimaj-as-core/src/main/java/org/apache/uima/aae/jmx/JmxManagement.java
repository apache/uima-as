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

package org.apache.uima.aae.jmx;

import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * 
 * Jmx Management interface for registering and unregistering MBeans.
 * 
 */
public interface JmxManagement {
  /**
   * Initializes instnace of this interface using provided properties
   * 
   * @param anInitMap
   *          - properties required during initialization
   * @throws Exception
   */
  public void initialize(Map anInitMap) throws Exception;

  /**
   * Returns instance of an MBeanServer
   * 
   * @return
   */
  public MBeanServer getMBeanServer();

  /**
   * Returns configured Jmx Domain
   * 
   * @return
   */
  public String getJmxDomain();

  /**
   * Sets the Jmx Domain
   * 
   * @param aJmxDomain
   */
  public void setJmxDomain(String aJmxDomain);

  /**
   * Registers given MBean with MBeanServer
   * 
   * @param anMBean
   *          - instance of MBean to register
   * @param anMBeanName
   *          - name of the MBean
   * @throws Exception
   */
  public void registerMBean(Object anMBean, ObjectName anMBeanName) throws Exception;

  /**
   * Removed an MBean from the MBeanServer registry
   * 
   * @param anMBeanName
   */
  public void unregisterMBean(ObjectName anMBeanName);

  /**
   * Performs cleanup when object goes out of scope
   * 
   * @throws Exception
   */
  public void destroy() throws Exception;
}
