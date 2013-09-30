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

package org.apache.uima.aae.deployment;

import org.apache.uima.resource.metadata.MetaDataObject;

/**

<analysisEngineDeploymentDescription 
      xmlns="http://uima.apache.org/resourceSpecifier">
      
  <!-- the standard (optional) header -->
  <name>[String]</name>
  <description>[String]</description>
  <version>[String]</version>
  <vendor>[String]</vendor>
      
  <deployment protocol="jms" provider="activemq">
    
    <casPool numberOfCASes="xxx" />
      
    <service>                           <!-- must have only 1 -->
      <inputQueue .../>
      
      <topDescriptor .../>         
                                               <!-- 0 or more -->
      <analysisEngine key="key name" async="[true/false]">
         
        <scaleout numberOfInstances="1"/>    <!-- optional  -->
        <casMultiplier poolSize="5"/>        <!-- optional  -->
        <errorConfiguration .../>            <!-- optional  -->
      
        <delegates>      <!-- optional, only for aggregates -->
                                               <!-- 0 or more -->
          <analysisEngine key="key name" async="[true/false]">      
                ...       <!-- optional nested specifications -->
          </analysisEngine>
                . . .     
          <remoteAnalysisEngine key="key name">  <!-- 0 or more -->   
            <inputQueue ... />
            <serializer method="xmi"/>
            <errorConfiguration ... />
          </remoteAnalysisEngine>
                . . .             
        </delegates>
      </analysisEngine>      
    </service> 
  </deployment>
</analysisEngineDeploymentDescription>

 */
public interface AEDeploymentDescription extends MetaDataObject {
    
    /**
     * @return the description
     */
    public String getDescription();

    /**
     * @param description the description to set
     */
    public void setDescription(String description);

    /**
     * @return the name
     */
    public String getName();

    /**
     * @param name the name to set
     */
    public void setName(String name);

    /**
     * @return the vendor
     */
    public String getVendor();

    /**
     * @param vendor the vendor to set
     */
    public void setVendor(String vendor);

    /**
     * @return the version
     */
    public String getVersion();

    /**
     * @param version the version to set
     */
    public void setVersion(String version);

    /**
     * @return the protocol
     */
    public String getProtocol();

    /**
     * @param protocol the protocol to set
     */
    public void setProtocol(String protocol);

    /**
     * @return the provider
     */
    public String getProvider();

    /**
     * @param provider the provider to set
     */
    public void setProvider(String provider);

    public AEService getAeService();

    public void setAeService(AEService aeService);
    
    /**
     * @return the casPoolSize
     */
    public int getCasPoolSize();

    /**
     * @param casPoolSize the casPoolSize to set
     */
    public void setCasPoolSize(int casPoolSize);
    
    public int getInitialFsHeapSize ();
    
    public void setInitialFsHeapSize(int initialSize);

}
