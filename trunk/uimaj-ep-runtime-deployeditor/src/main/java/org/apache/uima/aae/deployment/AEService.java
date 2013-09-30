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

import java.util.List;

import org.apache.uima.aae.deployment.impl.NameValue;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.MetaDataObject;
import org.apache.uima.util.InvalidXMLException;

public interface AEService extends MetaDataObject {

    public ResourceSpecifier resolveTopAnalysisEngineDescription(boolean recursive) throws InvalidXMLException;
    public ResourceSpecifier resolveTopAnalysisEngineDescription(ResourceManager aResourceManager, boolean recursive) throws InvalidXMLException;

    public ResourceSpecifier getTopAnalysisEngineDescription() throws InvalidXMLException;
    public ResourceSpecifier getTopAnalysisEngineDescription(ResourceManager aResourceManager) throws InvalidXMLException;
    
    /**
     * @return the analysisEngineDeploymentMetaData
     * @throws InvalidXMLException 
     */
    public AEDeploymentMetaData getAnalysisEngineDeploymentMetaData() throws InvalidXMLException;
    public AEDeploymentMetaData getAnalysisEngineDeploymentMetaData(ResourceManager aResourceManager) throws InvalidXMLException;

    /**
     * @param analysisEngineDeploymentMetaData the analysisEngineDeploymentMetaData to set
     */
    public void setAnalysisEngineDeploymentMetaData(
            AEDeploymentMetaData analysisEngineDeploymentMetaData);

    /**
     * @return the brokerURL
     */
    public String getBrokerURL();

    /**
     * @param brokerURL the brokerURL to set
     */
    public void setBrokerURL(String brokerURL);

    /**
     * @return the endPoint
     */
    public String getEndPoint();

    /**
     * @param endPoint the endPoint to set
     */
    public void setEndPoint(String endPoint);
    
    /**
     * @return the prefetch
     */
    public int getPrefetch();

    /**
     * @param prefetch the prefetch to set
     */
    public void setPrefetch(int prefetch);

    public Import getImportDescriptor();
    
    public void setImportDescriptor(Import importDescriptor);
    
    /**
     * @return the cPlusPlusTopAE
     */
    public boolean isCPlusPlusTopAE();

    /**
     * @param plusPlusTopAE the cPlusPlusTopAE to set
     */
    public void setCPlusPlusTopAE(boolean plusPlusTopAE);

    /**
     * @return the environmentVariables
     */
    public List<NameValue> getEnvironmentVariables();

    /**
     * @param environmentVariables the environmentVariables to set
     */
    public void setEnvironmentVariables(List<NameValue> environmentVariables);
    
    /**
     * @return the customValue
     */
    public String getCustomValue();

    /**
     * @param customValue the customValue to set
     */
    public void setCustomValue(String customValue);


}