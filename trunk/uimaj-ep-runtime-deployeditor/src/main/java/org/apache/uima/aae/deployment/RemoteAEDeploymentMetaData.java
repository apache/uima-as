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

import org.apache.uima.aae.deployment.impl.InputQueue;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLParser;
import org.w3c.dom.Element;



public interface RemoteAEDeploymentMetaData {

    /*************************************************************************/
    
    /**
     * @return the parent of this AEDeploymentMetaData
     */
    public AEDeploymentMetaData getParent ();
    /**
     * @param parent the parent to set
     */
    public void setParent(AEDeploymentMetaData parent);

    public boolean isSet (int i);
    
    public ResourceSpecifier getResourceSpecifier ();
    public void setResourceSpecifier (ResourceSpecifier rs, 
                        ResourceManager aResourceManager, boolean recursive) throws InvalidXMLException;

    public Import getImportedAE();
    public void setImportedAE(Import importedAE);

    
    /* (non-Javadoc)
     * @see org.apache.uima.resource.metadata.impl.MetaDataObject_impl#buildFromXMLElement(org.w3c.dom.Element, org.apache.uima.util.XMLParser, org.apache.uima.util.XMLParser.ParsingOptions)
     */
    public void buildFromXMLElement(Element aElement, XMLParser aParser,
            XMLParser.ParsingOptions aOptions) throws InvalidXMLException;

    /**
     * @return the casMultiplierPoolSize
     */
    public int getCasMultiplierPoolSize();

    /**
     * @param casMultiplierPoolSize the casMultiplierPoolSize to set
     */
    public void setCasMultiplierPoolSize(int casMultiplierPoolSize);

    /**
     * @return the errorConfiguration
     */
    public AsyncAEErrorConfiguration getAsyncAEErrorConfiguration();

    /**
     * @param errorConfiguration the errorConfiguration to set
     */
    public void setErrorConfiguration(AsyncAEErrorConfiguration errorConfiguration);

    /**
     * @return the inputQueue
     */
    public InputQueue getInputQueue();

    /**
     * @param inputQueue the inputQueue to set
     */
    public void setInputQueue(InputQueue inputQueue);

    /**
     * @return the key
     */
    public String getKey();

    /**
     * @param key the key to set
     */
    public void setKey(String key);

    /**
     * @return the replyQueueLocation
     */
    public String getReplyQueueLocation();

    /**
     * @param replyQueueLocation the replyQueueLocation to set
     */
    public void setReplyQueueLocation(String replyQueueLocation);

    /**
     * Get the number of reply queue listeners for remote delegates.
     * 
     * @return int Number of listeners
     */
    public int getRemoteReplyQueueScaleout();

    public void setRemoteReplyQueueScaleout(int concurrentConsumers);
    

    /**
     * @return the serializerMethod
     */
    public String getSerializerMethod();

    /**
     * @param serializerMethod the serializerMethod to set
     */
    public void setSerializerMethod(String serializerMethod);

    /**
     * @return the initialFsHeapSize
     */
    public int getInitialFsHeapSize();

    /**
     * @param initialFsHeapSize the initialFsHeapSize to set
     */
    public void setInitialFsHeapSize(int initialFsHeapSize);
    
}