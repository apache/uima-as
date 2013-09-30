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

import org.apache.uima.aae.deployment.impl.DeploymentMetaData_Impl;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLParser;
import org.w3c.dom.Element;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;



public interface AsyncAEErrorConfiguration {

    /*************************************************************************/

    public AsyncAEErrorConfiguration clone();
    
    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#buildFromXMLElement(org.w3c.dom.Element, org.apache.uima.util.XMLParser, org.apache.uima.util.XMLParser.ParsingOptions)
     */
    public void buildFromXMLElement(Element aElement, XMLParser aParser,
            XMLParser.ParsingOptions aOptions) throws InvalidXMLException;

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#toXML(org.xml.sax.ContentHandler, boolean)
     */
    public void toXML(ContentHandler aContentHandler,
            boolean aWriteDefaultNamespaceAttribute) throws SAXException;

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getCollectionProcessCompleteErrors()
     */
    public CollectionProcessCompleteErrors getCollectionProcessCompleteErrors();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setCollectionProcessCompleteErrors(com.ibm.uima.aae.deployment.CollectionProcessCompleteErrors)
     */
    public void setCollectionProcessCompleteErrors(
            CollectionProcessCompleteErrors collectionProcessCompleteErrors);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getDescription()
     */
    public String getDescription();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setDescription(java.lang.String)
     */
    public void setDescription(String description);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getGetMetadataErrors()
     */
    public GetMetadataErrors getGetMetadataErrors();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setGetMetadataErrors(com.ibm.uima.aae.deployment.GetMetadataErrors)
     */
    public void setGetMetadataErrors(GetMetadataErrors getMetadataErrors);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getName()
     */
    public String getName();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setName(java.lang.String)
     */
    public void setName(String name);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getProcessCasErrors()
     */
    public ProcessCasErrors getProcessCasErrors();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setProcessCasErrors(com.ibm.uima.aae.deployment.ProcessCasErrors)
     */
    public void setProcessCasErrors(ProcessCasErrors processCasErrors);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getVendor()
     */
    public String getVendor();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setVendor(java.lang.String)
     */
    public void setVendor(String vendor);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getVersion()
     */
    public String getVersion();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setVersion(java.lang.String)
     */
    public void setVersion(String version);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#hasImport()
     */
    public boolean hasImport();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setHasImport(boolean)
     */
    public void setHasImport(boolean hasImport);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#isImportByLocation()
     */
    public boolean isImportByLocation();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setImportByLocation(boolean)
     */
    public void setImportByLocation(boolean importByLocation);

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getImportedDescriptor()
     */
    public String getImportedDescriptor();

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setImportedDescriptor(java.lang.String)
     */
    public void setImportedDescriptor(String importedDescriptor);
    
    /**
     * @return the parentObject
     */
    public DeploymentMetaData_Impl gParentObject();

    /**
     * @param parentObject the parentObject to set
     */
    public void sParentObject(DeploymentMetaData_Impl parentObject);


}