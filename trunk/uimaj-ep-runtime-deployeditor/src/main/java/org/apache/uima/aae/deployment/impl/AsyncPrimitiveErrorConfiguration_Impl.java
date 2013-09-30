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

/**
 * 
 * Project UIMA Tooling
 * 
 * 
 * creation date: Aug 11, 2007, 10:24:35 PM
 * source:  AsyncPrimitiveErrorConfiguration_Impl.java
 */
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

package org.apache.uima.aae.deployment.impl;

import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.internal.util.XMLUtils;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLParser;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;


/**
 * 
 *
 */
public class AsyncPrimitiveErrorConfiguration_Impl extends MetaDataObject_impl
                    implements AEDeploymentConstants, AsyncPrimitiveErrorConfiguration
{
    private static final long serialVersionUID = 397306920555478205L;
    
    protected DeploymentMetaData_Impl parentObject; 

    protected String        name = "";
    protected String        description = "";
    protected String        version = "";
    protected String        vendor = "";

    protected String        importedDescriptor;
    protected boolean       hasImport = false;
    protected boolean       importByLocation = false;

    protected GetMetadataErrors    getMetadataErrors;
    protected ProcessCasErrors     processCasErrors;
    protected CollectionProcessCompleteErrors collectionProcessCompleteErrors;
    
    /*************************************************************************/

    public AsyncPrimitiveErrorConfiguration_Impl() 
    {
        processCasErrors = new ProcessCasErrors_Impl(this);
        collectionProcessCompleteErrors = new CollectionProcessCompleteErrors_Impl(this);       
    }
    
    public AsyncAEErrorConfiguration clone()
    {
        AsyncAEErrorConfiguration clone = new AsyncPrimitiveErrorConfiguration_Impl();
        clone.setName(getName());
        clone.setDescription(getDescription());
        clone.setVersion(getVersion());
        clone.setVendor(getVendor());

        if (processCasErrors != null) {
            clone.setProcessCasErrors(processCasErrors.clone(clone));
        }
        if (collectionProcessCompleteErrors != null) {
            clone.setCollectionProcessCompleteErrors(collectionProcessCompleteErrors.clone(clone));
        }
        return clone;
    }
    
    /*************************************************************************/

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#buildFromXMLElement(org.w3c.dom.Element, org.apache.uima.util.XMLParser, org.apache.uima.util.XMLParser.ParsingOptions)
     */
    public void buildFromXMLElement (Element aElement, XMLParser aParser, XMLParser.ParsingOptions aOptions)
                            throws InvalidXMLException
    {
        String val;
        
        NodeList childNodes = aElement.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node curNode = childNodes.item(i);
            if (curNode instanceof Element) {
                Element elem = (Element)curNode;
                if (TAG_NAME.equalsIgnoreCase(elem.getTagName())) {
                    setName(XMLUtils.getText(elem));

                } else if (TAG_DESCRIPTION.equalsIgnoreCase(elem.getTagName())) {
                    setDescription(XMLUtils.getText(elem));

                } else if (TAG_VERSION.equalsIgnoreCase(elem.getTagName())) {
                    setVersion(XMLUtils.getText(elem));

                } else if (TAG_VENDOR.equalsIgnoreCase(elem.getTagName())) {
                    setVendor(XMLUtils.getText(elem));

                } else if (TAG_IMPORT.equalsIgnoreCase(elem.getTagName())) {
                    String topDescr = "";
                    if (elem.getAttribute(TAG_ATTR_LOCATION) != null) {
                        // Trace.err("An import location=" + ((Element)n).getAttribute(TAG_ATTR_LOCATION));
                        topDescr = elem.getAttribute(TAG_ATTR_LOCATION);
                        setImportByLocation(true);
                    } else if (elem.getAttribute(TAG_ATTR_NAME) != null) {
                        topDescr = elem.getAttribute(TAG_ATTR_NAME);
                    } else {
                        throw new InvalidXMLException(
                                InvalidXMLException.UNKNOWN_ELEMENT,
                                new Object[]{elem.getTagName()});
                    }
                    setHasImport(true);
                    setImportedDescriptor(topDescr);

                } else if (TAG_PROCESS_CAS_ERRORS.equalsIgnoreCase(elem.getTagName())) {
                  setAttributesForProcessCasErrors(elem);

                } else if (TAG_COLLECTION_PROCESS_COMPLETE_ERRORS.equalsIgnoreCase(elem.getTagName())) {
                  setAttributesForCollectionProcessCompleteErrors(elem);

                } else {
                    throw new InvalidXMLException(
                            InvalidXMLException.UNKNOWN_ELEMENT,
                            new Object[]{elem.getTagName()});
                }
            }
        }
    }
    
    protected void setAttributesForProcessCasErrors (Element aElement) throws InvalidXMLException {
      NamedNodeMap map = aElement.getAttributes();
      if (map != null) {
        for (int i=0; i<map.getLength(); ++i) {
          Node item = map.item(i);
          String name = item.getNodeName();
          String val = item.getNodeValue();
          if (val == null) {
            val = "";
          } else {
            val = val.trim();
          }
          if (TAG_ATTR_THRESHOLD_COUNT.equalsIgnoreCase(name)) {
            if (val.length() > 0) {
              processCasErrors.setThresholdCount(Integer.parseInt(val));
            }
          } else if (TAG_ATTR_THRESHOLD_WINDOW.equalsIgnoreCase(name)) {
            if (val.length() > 0) {
              processCasErrors.setThresholdWindow(Integer.parseInt(val));
            }
          } else if (TAG_ATTR_THRESHOLD_ACTION.equalsIgnoreCase(name)) {
            if (val.length() > 0) {
              processCasErrors.setThresholdAction(val);
            }
           
          } else {
            throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT, new Object[]{name});
          }
        }
      }
    }
    
    protected void setAttributesForCollectionProcessCompleteErrors (Element aElement) throws InvalidXMLException {
      NamedNodeMap map = aElement.getAttributes();
      if (map != null) {
        for (int i=0; i<map.getLength(); ++i) {
          Node item = map.item(i);
          String name = item.getNodeName();
          String val = item.getNodeValue();
          if (val == null) {
            val = "";
          } else {
            val = val.trim();
          }
          if (TAG_ATTR_TIMEOUT.equalsIgnoreCase(name)) {
            if (val.length() > 0) {
              collectionProcessCompleteErrors.setTimeout(Integer.parseInt(val));
            }
          } else if (TAG_ATTR_ADDITIONAL_ERROR_ACTION.equalsIgnoreCase(name)) {
            if (val.length() > 0) {
              collectionProcessCompleteErrors.setAdditionalErrorAction(val);
            }
          } else {
            throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT, new Object[]{name});
          }
        }
      }
    }

    /*************************************************************************/
    
    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#toXML(org.xml.sax.ContentHandler, boolean)
     */
    public void toXML(ContentHandler aContentHandler, boolean aWriteDefaultNamespaceAttribute)
        throws SAXException
    {
        AttributesImpl attrs = new AttributesImpl();
        aContentHandler.startElement("", TAG_ASYNC_PRIMITIVE_ERROR_CONFIGURATION,
                TAG_ASYNC_PRIMITIVE_ERROR_CONFIGURATION, attrs);
/*        
        // <name>  </name>
        aContentHandler.startElement("", "", "name", attrs);
        String valStr = getName();
        aContentHandler.characters(valStr.toCharArray(), 0, valStr.length());
        aContentHandler.endElement("", "", "name");

        // <description>  </description>
        aContentHandler.startElement("", "", TAG_DESCRIPTION,attrs);
        valStr = getDescription();
        aContentHandler.characters(valStr.toCharArray(), 0, valStr.length());
        aContentHandler.endElement("", "", TAG_DESCRIPTION);

        // <version>  </version>
        aContentHandler.startElement("", "","version",attrs);
        valStr = getVersion();
        aContentHandler.characters(valStr.toCharArray(), 0, valStr.length());
        aContentHandler.endElement("", "", "version");

        // <vendor>  </vendor>
        aContentHandler.startElement("", "", "vendor", attrs);
        valStr = getVendor();
        aContentHandler.characters(valStr.toCharArray(), 0, valStr.length());
        aContentHandler.endElement("", "", "vendor");
*/
        // <import location=.../>
        if ( hasImport ) {
            if (isImportByLocation()) {
                attrs.addAttribute("", TAG_ATTR_LOCATION, TAG_ATTR_LOCATION,
                    null, getImportedDescriptor());
            } else {
                attrs.addAttribute("", TAG_ATTR_NAME, TAG_ATTR_NAME,
                        null, getImportedDescriptor());
            }
            aContentHandler.startElement("", TAG_IMPORT, TAG_IMPORT, attrs);
            aContentHandler.endElement("", "", TAG_IMPORT);
            attrs.clear();
        }                
        
        // <ProcessCasErrors ...>
        if (processCasErrors != null) {            
            attrs.addAttribute("", TAG_ATTR_THRESHOLD_COUNT, TAG_ATTR_THRESHOLD_COUNT,
                    null, Integer.toString(processCasErrors.getThresholdCount()));
            attrs.addAttribute("", TAG_ATTR_THRESHOLD_WINDOW, TAG_ATTR_THRESHOLD_WINDOW,
                    null, Integer.toString(processCasErrors.getThresholdWindow()));
            attrs.addAttribute("", TAG_ATTR_THRESHOLD_ACTION, TAG_ATTR_THRESHOLD_ACTION,
                    null, processCasErrors.getThresholdAction());
            
            aContentHandler.startElement("", TAG_PROCESS_CAS_ERRORS, TAG_PROCESS_CAS_ERRORS, attrs);
            aContentHandler.endElement("", "", TAG_PROCESS_CAS_ERRORS);
            attrs.clear();
        }
        
        // <CollectionProcessCompleteErrors ...>
        if (collectionProcessCompleteErrors != null) {
            attrs.addAttribute("", TAG_ATTR_TIMEOUT, TAG_ATTR_TIMEOUT,
                    null, Integer.toString(collectionProcessCompleteErrors.getTimeout()));
            attrs.addAttribute("", TAG_ATTR_ADDITIONAL_ERROR_ACTION, TAG_ATTR_ADDITIONAL_ERROR_ACTION,
                    null, collectionProcessCompleteErrors.getAdditionalErrorAction());
            
            aContentHandler.startElement("", TAG_COLLECTION_PROCESS_COMPLETE_ERRORS, 
                    TAG_COLLECTION_PROCESS_COMPLETE_ERRORS, attrs);
            aContentHandler.endElement("", "", TAG_COLLECTION_PROCESS_COMPLETE_ERRORS);
            attrs.clear();
        }
        
        aContentHandler.endElement("", "", TAG_ASYNC_PRIMITIVE_ERROR_CONFIGURATION);

    }
    

    @Override
    protected XmlizationInfo getXmlizationInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getCollectionProcessCompleteErrors()
     */
    public CollectionProcessCompleteErrors getCollectionProcessCompleteErrors() {
        return collectionProcessCompleteErrors;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setCollectionProcessCompleteErrors(com.ibm.uima.aae.deployment.CollectionProcessCompleteErrors)
     */
    public void setCollectionProcessCompleteErrors(
            CollectionProcessCompleteErrors collectionProcessCompleteErrors) {
        this.collectionProcessCompleteErrors = collectionProcessCompleteErrors;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getDescription()
     */
    public String getDescription() {
        return description;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setDescription(java.lang.String)
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getGetMetadataErrors()
     */
    public GetMetadataErrors getGetMetadataErrors() {
        return getMetadataErrors;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setGetMetadataErrors(com.ibm.uima.aae.deployment.GetMetadataErrors)
     */
    public void setGetMetadataErrors(GetMetadataErrors getMetadataErrors) {
        this.getMetadataErrors = getMetadataErrors;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getName()
     */
    public String getName() {
        return name;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setName(java.lang.String)
     */
    public void setName(String name) {
        this.name = name;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getProcessCasErrors()
     */
    public ProcessCasErrors getProcessCasErrors() {
        return processCasErrors;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setProcessCasErrors(com.ibm.uima.aae.deployment.ProcessCasErrors)
     */
    public void setProcessCasErrors(ProcessCasErrors processCasErrors) {
        this.processCasErrors = processCasErrors;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getVendor()
     */
    public String getVendor() {
        return vendor;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setVendor(java.lang.String)
     */
    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getVersion()
     */
    public String getVersion() {
        return version;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setVersion(java.lang.String)
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#hasImport()
     */
    public boolean hasImport() {
        return hasImport;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setHasImport(boolean)
     */
    public void setHasImport(boolean hasImport) {
        this.hasImport = hasImport;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#isImportByLocation()
     */
    public boolean isImportByLocation() {
        return importByLocation;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setImportByLocation(boolean)
     */
    public void setImportByLocation(boolean importByLocation) {
        this.importByLocation = importByLocation;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#getImportedDescriptor()
     */
    public String getImportedDescriptor() {
        return importedDescriptor;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration#setImportedDescriptor(java.lang.String)
     */
    public void setImportedDescriptor(String importedDescriptor) {
        this.importedDescriptor = importedDescriptor;
    }

    /**
     * @return the parentObject
     */
    public DeploymentMetaData_Impl gParentObject() {
      return parentObject;
    }

    /**
     * @param parentObject the parentObject to set
     */
    public void sParentObject(DeploymentMetaData_Impl parentObject) {
      this.parentObject = parentObject;
    }

    /*************************************************************************/


}
