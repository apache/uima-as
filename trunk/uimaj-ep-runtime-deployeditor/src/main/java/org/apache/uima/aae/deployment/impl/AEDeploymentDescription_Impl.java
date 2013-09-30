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
 * creation date: Apr 3, 2007, 6:00:01 PM
 * source:  AEDeploymentDescription.java
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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.AEService;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.internal.util.XMLUtils;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.OperationalProperties;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.PropertyXmlInfo;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLParser;
import org.apache.uima.util.XMLSerializer;
import org.apache.uima.util.XMLizable;
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
public class AEDeploymentDescription_Impl extends MetaDataObject_impl 
implements AEDeploymentDescription, AEDeploymentConstants
{

  protected String        name = "";
  protected String        description = "";
  protected String        version = "";
  protected String        vendor = "";

  protected String        protocol = "";
  protected String        provider = "";

  protected int           casPoolSize = DEFAULT_CAS_POOL_SIZE;
  protected int           initialFsHeapSize = DEFAULT_CAS_INITIAL_HEAP_SIZE;
  protected AEService     aeService;

  /*************************************************************************/

  public AEDeploymentDescription_Impl() {
    aeService = new AEService_Impl();
  }

  /*************************************************************************/

  /**
   * Overridden to provide custom XMLization.
   * @see org.apache.uima.util.XMLizable#buildFromXMLElement(org.w3c.dom.Element, org.apache.uima.util.XMLParser)
   */
  public void buildFromXMLElement (Element aElement, XMLParser aParser, XMLParser.ParsingOptions aOptions)
  throws InvalidXMLException
  {
    // Trace.err();        
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

        } else if (TAG_DEPLOYMENT.equalsIgnoreCase(elem.getTagName())) {
          buildTopDeployment(elem, aParser, aOptions); 

        } else {
          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                  new Object[]{elem.getTagName()});
        }
      }
    }
  }

  public String toString ()
  {
    StringWriter w = new StringWriter();
    try {
      toXML(w);
      return w.toString();
    } catch (SAXException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return "";
  }

  /**
   * 
   *   
   * <deployment protocol="jms" provider="activemq">
   *   <casPool numberOfCASes="5" initialFsHeapSize /> 
   *   
   *   <service>
   *     <inputQueue endpoint="MeetingDetectorTaeQueue" brokerURL="tcp://localhost:61616"/>
   *     <topDescriptor>
   *       <import location="../../descriptors/tutorial/ex4/MeetingDetectorTAE.xml"/> 
   *     </topDescriptor>
   *   
   *    <!-- 0 or ???more??? AEDeploymentMetaData -->
   *    <analysisEngine key="key name" async="[true/false]">
   *      <!-- ... -->
   *    </analysisEngine>
   *    
   *   </service>
   * </deployment>
   *
   * 
   * @param aElement
   * @param aParser
   * @throws InvalidXMLException
   * @return void
   */
  private void buildTopDeployment (Element aElement, XMLParser aParser,
          XMLParser.ParsingOptions aOptions)
  throws InvalidXMLException
  {
    // Check for "protocol" and "provider" attributes
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
        if (AEDeploymentConstants.TAG_ATTR_PROTOCOL.equalsIgnoreCase(name)) {
          // Set "protocol = ..." attribute
          setProtocol(val);
        } else if (AEDeploymentConstants.TAG_ATTR_PROVIDER.equalsIgnoreCase(name)) {
          // Set "provider =" attribute
          setProvider(val);
        } else {
          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                  new Object[]{name});
        }
      }
    }

    // Has "protocol" and "provider" attributes
    if (getProtocol() == null) {
        throw new InvalidXMLException(InvalidXMLException.ELEMENT_NOT_FOUND,
                new Object[]{AEDeploymentConstants.TAG_ATTR_PROTOCOL, AEDeploymentConstants.TAG_DEPLOYMENT});
    }
    if (getProvider() == null) {
      throw new InvalidXMLException(InvalidXMLException.ELEMENT_NOT_FOUND,
              new Object[]{AEDeploymentConstants.TAG_ATTR_PROVIDER, AEDeploymentConstants.TAG_DEPLOYMENT});
    }

    NodeList childNodes = aElement.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node curNode = childNodes.item(i);
      if (!(curNode instanceof Element)) {
        continue;
      }
      Element elem = (Element)curNode;
      if (TAG_CASPOOL.equalsIgnoreCase(elem.getTagName())) {
        String val = DDParserUtil.checkAndGetAttributeValue(TAG_CASPOOL, TAG_ATTR_NUMBER_OF_CASES, elem, true);
        if (val != null && val.trim().length() > 0) {
          try {
            casPoolSize = Integer.parseInt(val);
          } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                    new Object[] { TAG_CASPOOL }, e);
          }
        }
        
        // Check for Optional "initialFsHeapSize"
        val = DDParserUtil.checkAndGetAttributeValue(TAG_CASPOOL, TAG_ATTR_INIT_SIZE_OF_CAS_HEAP, elem, false);
        if (val != null && val.trim().length() > 0) {
          try {
            initialFsHeapSize = Integer.parseInt(val);
          } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                    new Object[] { TAG_ATTR_INIT_SIZE_OF_CAS_HEAP }, e);
          }
        }

      } else if (TAG_SERVICE.equalsIgnoreCase(elem.getTagName())) {
        aeService = (AEService) aParser.buildObject(elem, aOptions);

      } else {
        throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                new Object[]{elem.getTagName()});
      }
    } // for
  }

  /**
   * Overridden to provide custom XML representation.
   * @see org.apache.uima.util.XMLizable#toXML(ContentHandler)
   */
  public void toXML(ContentHandler aContentHandler, boolean aWriteDefaultNamespaceAttribute)
  throws SAXException
  {
    AttributesImpl attrs = new AttributesImpl();
    aContentHandler.startElement(getXmlizationInfo().namespace,
            "analysisEngineDeploymentDescription","analysisEngineDeploymentDescription",attrs);

    // <name>  </name>
    aContentHandler.startElement("", "", "name", attrs);
    String valStr = getName();
    aContentHandler.characters(valStr.toCharArray(), 0, valStr.length());
    aContentHandler.endElement("", "", "name");

    // <description>  </description>
    aContentHandler.startElement("", "", TAG_DESCRIPTION,attrs);
    valStr = multiLineFix(getDescription());
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

    // <TAG_DEPLOYMENT protocol=  provider=>
    attrs.addAttribute("", TAG_ATTR_PROTOCOL, TAG_ATTR_PROTOCOL, null, protocol);
    attrs.addAttribute("", TAG_ATTR_PROVIDER, TAG_ATTR_PROVIDER, null, provider);
    aContentHandler.startElement("", "", TAG_DEPLOYMENT, attrs);        
    attrs.clear();

    // <TAG_CASPOOL>
    attrs.addAttribute("", TAG_ATTR_NUMBER_OF_CASES, TAG_ATTR_NUMBER_OF_CASES,
            null, ""+casPoolSize);
    attrs.addAttribute("", TAG_ATTR_INIT_SIZE_OF_CAS_HEAP, TAG_ATTR_INIT_SIZE_OF_CAS_HEAP,
            null, ""+initialFsHeapSize);
    aContentHandler.startElement("", "", TAG_CASPOOL, attrs);        
    // </TAG_CASPOOL>
    aContentHandler.endElement("", "", TAG_CASPOOL);        
    attrs.clear();

    // <TAG_SERVICES>
    aeService.toXML(aContentHandler, aWriteDefaultNamespaceAttribute);

    // </TAG_DEPLOYMENT>
    aContentHandler.endElement("", "",TAG_DEPLOYMENT);

    attrs.clear();

    aContentHandler.endElement(getXmlizationInfo().namespace,
            "analysisEngineDeploymentDescription","analysisEngineDeploymentDescription");

  }

  /*************************************************************************/
  
  static public String multiLineFix(String s) {
    if (null == s) {
      return null;
    }
    return s.replaceAll("\\r\\n", "\n"); //$NON-NLS-1$ //$NON-NLS-2$
  }

  public String prettyPrint (int indent) {
    StringWriter writer = new StringWriter();
    String parsedText = null;
    try {
      XMLSerializer xmlSerializer = new XMLSerializer(true);
      xmlSerializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", ""+indent);
      xmlSerializer.setWriter(writer);
      ContentHandler contentHandler = xmlSerializer.getContentHandler();
      contentHandler.startDocument();

      toXML(contentHandler, true);

      contentHandler.endDocument();
      writer.close();
      parsedText = writer.toString();

    } catch (SAXException e) {
      // throw new InternalErrorCDE(Messages.getString("MultiPageEditor.22"), e); //$NON-NLS-1$
    } catch (IOException e) {
      // throw new InternalErrorCDE(Messages.getString("MultiPageEditor.23"), e); //$NON-NLS-1$
    }
    return parsedText;
  }

  static public boolean isCASMultiplier (ResourceSpecifier rs)
  {
    if (rs != null) {
      if (rs instanceof AnalysisEngineDescription) {    
        return isCASMultiplier((AnalysisEngineDescription) rs);
      } else if (rs instanceof CollectionReaderDescription) {
        return true;
      }
    } 
    return false;
  }

  static public boolean isCASMultiplier (AnalysisEngineDescription ae)
  {
    return isCASMultiplier(ae.getAnalysisEngineMetaData());
  }

  static public boolean isCASMultiplier (AnalysisEngineMetaData meta)
  {
    OperationalProperties op = meta.getOperationalProperties();
    if (op != null) {
      return op.getOutputsNewCASes();
    }
    return false;
  }

  /*************************************************************************/

  protected XmlizationInfo getXmlizationInfo() {
    // return XMLIZATION_INFO;
    return new XmlizationInfo(null, null);
    //this object has custom XMLization routines
  }

  static final private XmlizationInfo XMLIZATION_INFO =
    new XmlizationInfo("analysisEngineDeploymentDescription",
            new PropertyXmlInfo[]{
            new PropertyXmlInfo("cpeDescriptor", false),
            new PropertyXmlInfo("defaultCasProcessorSettings", false)          
    });



  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#getDescription()
   */
  public String getDescription() {
    return description;
  }




  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#setDescription(java.lang.String)
   */
  public void setDescription(String description) {
    this.description = description;
  }




  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#getName()
   */
  public String getName() {
    return name;
  }




  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#setName(java.lang.String)
   */
  public void setName(String name) {
    this.name = name;
  }




  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#getVendor()
   */
  public String getVendor() {
    return vendor;
  }




  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#setVendor(java.lang.String)
   */
  public void setVendor(String vendor) {
    this.vendor = vendor;
  }




  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#getVersion()
   */
  public String getVersion() {
    return version;
  }




  /* (non-Javadoc)
   * @see com.ibm.uima.application.metadata.impl.AEDeploymentDescription#setVersion(java.lang.String)
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * @return the protocol
   */
  public String getProtocol() {
    return protocol;
  }

  /**
   * @param protocol the protocol to set
   */
  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  /**
   * @return the provider
   */
  public String getProvider() {
    return provider;
  }

  /**
   * @param provider the provider to set
   */
  public void setProvider(String provider) {
    this.provider = provider;
  }

  public AEService getAeService() {
    return aeService;
  }

  public void setAeService(AEService aeService) {
    this.aeService = aeService;
  }

  /**
   * @return the casPoolSize
   */
  public int getCasPoolSize() {
    return casPoolSize;
  }

  /**
   * @param casPoolSize the casPoolSize to set
   */
  public void setCasPoolSize(int casPoolSize) {
    this.casPoolSize = casPoolSize;
  }

  public int getInitialFsHeapSize() {
    return initialFsHeapSize;
  }

  public void setInitialFsHeapSize(int initialSize) {
    initialFsHeapSize = initialSize;
  }

}
