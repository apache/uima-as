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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.Constants;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AEService;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.impl.Import_impl;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.PropertyXmlInfo;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
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
public class AEService_Impl extends MetaDataObject_impl implements AEService, AEDeploymentConstants {
  protected String endPoint = null;

  protected String brokerURL = null;
  
  protected int prefetch = 0; // default to 0

  protected AEDeploymentMetaData analysisEngineDeploymentMetaData;

  protected Import importDescriptor;
  
  protected boolean importResolved = false;

  protected ResourceSpecifier topAnalysisEngineDescription;
  
  protected boolean cPlusPlusTopAE = false;
  
  protected String customValue;  // <custom name=....>
  
  protected List<NameValue> environmentVariables = new ArrayList<NameValue>();

  /** ********************************************************************** */

  public AEService_Impl() {
  }

  /** ********************************************************************** */

  protected ResourceSpecifier resolveImport(ResourceManager aResourceManager)
          throws InvalidXMLException {
    // Trace.err("resolveImport: " + importDescriptor.getLocation());
    // make sure Import's relative path base is set, to allow for
    // users who create new import objects
    // Trace.err("resolveImport 1 - getSourceUrlString: " + importDescriptor.getSourceUrlString());
    if (importDescriptor instanceof Import_impl) {
      ((Import_impl) importDescriptor).setSourceUrlIfNull(this.getSourceUrl());
    }
    // locate import target
    URL url = importDescriptor.findAbsoluteUrl(aResourceManager);
    // Trace.err("resolveImport 2 - getSourceUrlString: " + importDescriptor.getSourceUrlString());
    // Trace.err("resolveImport 3 - findAbsoluteUrl: " + url.toString());

    // parse import target
    XMLInputSource input;
    try {
      input = new XMLInputSource(url);
    } catch (IOException e) {
      throw new InvalidXMLException(InvalidXMLException.IMPORT_FAILED_COULD_NOT_READ_FROM_URL,
              new Object[] { url, importDescriptor.getSourceUrlString() }, e);
    }
    ResourceSpecifier spec = UIMAFramework.getXMLParser().parseResourceSpecifier(input);
    // Trace.err("Top AE sourceUrl: " + ((AnalysisEngineDescription) spec).getSourceUrlString());

    return spec;
  }

  /**
   * Overridden to provide custom XMLization.
   * 
   * @see org.apache.uima.util.XMLizable#buildFromXMLElement(org.w3c.dom.Element,
   *      org.apache.uima.util.XMLParser)
   */
  public void buildFromXMLElement(Element aElement, XMLParser aParser,
          XMLParser.ParsingOptions aOptions) throws InvalidXMLException {
    // Trace.trace();
    NodeList childNodes = aElement.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node curNode = childNodes.item(i);
      if (curNode instanceof Element) {
        Element elem = (Element) curNode;

        if (TAG_INPUT_QUEUE.equalsIgnoreCase(elem.getTagName())) {
          checkAndSetInputQueueAttributes(elem);

        } else if (TAG_TOP_DESCRIPTOR.equalsIgnoreCase(elem.getTagName())) {
          // setTopDescriptor(XMLUtils.getText(elem));
          // setImportByLocation("");
          NodeList nodes = elem.getChildNodes();
          if (nodes.getLength() > 0) {
            // Should be an "import"
            for (int k = 0; k < nodes.getLength(); ++k) {
              Node n = nodes.item(k);
              if (!(n instanceof Element)) {
                // Trace.err("NOT an import");
                // setImportByLocation("");
                continue;
              } else if (!((Element) n).getTagName().equalsIgnoreCase(TAG_IMPORT)) {
                throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                        new Object[]{((Element) n).getTagName()});
              } else {
                importDescriptor = (Import) aParser.buildObject((Element) n, aOptions);
              }
            }
          }
        } else if (TAG_ANALYSIS_ENGINE.equalsIgnoreCase(elem.getTagName())) {
          analysisEngineDeploymentMetaData = (AEDeploymentMetaData) aParser.buildObject(elem,
                  aOptions);
          analysisEngineDeploymentMetaData.setTopAnalysisEngine(true);
          
        } else if (TAG_CUSTOM.equalsIgnoreCase(elem.getTagName())) {
          // Check for "name"
          NamedNodeMap map = elem.getAttributes();
          if (map != null) {
            for (int k=0; k<map.getLength(); ++k) {
              Node item = map.item(k);
              String name = item.getNodeName();
              String val = item.getNodeValue();
              if (val == null) {
                val = "";
              } else {
                val = val.trim();
              }
              if (TAG_ATTR_NAME.equalsIgnoreCase(name)) {
                // set "name = ..." attribute
                customValue = val;
              } else {
                throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                        new Object[]{name});
              }
            }
          }
          
        } else if (TAG_ENV_VARS.equalsIgnoreCase(elem.getTagName())) {
          // delegates = new AEDelegates_Impl(this);
          NodeList nodes = elem.getChildNodes();
          if (nodes.getLength() > 0) {
            // Look for "environmentVariable"
            for (int k = 0; k < nodes.getLength(); ++k) {
              Node n = nodes.item(k);
              if (!(n instanceof Element)) {
                continue;
              }

              Element e = (Element) n;
              if (TAG_ENV_VAR.equalsIgnoreCase(e.getTagName())) {
                String envName = getValueOfNameAttribute(e);
                if (envName == null) {
                  throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                          new Object[]{e.getTagName()});
                }
                if (environmentVariables == null) {
                  environmentVariables = new ArrayList<NameValue>();
                }
                environmentVariables.add(new NameValue(envName, e.getTextContent()));
              } else {     
                throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                        new Object[]{e.getTagName()});
              }
            }
          }
          
        } else {
            throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                    new Object[]{elem.getTagName()});
        }
      }
    }

    // Create DEFAULT AEDeploymentMetaData for TOP AE
    if (analysisEngineDeploymentMetaData == null) {
      analysisEngineDeploymentMetaData = new AEDeploymentMetaData_Impl();
      analysisEngineDeploymentMetaData.setTopAnalysisEngine(true);
    }
    if (analysisEngineDeploymentMetaData.getAsyncAEErrorConfiguration() == null) {
      // Create a new Error Config
      Object obj = UIMAFramework.getResourceSpecifierFactory().createObject(
              AsyncPrimitiveErrorConfiguration.class);
      if (obj == null) {
        Trace.err("Cannot create AsyncPrimitiveErrorConfiguration for TOP");
      }
      analysisEngineDeploymentMetaData.setAsyncAEErrorConfiguration((AsyncPrimitiveErrorConfiguration) obj);
    }
  }
  
  protected String getValueOfNameAttribute (Element elem) throws InvalidXMLException {
    // Check for "name"
    NamedNodeMap map = elem.getAttributes();
    if (map != null) {
      for (int k=0; k<map.getLength(); ++k) {
        Node item = map.item(k);
        String name = item.getNodeName();
        String val = item.getNodeValue();
        if (val == null) {
          val = "";
        } else {
          val = val.trim();
        }
        if (TAG_ATTR_NAME.equalsIgnoreCase(name)) {
          // get "name = ..." attribute
          return val;
        } else {
          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                  new Object[]{name});
        }
      }
    }
    return null;
  }
  
  /**
   * Check and set the attributes of Input Queue
   * 
   * @param aElement
   * @throws InvalidXMLException
   * @return void
   */
  protected void checkAndSetInputQueueAttributes (Element aElement) throws InvalidXMLException {
    // Check for "key" and "async" attributes
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
        if (AEDeploymentConstants.TAG_ATTR_END_POINT.equalsIgnoreCase(name)) {
          // set "endpoint = ..." attribute
          setEndPoint(val);

        } else if (AEDeploymentConstants.TAG_ATTR_BROKER_URL.equalsIgnoreCase(name)) {
          // set "brokerURL =" attribute
          setBrokerURL(val);

        } else if (AEDeploymentConstants.TAG_ATTR_PREFETCH.equalsIgnoreCase(name)) {
          // set "prefetch =" attribute
          if (val.trim().length() > 0) {
            setPrefetch(Integer.parseInt(val));
          }
        } else {
          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                  new Object[]{name});
        }
      }
    }
    
    // Check for missing attributes
    if (getEndPoint() == null) {
      throw new InvalidXMLException(InvalidXMLException.ELEMENT_NOT_FOUND,
              new Object[]{AEDeploymentConstants.TAG_ATTR_END_POINT, AEDeploymentConstants.TAG_SERVICE});
    }
    if (getBrokerURL() == null) {
      throw new InvalidXMLException(InvalidXMLException.ELEMENT_NOT_FOUND,
              new Object[]{AEDeploymentConstants.TAG_ATTR_BROKER_URL, AEDeploymentConstants.TAG_SERVICE});
    }    
  }

  /**
   * Overridden to provide custom XML representation.
   * 
   * @see org.apache.uima.util.XMLizable#toXML(ContentHandler)
   */
  public void toXML(ContentHandler aContentHandler, boolean aWriteDefaultNamespaceAttribute)
          throws SAXException {
    // <TAG_SERVICES>
    AttributesImpl attrs = new AttributesImpl();
    aContentHandler.startElement("", TAG_SERVICE, TAG_SERVICE, attrs);

    // <TAG_INPUT_QUEUE />
    attrs.addAttribute("", TAG_ATTR_END_POINT, TAG_ATTR_END_POINT, null, getEndPoint());
    attrs.addAttribute("", TAG_ATTR_BROKER_URL, TAG_ATTR_BROKER_URL, null, getBrokerURL());
    attrs.addAttribute("", TAG_ATTR_PREFETCH, TAG_ATTR_PREFETCH, null, ""+getPrefetch());
    aContentHandler.startElement("", TAG_INPUT_QUEUE, TAG_INPUT_QUEUE, attrs);
    aContentHandler.endElement("", "", TAG_INPUT_QUEUE);
    attrs.clear();
    
    if (isCPlusPlusTopAE()) {
      if (customValue != null) {
        attrs.addAttribute("", TAG_ATTR_NAME, TAG_ATTR_NAME, null, getCustomValue());
        aContentHandler.startElement("", TAG_CUSTOM, TAG_CUSTOM, attrs);
        aContentHandler.endElement("", "", TAG_CUSTOM);
        attrs.clear();
        if (environmentVariables != null && environmentVariables.size() > 0) {
          aContentHandler.startElement("", TAG_ENV_VARS, TAG_ENV_VARS, attrs);
          for (NameValue nv: environmentVariables) {
            attrs.addAttribute("", TAG_ATTR_NAME, TAG_ATTR_NAME, null, nv.getName());
            aContentHandler.startElement("", TAG_ENV_VAR, TAG_ENV_VAR, attrs);
            aContentHandler.characters(nv.getValue().toCharArray(), 0, nv.getValue().length());
            aContentHandler.endElement("", "", TAG_ENV_VAR);
            attrs.clear();
          }
          aContentHandler.endElement("", "", TAG_ENV_VARS);
        }
      }
    } 

    // <TAG_TOP_DESCRIPTOR>
    aContentHandler.startElement("", TAG_TOP_DESCRIPTOR, TAG_TOP_DESCRIPTOR, attrs);

    if (importDescriptor != null) {
      importDescriptor.toXML(aContentHandler, aWriteDefaultNamespaceAttribute);
    }

    // </TAG_TOP_DESCRIPTOR>
    aContentHandler.endElement("", "", TAG_TOP_DESCRIPTOR);
    attrs.clear();

    if (analysisEngineDeploymentMetaData != null) {
      analysisEngineDeploymentMetaData.toXML(aContentHandler, aWriteDefaultNamespaceAttribute);
    }

    // </TAG_SERVICES>
    aContentHandler.endElement("", "", TAG_SERVICE);
  }

  /** ********************************************************************** */

  protected XmlizationInfo getXmlizationInfo() {
    // return XMLIZATION_INFO;
    return new XmlizationInfo(null, null);
    // this object has custom XMLization routines
  }

  static final private XmlizationInfo XMLIZATION_INFO = new XmlizationInfo("uimaApplication",
          new PropertyXmlInfo[] { new PropertyXmlInfo("cpeDescriptor", false),
              new PropertyXmlInfo("defaultCasProcessorSettings", false) });

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.uima.application.metadata.impl.AEService#getBrokerURL()
   */
  public String getBrokerURL() {
    return brokerURL;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.uima.application.metadata.impl.AEService#setBrokerURL(java.lang.String)
   */
  public void setBrokerURL(String brokerURL) {
    this.brokerURL = brokerURL;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.uima.application.metadata.impl.AEService#getEndPoint()
   */
  public String getEndPoint() {
    return endPoint;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.uima.application.metadata.impl.AEService#setEndPoint(java.lang.String)
   */
  public void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.uima.application.metadata.impl.AEService#getImportByLocation()
   */
  // public String getImportByLocation() {
  // return importByLocation;
  // }
  //
  // /* (non-Javadoc)
  // * @see com.ibm.uima.application.metadata.impl.AEService#setImportByLocation(java.lang.String)
  // */
  // public void setImportByLocation(String importByLocation) {
  // this.importByLocation = importByLocation;
  // }
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.uima.application.metadata.impl.AEService#getTopDescriptor()
   */
  // public String getTopDescriptor() {
  // return topDescriptor;
  // }
  //
  // /* (non-Javadoc)
  // * @see com.ibm.uima.application.metadata.impl.AEService#setTopDescriptor(java.lang.String)
  // */
  // public void setTopDescriptor(String topDescriptor) {
  // this.topDescriptor = topDescriptor;
  // }
  // public String getImportByName() {
  // return importByName;
  // }
  //
  // public void setImportByName(String importByName) {
  // this.importByName = importByName;
  // }
  /**
   * @return the analysisEngineDeploymentMetaData
   * @throws InvalidXMLException
   */
  public AEDeploymentMetaData getAnalysisEngineDeploymentMetaData() throws InvalidXMLException {
    // Trace.err(10, "");
    return getAnalysisEngineDeploymentMetaData(UIMAFramework.newDefaultResourceManager());
  }

  /**
   * @return the analysisEngineDeploymentMetaData
   * @throws InvalidXMLException
   */
  public AEDeploymentMetaData getAnalysisEngineDeploymentMetaData(ResourceManager aResourceManager)
          throws InvalidXMLException {
    // Trace.err("ResourceManager 0");
    if (analysisEngineDeploymentMetaData == null) {
      return null;
    }
    // Trace.err("ResourceManager 1");

    // Resolve import descriptor
    // CANNOT RESOLVE IMPORT HERE BECAUSE THIS METHOD IS CALLED BY REFLECTION
    // FROM SUPER-CLASS WHEN PARSING "IMPORT"
    // resolveTopAnalysisEngineDescription(aResourceManager, false);

    return analysisEngineDeploymentMetaData;
  }

  /**
   * @param analysisEngineDeploymentMetaData
   *          the analysisEngineDeploymentMetaData to set
   */
  public void setAnalysisEngineDeploymentMetaData(
          AEDeploymentMetaData analysisEngineDeploymentMetaData) {
    this.analysisEngineDeploymentMetaData = analysisEngineDeploymentMetaData;
    analysisEngineDeploymentMetaData.setTopAnalysisEngine(true);
  }

  /**
   * @return the importDescriptor
   */
  public Import getImportDescriptor() {
    return importDescriptor;
  }

  /**
   * @param importDescriptor
   *          the importDescriptor to set
   */
  public void setImportDescriptor(Import importDescriptor) {
    this.importDescriptor = importDescriptor;
    
    // Handle the case that the top AE descriptor is reloaded
    importResolved = false;
    topAnalysisEngineDescription = null;
  }

  /**
   * @return the topAnalysisEngineDescription
   * @throws InvalidXMLException
   */
  public ResourceSpecifier getTopAnalysisEngineDescription() throws InvalidXMLException {
    return getTopAnalysisEngineDescription(UIMAFramework.newDefaultResourceManager());
  }

  /**
   * @return the topAnalysisEngineDescription
   * @throws InvalidXMLException
   */
  public ResourceSpecifier getTopAnalysisEngineDescription(ResourceManager aResourceManager)
          throws InvalidXMLException {
    if (topAnalysisEngineDescription == null) {
      // Resolve import. 
      // CANNOT RESOLVE IMPORT HERE BECAUSE THIS METHOD IS CALLED BY REFLECTION
      // FROM SUPER-CLASS WHEN PARSING "IMPORT"
      // resolveTopAnalysisEngineDescription(aResourceManager, false);
    }
    return topAnalysisEngineDescription;
  }

  /**
   * @return the topAnalysisEngineDescription
   * @throws InvalidXMLException
   */
  public ResourceSpecifier resolveTopAnalysisEngineDescription(boolean recursive)
          throws InvalidXMLException {
    return resolveTopAnalysisEngineDescription(UIMAFramework.newDefaultResourceManager(), recursive);
  }

  /**
   * @return the topAnalysisEngineDescription
   * @throws InvalidXMLException
   */
  public ResourceSpecifier resolveTopAnalysisEngineDescription(
          ResourceManager aResourceManager, boolean recursive) throws InvalidXMLException {
    if (importDescriptor == null) {
      return null;
    }
        
    topAnalysisEngineDescription = resolveImport(aResourceManager);

    // If C++ descriptor, active C++ settings
    if (topAnalysisEngineDescription != null) {
      if (((ResourceCreationSpecifier)topAnalysisEngineDescription).getFrameworkImplementation().equalsIgnoreCase(Constants.CPP_FRAMEWORK_NAME)) {
        cPlusPlusTopAE = true;
      } else {
        cPlusPlusTopAE = false;
        // Check that there are NO Settings for C++ 
        // Note: Comment out to be "user-friendly" when switching from C++ to Java AE
//        if (customValue != null) {
//          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
//                  new Object[]{TAG_CUSTOM});
//        }
      }
    }
    
    if (topAnalysisEngineDescription != null && analysisEngineDeploymentMetaData != null) {
      analysisEngineDeploymentMetaData.setResourceSpecifier(topAnalysisEngineDescription,
              aResourceManager, recursive);
    }
    importResolved = true;
    return topAnalysisEngineDescription;
  }

  /**
   * @param topAnalysisEngineDescription
   *          the topAnalysisEngineDescription to set
   */
  public void setTopAnalysisEngineDescription(AnalysisEngineDescription topAnalysisEngineDescription) {
    this.topAnalysisEngineDescription = topAnalysisEngineDescription;
  }

  /**
   * Will through exception if import is not resolved
   * 
   * @return void
   */
  protected void checkImport () {
    Trace.err(" ---> Import is not resolved");
  }
  
  /**
   * @return the importResolved
   */
  protected boolean isImportResolved() {
    return importResolved;
  }

  /**
   * @param importResolved the importResolved to set
   */
  protected void setImportResolved(boolean importResolved) {
    this.importResolved = importResolved;
  }

  /**
   * @return the prefetch
   */
  public int getPrefetch() {
    return prefetch;
  }

  /**
   * @param prefetch the prefetch to set
   */
  public void setPrefetch(int prefetch) {
    this.prefetch = prefetch;
  }

  /**
   * @return the environmentVariables
   */
  public List<NameValue> getEnvironmentVariables() {
    return environmentVariables;
  }

  /**
   * @param environmentVariables the environmentVariables to set
   */
  public void setEnvironmentVariables(List<NameValue> environmentVariables) {
    this.environmentVariables = environmentVariables;
  }

  /**
   * @return the customValue
   */
  public String getCustomValue() {
    return customValue;
  }

  /**
   * @param customValue the customValue to set
   */
  public void setCustomValue(String customValue) {
    this.customValue = customValue;
  }

  /**
   * @return the cPlusPlusTopAE
   */
  public boolean isCPlusPlusTopAE() {
    return cPlusPlusTopAE;
  }

  /**
   * @param plusPlusTopAE the cPlusPlusTopAE to set
   */
  public void setCPlusPlusTopAE(boolean plusPlusTopAE) {
    cPlusPlusTopAE = plusPlusTopAE;
  }

}
