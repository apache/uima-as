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

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
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
public class RemoteAEDeploymentMetaData_Impl extends DeploymentMetaData_Impl 
implements RemoteAEDeploymentMetaData, AEDeploymentConstants
{

  private static final long serialVersionUID = -6028698292841920067L;

  protected int           casMultiplierPoolSize = DEFAULT_CAS_MULTIPLIER_POOL_SIZE;
  protected int initialFsHeapSize = DEFAULT_CAS_INITIAL_HEAP_SIZE;

  protected InputQueue    inputQueue = new InputQueue_Impl();
  protected String        replyQueueLocation = null; // DEFAULT_REPLY_QUEUE_LOCATION; // NOT defined
  protected int           remoteReplyQueueScaleout = -1; // DEFAULT_CONCURRENT_CONSUMERS; // NOT defined
  protected String        serializerMethod = "xmi"; // NOT defined

  protected AsyncAEErrorConfiguration errorConfiguration;
  protected ResourceSpecifier         resourceSpecifier;
  protected Import importedAE;    // import by location|name of AE descriptor


  /*************************************************************************/

  /**
   * 
   */
  public RemoteAEDeploymentMetaData_Impl() {
  }

  public boolean isSet (int i) {
    return i != UNDEFINED_INT;
  }

  public ResourceSpecifier getResourceSpecifier () {
    return resourceSpecifier;
  }

  public void setResourceSpecifier (ResourceSpecifier rs, 
          ResourceManager aResourceManager, boolean recursive) throws InvalidXMLException {
    resourceSpecifier = rs;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resource.metadata.impl.MetaDataObject_impl#buildFromXMLElement(org.w3c.dom.Element, org.apache.uima.util.XMLParser, org.apache.uima.util.XMLParser.ParsingOptions)
   */
  public void buildFromXMLElement (Element aElement, XMLParser aParser, XMLParser.ParsingOptions aOptions)
  throws InvalidXMLException
  {
    String val;

    // Check for "key" and "remoteReplyQueueScaleout" attributes
    NamedNodeMap map = aElement.getAttributes();
    if (map != null) {
      for (int i=0; i<map.getLength(); ++i) {
        Node item = map.item(i);
        String name = item.getNodeName();
        val = item.getNodeValue();
        if (val == null) {
          val = "";
        } else {
          val = val.trim();
        }
        if (AEDeploymentConstants.TAG_ATTR_KEY.equalsIgnoreCase(name)) {
          // Set "key = ..." attribute
          setKey(val);
        } else if (AEDeploymentConstants.TAG_ATTR_REMOTE_REPLYQUEUE_SCALEOUT.equalsIgnoreCase(name)) {          
          // "remoteReplyQueueScaleout" attribute
          try {
            int n = Integer.parseInt(val);
            if (n > 0) {
              setRemoteReplyQueueScaleout(n);
            } else {
              throw new InvalidXMLException(InvalidXMLException.INVALID_ELEMENT_TEXT,
                      new Object[] { n, TAG_ATTR_REMOTE_REPLYQUEUE_SCALEOUT });
            }
          } catch (NumberFormatException e) {
            throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                    new Object[] { TAG_ATTR_REMOTE_REPLYQUEUE_SCALEOUT }, e);
          }

        } else {
          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                  new Object[]{name});
        }
      }
    }

    NodeList childNodes = aElement.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      Node curNode = childNodes.item(i);
      if (curNode instanceof Element) {
        Element elem = (Element)curNode;

        if (TAG_INPUT_QUEUE.equalsIgnoreCase(elem.getTagName())) {
          // inputQueue.setEndPoint(elem.getAttribute(TAG_ATTR_END_POINT));
          // inputQueue.setBrokerURL(elem.getAttribute(TAG_ATTR_BROKER_URL));
          checkAndSetInputQueueAttributes(elem);

        } else if (AEDeploymentConstants.TAG_CAS_MULTIPLIER.equalsIgnoreCase(elem.getTagName())) {
          // Get "poolSize" attribute
          int n;
          val = DDParserUtil.checkAndGetAttributeValue(TAG_CAS_MULTIPLIER, TAG_ATTR_POOL_SIZE, elem, true);
          if (val == null || val.trim().length() == 0) {
            n = 0;
          } else {
            try {
              n = Integer.parseInt(val);
            } catch (NumberFormatException e) {
              e.printStackTrace();
              throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                      new Object[] { TAG_ATTR_POOL_SIZE }, e);
            }
          }
          setCasMultiplierPoolSize(n);

          // Check for Optional "initialFsHeapSize"
          val = DDParserUtil.checkAndGetAttributeValue(TAG_CAS_MULTIPLIER, TAG_ATTR_INIT_SIZE_OF_CAS_HEAP, elem, false);
          if (val != null && val.trim().length() > 0) {
            try {
              initialFsHeapSize = Integer.parseInt(val);
            } catch (NumberFormatException e) {
              e.printStackTrace();
              throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                      new Object[] { TAG_ATTR_INIT_SIZE_OF_CAS_HEAP }, e);
            }
          }
          
        } else if (TAG_REPLY_QUEUE.equalsIgnoreCase(elem.getTagName())) {
          val = DDParserUtil.checkAndGetAttributeValue(TAG_REPLY_QUEUE, TAG_ATTR_LOCATION, elem, false);
          if (val != null && val.trim().length() > 0) {
            val = val.trim();
            if ( /*val.equalsIgnoreCase("local") ||*/  val.equalsIgnoreCase("remote")) {
              setReplyQueueLocation(val);
            } else {
              throw new InvalidXMLException(InvalidXMLException.INVALID_ELEMENT_TEXT,
                      new Object[] {val, TAG_ATTR_LOCATION });
            }
          }
          
        } else if (TAG_SERIALIZER.equalsIgnoreCase(elem.getTagName())) {
          // setSerializerMethod(elem.getAttribute(TAG_ATTR_METHOD));
          setSerializerMethod(DDParserUtil.checkAndGetAttributeValue(TAG_SERIALIZER, TAG_ATTR_METHOD, elem, true));

        } else if (TAG_ASYNC_AGGREGATE_ERROR_CONFIGURATION.equalsIgnoreCase(elem.getTagName())) {
          errorConfiguration = (AsyncAggregateErrorConfiguration) aParser.buildObject(elem, aOptions); 
          errorConfiguration.sParentObject(this);

        } else {
          throw new InvalidXMLException(
                  InvalidXMLException.UNKNOWN_ELEMENT,
                  new Object[]{elem.getTagName()});
        }
      }
    } // for
    
    // Create Error Config if not specified
    if (errorConfiguration == null) {
      errorConfiguration = (AsyncAEErrorConfiguration) UIMAFramework
                        .getResourceSpecifierFactory().createObject(AsyncAggregateErrorConfiguration.class);
      setErrorConfiguration(errorConfiguration);
      errorConfiguration.sParentObject(this);
    }

    
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
          inputQueue.setEndPoint(val);

        } else if (AEDeploymentConstants.TAG_ATTR_BROKER_URL.equalsIgnoreCase(name)) {
          // set "brokerURL =" attribute
          inputQueue.setBrokerURL(val);

        } else {
          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
                  new Object[]{name});
        }
      }
    }

    // Check for missing attributes
    if (inputQueue.getEndPoint() == null) {
      throw new InvalidXMLException(InvalidXMLException.ELEMENT_NOT_FOUND,
              new Object[]{AEDeploymentConstants.TAG_ATTR_END_POINT, AEDeploymentConstants.TAG_INPUT_QUEUE});
    }
    if (inputQueue.getBrokerURL() == null) {
      throw new InvalidXMLException(InvalidXMLException.ELEMENT_NOT_FOUND,
              new Object[]{AEDeploymentConstants.TAG_ATTR_BROKER_URL, AEDeploymentConstants.TAG_INPUT_QUEUE});
    }    
  }

  public void toXML(ContentHandler aContentHandler, boolean aWriteDefaultNamespaceAttribute)
                    throws SAXException
  {
    // <TAG_REMOTE_DELEGATE key=   >
    AttributesImpl attrs = new AttributesImpl();
    if (getKey() != null && getKey().trim().length() > 0) {
      attrs.addAttribute("", TAG_ATTR_KEY, AEDeploymentConstants.TAG_ATTR_KEY, null, getKey());
    }
    if (remoteReplyQueueScaleout > 0) {
      attrs.addAttribute("", TAG_ATTR_REMOTE_REPLYQUEUE_SCALEOUT, TAG_ATTR_REMOTE_REPLYQUEUE_SCALEOUT,
              null, "" + remoteReplyQueueScaleout);        
    }
    aContentHandler.startElement("", TAG_REMOTE_DELEGATE, 
            TAG_REMOTE_DELEGATE, attrs);
    attrs.clear();

    // <casMultiplier poolSize="5"/> <!-- req | omit-->
    if (AEDeploymentDescription_Impl.isCASMultiplier(getResourceSpecifier())) {
      if (getCasMultiplierPoolSize() != UNDEFINED_INT) {
        attrs.addAttribute("", TAG_ATTR_POOL_SIZE, TAG_ATTR_POOL_SIZE,
                null, "" + getCasMultiplierPoolSize());
        attrs.addAttribute("", TAG_ATTR_INIT_SIZE_OF_CAS_HEAP, TAG_ATTR_INIT_SIZE_OF_CAS_HEAP,
                null, ""+initialFsHeapSize);

        aContentHandler.startElement("", TAG_CAS_MULTIPLIER, TAG_CAS_MULTIPLIER, attrs);
        aContentHandler.endElement("", "", TAG_CAS_MULTIPLIER);
        attrs.clear();        
      }
    }

    // <inputQueue ... />
    if (inputQueue != null) {
      attrs.addAttribute("", TAG_ATTR_BROKER_URL, TAG_ATTR_BROKER_URL,
              null, inputQueue.getBrokerURL());
      attrs.addAttribute("", TAG_ATTR_END_POINT, TAG_ATTR_END_POINT,
              null, inputQueue.getEndPoint());
      aContentHandler.startElement("",
              TAG_INPUT_QUEUE, TAG_INPUT_QUEUE, attrs);
      aContentHandler.endElement("", "", TAG_INPUT_QUEUE);
      attrs.clear();
    }

    // <replyQueue location="[local|remote]"/> <!-- optional -->
    // Note: location="local" is NO longer support. Will be warned by DDE
    if (replyQueueLocation != null) {
      attrs.addAttribute("", TAG_ATTR_LOCATION, TAG_ATTR_LOCATION,
                null, replyQueueLocation);
      aContentHandler.startElement("",
              TAG_REPLY_QUEUE, TAG_REPLY_QUEUE, attrs);
      aContentHandler.endElement("", "", TAG_REPLY_QUEUE);
      attrs.clear();
    }

    // <serializer method="xmi"/> <!-- optional -->
    if (getSerializerMethod() != null) {
      attrs.addAttribute("", TAG_ATTR_METHOD, TAG_ATTR_METHOD,
              null, getSerializerMethod());
      aContentHandler.startElement("",
              TAG_SERIALIZER, TAG_SERIALIZER, attrs);
      aContentHandler.endElement("", "", TAG_SERIALIZER);
      attrs.clear();
    }

    // <asyncAggregateErrorConfiguration .../> <!-- optional -->
    if (errorConfiguration != null) {
      errorConfiguration.toXML(aContentHandler, aWriteDefaultNamespaceAttribute);
    }

    // </TAG_REMOTE_DELEGATE>
    aContentHandler.endElement("", "", TAG_REMOTE_DELEGATE);
  }

  @Override
  protected XmlizationInfo getXmlizationInfo() {
    return new XmlizationInfo(null, null);
  }

  /*************************************************************************/

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#getCasMultiplierPoolSize()
   */
  public int getCasMultiplierPoolSize() {
    return casMultiplierPoolSize;
  }

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#setCasMultiplierPoolSize(int)
   */
  public void setCasMultiplierPoolSize(int casMultiplierPoolSize) {
    this.casMultiplierPoolSize = casMultiplierPoolSize;
  }

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#getErrorConfiguration()
   */
  public AsyncAEErrorConfiguration getAsyncAEErrorConfiguration() {
    return errorConfiguration;
  }

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#setErrorConfiguration(com.ibm.uima.aae.deployment.impl.AsyncAggregateErrorConfiguration_Impl)
   */
  public void setErrorConfiguration(AsyncAEErrorConfiguration errorConfiguration) {
    this.errorConfiguration = errorConfiguration;
  }

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#getReplyQueueLocation()
   */
  public String getReplyQueueLocation() {
    return replyQueueLocation;
  }

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#setReplyQueueLocation(java.lang.String)
   */
  public void setReplyQueueLocation(String replyQueueLocation) {
    this.replyQueueLocation = replyQueueLocation;
  }

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#getSerializerMethod()
   */
  public String getSerializerMethod() {
    return serializerMethod;
  }

  /* (non-Javadoc)
   * @see com.ibm.uima.aae.deployment.impl.RemoteAEDeploymentMetaData#setSerializerMethod(java.lang.String)
   */
  public void setSerializerMethod(String serializerMethod) {
    this.serializerMethod = serializerMethod;
  }

  /**
   * @return the inputQueue
   */
  public InputQueue getInputQueue() {
    return inputQueue;
  }

  /**
   * @param inputQueue the inputQueue to set
   */
  public void setInputQueue(InputQueue inputQueue) {
    this.inputQueue = inputQueue;
  }

  /**
   * @return the importedAE
   */
  public Import getImportedAE() {
    return importedAE;
  }

  /**
   * @param importedAE the importedAE to set
   */
  public void setImportedAE(Import importedAE) {
    this.importedAE = importedAE;
  }

  /**
   * @return the initialFsHeapSize
   */
  public int getInitialFsHeapSize() {
      return initialFsHeapSize;
  }

  /**
   * @param initialFsHeapSize the initialFsHeapSize to set
   */
  public void setInitialFsHeapSize(int initialFsHeapSize) {
      this.initialFsHeapSize = initialFsHeapSize;
  }

  public int getRemoteReplyQueueScaleout() {
    return remoteReplyQueueScaleout;
  }

  public void setRemoteReplyQueueScaleout(int concurrentConsumers) {
    remoteReplyQueueScaleout = concurrentConsumers;    
  }


}
