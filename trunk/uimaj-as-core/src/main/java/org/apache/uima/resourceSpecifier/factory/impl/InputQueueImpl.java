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
package org.apache.uima.resourceSpecifier.factory.impl;


import org.apache.uima.resourceSpecifier.InputQueueType;
import org.apache.uima.resourceSpecifier.factory.InputQueue;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class InputQueueImpl.
 */
public class InputQueueImpl implements InputQueue {
  
  /** The iqt. */
  private InputQueueType iqt;
  
  /**
   * Instantiates a new input queue impl.
   *
   * @param iqt the iqt
   * @param endpoint the endpoint
   * @param brokerURL the broker url
   * @param prefetch the prefetch
   */
  public InputQueueImpl(InputQueueType iqt, String endpoint, String brokerURL, int prefetch) {
    this.iqt = iqt;
    setEndpoint(endpoint);
    setBroker(brokerURL);
    setPrefetch(prefetch);
  }
  
  /**
   * Instantiates a new input queue impl.
   *
   * @param iqt the iqt
   * @param context the context
   */
  public InputQueueImpl(InputQueueType iqt, ServiceContext context) {
    this.iqt = iqt;
    Assert.notNull(context.getEndpoint());
    setEndpoint(context.getEndpoint());
    setBroker(context.getBrokerURL());
    setPrefetch(context.getPrefetch());
    
//    setAttr(UimaASDeploymentDescriptor.ENDPOINT,props);
//    if ( props.containsKey(UimaASDeploymentDescriptor.BROKERURL)) {
//      setAttr(UimaASDeploymentDescriptor.BROKERURL,props);
//    } else {
//      setBroker(UimaASDeploymentDescriptor.DEFAULT_BROKER_URL);
//    }
//    if ( props.containsKey(UimaASDeploymentDescriptor.PREFETCH)) {
//      setAttr(UimaASDeploymentDescriptor.PREFETCH,props);
//    } else {
//      setPrefetch(0);
//    }

//    setAttr(UimaASDeploymentDescriptor.BROKERURL,props);
//    setAttr(UimaASDeploymentDescriptor.PREFETCH,props);
  }
//  private void setAttr(String key, Properties props) {
//    if ( props.containsKey(key)) {
//      if ( UimaASDeploymentDescriptor.ENDPOINT.equals(key)) {
//        setEndpoint(props.getProperty(key));
//      } else if (UimaASDeploymentDescriptor.BROKERURL.equals(key)) {
//        setBroker(props.getProperty(key));
//      } else if (UimaASDeploymentDescriptor.PREFETCH.equals(key)) {
//        setBroker(props.getProperty(key));
//      }
//    }
//  }

  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.InputQueue#getEndpoint()
 */
public String getEndpoint() {
    Assert.notNull(iqt);
    return iqt.getEndpoint();
  } 

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.InputQueue#setEndpoint(java.lang.String)
   */
  public void setEndpoint(String endpoint) {
    Assert.notNull(iqt);
    iqt.setEndpoint(endpoint);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.InputQueue#getBroker()
   */
  public String getBroker() {
    Assert.notNull(iqt);
    return iqt.getBrokerURL();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.InputQueue#setBroker(java.lang.String)
   */
  public void setBroker(String broker) {
    Assert.notNull(iqt);
    iqt.setBrokerURL(broker);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.InputQueue#setPrefetch(int)
   */
  public void setPrefetch( int prefetch ) {
    Assert.notNull(iqt);
    iqt.setPrefetch(prefetch);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.InputQueue#getPrefetch()
   */
  public int getPrefetch() {
    Assert.notNull(iqt);
    return iqt.getPrefetch();
  }

}
