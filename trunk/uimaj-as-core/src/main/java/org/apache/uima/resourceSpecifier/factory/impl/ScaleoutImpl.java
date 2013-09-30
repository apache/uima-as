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

import java.util.Properties;

import org.apache.uima.resourceSpecifier.ScaleoutType;
import org.apache.uima.resourceSpecifier.factory.Scaleout;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class ScaleoutImpl.
 */
public class ScaleoutImpl implements Scaleout {

  /** The st. */
  private ScaleoutType st;
  
  /**
   * Instantiates a new scaleout impl.
   *
   * @param st the st
   * @param context the context
   */
  protected ScaleoutImpl( ScaleoutType st, ServiceContext context) {
    this.st = st;
    setNumberOfInstances(context.getScaleup());
//    setAttr(UimaASDeploymentDescriptor.INPUTQSCALEOUT,props);
  }
//  private void setAttr(String key, Properties props) {
//
//    int value;
//    try {
//      value = Integer.parseInt(props.getProperty(key));
//      if ( props.containsKey(key)) {
//        if ( UimaASDeploymentDescriptor.INPUTQSCALEOUT.equals(key)) {
//          setNumberOfInstances(value);
//        } 
//      }
//    } catch( NumberFormatException e) {
//      Assert.isTrue(false, "Scaleout value provided is not a number");   // force the failure
//    }
//  }

  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.Scaleout#getNumberOfInstances()
 */
public int getNumberOfInstances() {
    Assert.notNull(st);
    return st.getNumberOfInstances();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Scaleout#setNumberOfInstances(int)
   */
  public void setNumberOfInstances(int numberOfInstances) {
    Assert.notNull(st);
    st.setNumberOfInstances(numberOfInstances);
  }

}
