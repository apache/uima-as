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


import org.apache.uima.resourceSpecifier.CasPoolType;
import org.apache.uima.resourceSpecifier.factory.CasPool;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;


// TODO: Auto-generated Javadoc
/**
 * The Class CasPoolImpl.
 */
public class CasPoolImpl implements CasPool {
  
  /** The cpt. */
  private CasPoolType cpt;
  
  /**
   * Instantiates a new cas pool impl.
   *
   * @param cpt the cpt
   * @param context the context
   */
  public CasPoolImpl(CasPoolType cpt, ServiceContext context) {
    Assert.notNull(cpt);
    this.cpt = cpt;
    setNumberOfCases(context.getCasPoolSize());
    setInitialFsHeapSize(context.getInitialHeapSize());

//    if ( props.containsKey(UimaASDeploymentDescriptor.CASPOOL_CAS_COUNT)) {
//      setAttr(UimaASDeploymentDescriptor.CASPOOL_CAS_COUNT,props);
//    } else {
//      setNumberOfCases(1);
//    }
//    if ( props.containsKey(UimaASDeploymentDescriptor.CASPOOL_INITIAL_FD_HEAP_SIZE)) {
//      setAttr(UimaASDeploymentDescriptor.CASPOOL_INITIAL_FD_HEAP_SIZE,props);
//    } else {
//      setInitialFsHeapSize(2000000);
//    }
  }
//  private void setAttr(String key, Properties props) {
//    if ( props.containsKey(key)) {
//      if ( UimaASDeploymentDescriptor.CASPOOL_CAS_COUNT.equals(key)) {
//        setNumberOfCases(Integer.parseInt(props.getProperty(key)));
//      } else if (UimaASDeploymentDescriptor.CASPOOL_INITIAL_FD_HEAP_SIZE.equals(key)) {
//        setInitialFsHeapSize(Integer.parseInt(props.getProperty(key)));
//      } 
//    }
//  }

  
  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.CasPool#getNumberOfCases()
 */
public int getNumberOfCases() {
    Assert.notNull(cpt);
    return cpt.getNumberOfCASes();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasPool#setNumberOfCases(int)
   */
  public void setNumberOfCases(int casCount) {
    Assert.notNull(cpt);
    cpt.setNumberOfCASes(casCount);
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasPool#getInitialFsHeapSize()
   */
  public int getInitialFsHeapSize() {
    Assert.notNull(cpt);
    return cpt.getInitialFsHeapSize();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasPool#setInitialFsHeapSize(int)
   */
  public void setInitialFsHeapSize(int heapSize) {
    Assert.notNull(cpt);
    cpt.setInitialFsHeapSize(heapSize);
  }

}
