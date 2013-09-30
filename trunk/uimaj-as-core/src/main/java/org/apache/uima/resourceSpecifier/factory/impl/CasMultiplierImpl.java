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

import org.apache.uima.resourceSpecifier.CasMultiplierType;
import org.apache.uima.resourceSpecifier.factory.CasMultiplier;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class CasMultiplierImpl.
 */
public class CasMultiplierImpl implements CasMultiplier {
  
  /** The cmt. */
  private CasMultiplierType cmt;
  
  /**
   * Instantiates a new cas multiplier impl.
   *
   * @param cmt the cmt
   */
  protected CasMultiplierImpl(CasMultiplierType cmt ) { 
    this(cmt,1,2000000,false);
  }
  
  /**
   * Instantiates a new cas multiplier impl.
   *
   * @param cmt the cmt
   * @param casPoolSize the cas pool size
   * @param initialHeapSize the initial heap size
   * @param processParentLast the process parent last
   */
  protected CasMultiplierImpl(CasMultiplierType cmt, int casPoolSize, int initialHeapSize, boolean processParentLast) {
    this.cmt = cmt;
    setCasPoolSize(casPoolSize);
    setInitialFsHeapSize(initialHeapSize);
    setProcessParentLast(processParentLast);
  }
  
  /**
   * Instantiates a new cas multiplier impl.
   *
   * @param cmt the cmt
   * @param context the context
   */
  protected CasMultiplierImpl(CasMultiplierType cmt, ServiceContext context) {
    this.cmt = cmt;
    setCasPoolSize(context.getCasPoolSize());
    setInitialFsHeapSize(context.getInitialHeapSize());
    setProcessParentLast(context.processParentLast());

//    if ( props.containsKey(UimaASDeploymentDescriptor.CASPOOL_CAS_COUNT)) {
//      setAttr(UimaASDeploymentDescriptor.CASPOOL_CAS_COUNT,props);
//    } else {
//      setCasPoolSize(1);
//    }
//    if ( props.containsKey(UimaASDeploymentDescriptor.CASPOOL_INITIAL_FD_HEAP_SIZE)) {
//      setAttr(UimaASDeploymentDescriptor.CASPOOL_INITIAL_FD_HEAP_SIZE,props);
//    } else {
//      setInitialFsHeapSize(200000);
//    }
//
//    if ( props.containsKey(UimaASDeploymentDescriptor.PROCESS_PARENT_LAST)) {
//      setAttr(UimaASDeploymentDescriptor.PROCESS_PARENT_LAST,props);
//    } else {
//      setProcessParentLast(false);
//    }
  }
//  private void setAttr(String key, Properties props) {
//    if ( props.containsKey(key)) {
//      if ( UimaASDeploymentDescriptor.CASPOOL_CAS_COUNT.equals(key)) {
//        setCasPoolSize(Integer.parseInt(props.getProperty(key)));
//      } else if (UimaASDeploymentDescriptor.CASPOOL_INITIAL_FD_HEAP_SIZE.equals(key)) {
//        setInitialFsHeapSize(Integer.parseInt(props.getProperty(key)));
//      } else if (UimaASDeploymentDescriptor.PROCESS_PARENT_LAST.equals(key)) {
//        setInitialFsHeapSize(Integer.parseInt(props.getProperty(key)));
//      }
//    }
//  }

  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.CasMultiplier#getCasPoolSize()
 */
public int getCasPoolSize() {
    Assert.notNull(cmt);
    return cmt.getPoolSize();
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasMultiplier#setCasPoolSize(int)
   */
  public void setCasPoolSize(int casPoolSize) {
    Assert.notNull(cmt);
    cmt.setPoolSize(casPoolSize);
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasMultiplier#setInitialFsHeapSize(long)
   */
  public void setInitialFsHeapSize(long initialFsHeapSize) {
    Assert.notNull(cmt);
    cmt.setInitialFsHeapSize(String.valueOf(initialFsHeapSize));
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasMultiplier#getInitialFsHeapSize()
   */
  public long getInitialFsHeapSize() {
    Assert.notNull(cmt);
    long heapSize =0;
    try {
      heapSize = Long.parseLong(cmt.getInitialFsHeapSize()); 
    } catch(NumberFormatException nfe) {
      heapSize = -1;
    }
    Assert.isTrue(heapSize > -1);
    return heapSize;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasMultiplier#getProcessParentLast()
   */
  public boolean getProcessParentLast() {
    Assert.notNull(cmt);
    return Boolean.parseBoolean(cmt.getProcessParentLast());
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CasMultiplier#setProcessParentLast(boolean)
   */
  public void setProcessParentLast(boolean processParentLast) {
    Assert.notNull(cmt);
    cmt.setProcessParentLast(Boolean.toString(processParentLast));
  }
}
