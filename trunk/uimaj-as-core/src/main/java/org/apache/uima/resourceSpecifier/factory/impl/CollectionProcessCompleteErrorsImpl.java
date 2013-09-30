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

import org.apache.uima.resourceSpecifier.CollectionProcessCompleteErrorsType;
import org.apache.uima.resourceSpecifier.factory.Action;
import org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrors;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASDeploymentDescriptor;
import org.springframework.util.Assert;


// TODO: Auto-generated Javadoc
/**
 * The Class CollectionProcessCompleteErrorsImpl.
 */
public class CollectionProcessCompleteErrorsImpl implements CollectionProcessCompleteErrors {

  /** The cpcet. */
  private CollectionProcessCompleteErrorsType cpcet = null;
  
  /**
   * Instantiates a new collection process complete errors impl.
   *
   * @param cpcet the cpcet
   * @param context the context
   * @param topLevel the top level
   */
  public CollectionProcessCompleteErrorsImpl(CollectionProcessCompleteErrorsType cpcet, ServiceContext context, boolean topLevel) {
    Assert.notNull(cpcet);
    this.cpcet = cpcet;
    setErrorAction(context.getCpCAdditionalAction());

//    if ( props.containsKey(UimaASDeploymentDescriptor.ERROR_CPC_ADDITIONAL_ERROR_ACTION)) {
//      setAttr(UimaASDeploymentDescriptor.ERROR_PROCESS_CAS_THRESHOLDACTION,props);
//    } else {
//      setErrorAction(Action.Terminate);
//    }
  }
  
//  private void setAttr(String key, Properties props) {
//    if ( UimaASDeploymentDescriptor.ERROR_CPC_ADDITIONAL_ERROR_ACTION.equals(key)) {
//      if ( props.containsKey(key)) {
//        setErrorAction(Action.valueOf((String)props.get(key)));
//      }
//    } 
//  }

  /* (non-Javadoc)
 * @see org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrors#getErrorAction()
 */
public Action getErrorAction() {
    Assert.notNull(cpcet);
    return Action.valueOf(cpcet.getAdditionalErrorAction());
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.CollectionProcessCompleteErrors#setErrorAction(org.apache.uima.resourceSpecifier.factory.Action)
   */
  public void setErrorAction(Action action) {
    Assert.notNull(cpcet);
    cpcet.setAdditionalErrorAction(action.toString().toLowerCase());
  }

}
