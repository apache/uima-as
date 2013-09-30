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

package org.apache.uima.dde.internal.provider;

import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.aae.deployment.impl.DeploymentMetaData_Impl;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.Viewer;


public class ErrorConfigContentProvider implements IStructuredContentProvider {
  private static final Object[] EMPTY_ARRAY = new Object[0];

  public Object[] getElements(Object inputElement) {
    Object[] objs = null;
    int count = 0;
    int status = 0;

    if (inputElement instanceof GetMetadataErrors) {
      GetMetadataErrors getMetadataErrors = (GetMetadataErrors) inputElement;
      DeploymentMetaData_Impl parentMetaData = getMetadataErrors.getParent().gParentObject();
      
      if ( (parentMetaData instanceof AEDeploymentMetaData) 
              && !((AEDeploymentMetaData) parentMetaData).isAsync() ) {
        status = NameValuePair.STATUS_NON_EDITABLE;
      }
      
      objs = new Object[3];
      objs[0] = new NameValuePair(getMetadataErrors, GetMetadataErrors.KIND_TIMEOUT, 
              GetMetadataErrors.NAME_TIMEOUT,
              getMetadataErrors.getTimeout(), Integer.class);
      objs[1] = new NameValuePair(getMetadataErrors, GetMetadataErrors.KIND_ERRORACTION,
              GetMetadataErrors.NAME_ERRORACTION,
              getMetadataErrors.getErrorAction(), String.class);
      objs[2] = new NameValuePair(getMetadataErrors, GetMetadataErrors.KIND_MAX_RETRIES,
              GetMetadataErrors.NAME_MAX_RETRIES, 
              getMetadataErrors.getMaxRetries(), Integer.class);
      // ((NameValuePair) objs[0]).setStatusFlags(status);
      // ((NameValuePair) objs[1]).setStatusFlags(status);
      ((NameValuePair) objs[2]).setStatusFlags(status);
      return objs;

    } else if (inputElement instanceof ProcessCasErrors) {
      ProcessCasErrors processCasErrors = (ProcessCasErrors) inputElement;
      AsyncAEErrorConfiguration parent = processCasErrors.getParent();
      DeploymentMetaData_Impl parentMetaData = parent.gParentObject();
      
      if (parent instanceof AsyncAggregateErrorConfiguration) {
        count = 6;
      } else {
        count = 3;
      }
      objs = new Object[count];
      
      // Set names
      String nameThresholdCount;
      String nameThresholdWindow;
      String nameThresholdAction;
      
      if (count == 6) {
        // For AsyncAggregateErrorConfiguration
        objs[0] = new NameValuePair(processCasErrors, ProcessCasErrors.KIND_MAX_RETRIES,
                ProcessCasErrors.NAME_MAX_RETRIES, 
                processCasErrors.getMaxRetries(), Integer.class);
        objs[1] = new NameValuePair(processCasErrors, ProcessCasErrors.KIND_TIMEOUT, 
                ProcessCasErrors.NAME_TIMEOUT,
                processCasErrors.getTimeout(), Integer.class);
        objs[2] = new NameValuePair(processCasErrors, ProcessCasErrors.KIND_CONTINUE_ON_RETRY,
                ProcessCasErrors.NAME_CONTINUE_ON_FAILURE, 
                processCasErrors.isContinueOnRetryFailure(),
                Boolean.class);
        if ( (parentMetaData instanceof AEDeploymentMetaData) 
                && !((AEDeploymentMetaData) parentMetaData).isAsync() ) {
          ((NameValuePair) objs[0]).setStatusFlags(NameValuePair.STATUS_NON_EDITABLE);
          // ((NameValuePair) objs[2]).setStatusFlags(NameValuePair.STATUS_NON_EDITABLE);
          
        }

        nameThresholdCount = ProcessCasErrors.NAME_DELEGATE_THRESHOLD_COUNT;
        nameThresholdWindow = ProcessCasErrors.NAME_DELEGATE_THRESHOLD_WINDOW;
        nameThresholdAction = ProcessCasErrors.NAME_DELEGATE_THRESHOLD_ACTION;
      } else {
        // For AsyncPrimitiveErrorConfiguration
        nameThresholdCount = ProcessCasErrors.NAME_THRESHOLD_COUNT;
        nameThresholdWindow = ProcessCasErrors.NAME_THRESHOLD_WINDOW;
        nameThresholdAction = ProcessCasErrors.NAME_THRESHOLD_ACTION;
      }
      objs[count-3] = new NameValuePair(processCasErrors, ProcessCasErrors.KIND_THRESHOLD_COUNT,
              nameThresholdCount, 
              processCasErrors.getThresholdCount(), Integer.class);
      objs[count-2] = new NameValuePair(processCasErrors, ProcessCasErrors.KIND_THRESHOLD_WINDOW,
              nameThresholdWindow, 
              processCasErrors.getThresholdWindow(), Integer.class);
      objs[count-1] = new NameValuePair(processCasErrors, ProcessCasErrors.KIND_THRESHOLD_ACTION,
              nameThresholdAction, 
              processCasErrors.getThresholdAction(), String.class);
      
      if (processCasErrors.getThresholdCount() == 0) {
        status = NameValuePair.STATUS_NON_EDITABLE;
        ((NameValuePair) objs[count-2]).setStatusFlags(NameValuePair.STATUS_NON_EDITABLE);
        ((NameValuePair) objs[count-1]).setStatusFlags(NameValuePair.STATUS_NON_EDITABLE);
      } else {
        // For Async Primitive Error
        if (processCasErrors.getParent() instanceof AsyncPrimitiveErrorConfiguration) {
          ((NameValuePair) objs[count-1]).setStatusFlags(NameValuePair.STATUS_NON_EDITABLE);
        }
      }
      return objs;

    } else if (inputElement instanceof CollectionProcessCompleteErrors) {
      CollectionProcessCompleteErrors completeErrors = (CollectionProcessCompleteErrors) inputElement;

      objs = new Object[2];
      objs[0] = new NameValuePair(completeErrors, CollectionProcessCompleteErrors.KIND_TIMEOUT,
              CollectionProcessCompleteErrors.NAME_TIMEOUT, 
              completeErrors.getTimeout(), Integer.class);
      objs[1] = new NameValuePair(completeErrors,
              CollectionProcessCompleteErrors.KIND_ADDITIONA_ERROR_ACTION,
              CollectionProcessCompleteErrors.NAME_ADDITIONA_ERROR_ACTION, 
              completeErrors.getAdditionalErrorAction(), String.class);
      if (completeErrors.getParent() instanceof AsyncPrimitiveErrorConfiguration) {
        ((NameValuePair) objs[1]).setStatusFlags(NameValuePair.STATUS_NON_EDITABLE);
      }
      return objs;

    }

    return EMPTY_ARRAY;
  }

  public void dispose() {
  }

  public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
  }

}
