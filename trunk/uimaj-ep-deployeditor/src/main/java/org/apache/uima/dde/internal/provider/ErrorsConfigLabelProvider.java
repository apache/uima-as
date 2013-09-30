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

import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration;
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.aae.deployment.impl.AEDelegates_Impl;
import org.apache.uima.aae.deployment.impl.AsyncAggregateErrorConfiguration_Impl;
import org.apache.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration_Impl;
import org.apache.uima.aae.deployment.impl.DeploymentMetaData_Impl;
import org.eclipse.jface.viewers.IColorProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;


public class ErrorsConfigLabelProvider extends LabelProvider implements IColorProvider,
        ITableLabelProvider {
  
  protected TableViewer viewer;
  
  public ErrorsConfigLabelProvider(TableViewer viewer) {
    super();
    this.viewer = viewer;
  }

  public String getText(Object obj) {
    if (obj instanceof AEDeploymentMetaData) {
      String key = ((AEDeploymentMetaData) obj).getKey();
      return (key == null ? "no name" : key);
      // + " ; async=" + ((AEDeploymentMetaData) obj).isAsync();

    } else if (obj instanceof RemoteAEDeploymentMetaData) {
      return "Remote " + ((RemoteAEDeploymentMetaData) obj).getKey();

    } else if (obj instanceof AEDelegates_Impl) {
      return "Delegates";

    } else if (obj instanceof AsyncAggregateErrorConfiguration_Impl) {
      return "AsyncAggregateErrorConfiguration";

    } else if (obj instanceof AsyncPrimitiveErrorConfiguration_Impl) {
      return "AsyncPrimitiveErrorConfiguration";

    } else if (obj instanceof GetMetadataErrors) {
      return "GetMetadataErrors";

    } else if (obj instanceof ProcessCasErrors) {
      return "ProcessCasErrors";

    } else if (obj instanceof CollectionProcessCompleteErrors) {
      return "CollectionProcessCompleteErrors";

    } else if (obj instanceof AEDeploymentDescription) {

    }

    if (obj != null) {
      return obj.toString();
    } else {
      return "";
    }
  }

  public Color getBackground(Object element) {
    return null;
  }

  public Color getForeground(Object element) {
    if (element instanceof NameValuePair) {
//      int id = ((NameValuePair) element).getId();
//      Object obj = ((NameValuePair) element).getParent();
//      if (obj instanceof GetMetadataErrors) {
//        DeploymentMetaData_Impl metaData = ((GetMetadataErrors) obj).getParent().gParentObject();
//        if (metaData instanceof AEDeploymentMetaData) {
//          return viewer.getControl().getShell().getDisplay().getSystemColor(SWT.COLOR_WIDGET_NORMAL_SHADOW);
//        }
//      }
      if (((NameValuePair) element).getStatusFlags() == NameValuePair.STATUS_NON_EDITABLE) {
        return viewer.getControl().getShell().getDisplay().getSystemColor(SWT.COLOR_WIDGET_NORMAL_SHADOW);
      }
    }
    return null;
  }

  public Image getColumnImage(Object element, int columnIndex) {
    return null;
  }

  final static int COLUMN_NAME = 0;

  final static int COLUMN_VALUE = 1;

  public String getColumnText(Object obj, int index) {
    if (index == COLUMN_NAME) {
      if (obj instanceof NameValuePair) {
        int id = ((NameValuePair) obj).getId();

        if ( ((NameValuePair) obj).getParent() instanceof ProcessCasErrors) {
          // Blank out Threshold Window and Action if Threshold Count = 0
          ProcessCasErrors pce = (ProcessCasErrors) ((NameValuePair) obj).getParent();
          if (pce.getThresholdCount() == 0) {
            if (id == ProcessCasErrors.KIND_THRESHOLD_WINDOW
                    || id == ProcessCasErrors.KIND_THRESHOLD_ACTION) {
              return "";
            }
          }
        } else if ( ((NameValuePair) obj).getParent() instanceof GetMetadataErrors) {
          // Blank out Error Action if Timeout = 0
          GetMetadataErrors gme = (GetMetadataErrors) ((NameValuePair) obj).getParent();
          if (gme.getTimeout() == 0 && id == GetMetadataErrors.KIND_ERRORACTION) {
            return "";
          }
        }
        return ((NameValuePair) obj).getName();
      }
      return getText(obj);

    } else if (index == COLUMN_VALUE) {
      if (obj instanceof NameValuePair) {
        int id = ((NameValuePair) obj).getId();
        
        if ( ((NameValuePair) obj).getParent() instanceof GetMetadataErrors) {
          if (id == GetMetadataErrors.KIND_TIMEOUT) {
            if ( ((NameValuePair) obj).getValue() == Integer.valueOf(0) ) {
              return AEDeploymentConstants.ERROR_KIND_STRING_NO_TIMEOUT;              
            }
          } else if (id == GetMetadataErrors.KIND_MAX_RETRIES) {
            if ( ((NameValuePair) obj).getValue() == Integer.valueOf(0) ) {
              return AEDeploymentConstants.ERROR_KIND_STRING_NO_RETRIES;              
            }
          } else if (id == GetMetadataErrors.KIND_ERRORACTION) {
            // If Timeout=0, blank out action
            GetMetadataErrors gme = (GetMetadataErrors) ((NameValuePair) obj).getParent();
            if (gme.getTimeout() == 0) {
              return "";
            }

          }

        } else if ( ((NameValuePair) obj).getParent() instanceof ProcessCasErrors) {
          ProcessCasErrors pce = (ProcessCasErrors) ((NameValuePair) obj).getParent();
          if (id == ProcessCasErrors.KIND_TIMEOUT) {
            if ( ((NameValuePair) obj).getValue() == Integer.valueOf(0) ) {
              return AEDeploymentConstants.ERROR_KIND_STRING_NO_TIMEOUT;              
            }
          } else if (id == ProcessCasErrors.KIND_MAX_RETRIES) {
            if ( ((NameValuePair) obj).getValue() == Integer.valueOf(0) ) {
              return AEDeploymentConstants.ERROR_KIND_STRING_NO_RETRIES;              
            }
          } else if (id == ProcessCasErrors.KIND_THRESHOLD_COUNT) {
            if ( ((NameValuePair) obj).getValue() == Integer.valueOf(0) ) {
              return AEDeploymentConstants.ERROR_KIND_STRING_NO_THRESHOLD_COUNT;              
            }
          } else if (id == ProcessCasErrors.KIND_THRESHOLD_WINDOW) {
            // Blank out Threshold Window and Action if Threshold Count = 0
            if (pce.getThresholdCount() == 0) {
              return "";
            } else {
              if ( ((NameValuePair) obj).getValue() == Integer.valueOf(0) ) {
                return AEDeploymentConstants.ERROR_KIND_STRING_NO_THRESHOLD_WINDOW;              
              }
            }
          } else if (id == ProcessCasErrors.KIND_THRESHOLD_ACTION) {
            // Blank out Threshold Window and Action if Threshold Count = 0
            if (pce.getThresholdCount() == 0) {
              return "";
            }
          }
          
        } else if ( ((NameValuePair) obj).getParent() instanceof CollectionProcessCompleteErrors) {
          if (id == CollectionProcessCompleteErrors.KIND_TIMEOUT) {
            if ( ((NameValuePair) obj).getValue() == Integer.valueOf(0) ) {
              return AEDeploymentConstants.ERROR_KIND_STRING_NO_TIMEOUT;              
            }
          }
          
        }
        return ((NameValuePair) obj).getValue().toString();
      }

    }
    return null;
  }

}
