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
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.aae.deployment.impl.AEDelegates_Impl;
import org.apache.uima.aae.deployment.impl.AsyncAggregateErrorConfiguration_Impl;
import org.apache.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration_Impl;
import org.eclipse.jface.viewers.IColorProvider;
import org.eclipse.jface.viewers.IFontProvider;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Image;
import org.eclipse.ui.dialogs.FilteredTree;
import org.eclipse.ui.dialogs.PatternFilter;


public class DDTreeLabelProvider extends LabelProvider implements IColorProvider,
        ITableLabelProvider, IFontProvider {
  // Used by FilteredTree to return Bold Face font
  private FilteredTree fFilteredTree;

  private PatternFilter fPatternFilter;

  public DDTreeLabelProvider() {
    super();
  }

  public void setFilteredTree(FilteredTree fFilteredTree, PatternFilter fPatternFilter) {
    this.fFilteredTree = fFilteredTree;
    this.fPatternFilter = fPatternFilter;
  }

  public String getText(Object obj) {
    if (obj instanceof AEDeploymentMetaData) {
      String key = ((AEDeploymentMetaData) obj).getKey();
      if (key == null) {
        if (((AEDeploymentMetaData) obj).isTopAnalysisEngine()) {
          key = "Top Analysis Engine";
        } else {
          key = "(no name)";
        }
      }
      return key;

    } else if (obj instanceof RemoteAEDeploymentMetaData) {
      return ((RemoteAEDeploymentMetaData) obj).getKey();

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
    return null;
  }

  public Image getColumnImage(Object element, int columnIndex) {
    return null;
  }

  final static int COLUMN_NAME = 0;

  final static int COLUMN_REMOTE = 1;

  final static int COLUMN_INSTANCESC = 2;

  public String getColumnText(Object obj, int index) {
    if (index == COLUMN_NAME) {
      return getText(obj);

    } else if (index == COLUMN_REMOTE) {
      if (obj instanceof RemoteAEDeploymentMetaData) {
        return "true";
      }

    } else if (index == COLUMN_INSTANCESC) {
      if (obj instanceof AEDeploymentMetaData) {
        if ( ! ((AEDeploymentMetaData) obj).isAsync() ) { 
          return ""+((AEDeploymentMetaData) obj).getNumberOfInstances();
        }
      } else if (obj instanceof RemoteAEDeploymentMetaData) {
        // return "1";
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.jface.viewers.IFontProvider#getFont(java.lang.Object)
   */
  public Font getFont(Object element) {
    if (fFilteredTree != null && fPatternFilter != null) {
      return FilteredTree.getBoldFont(element, fFilteredTree, fPatternFilter);
    } else {
      return null;
    }
  }

}
