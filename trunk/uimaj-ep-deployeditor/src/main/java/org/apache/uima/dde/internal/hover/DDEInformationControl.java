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

package org.apache.uima.dde.internal.hover;

import java.util.Map;
import java.util.Set;

import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.tools.debug.util.Trace;
import org.eclipse.jface.dialogs.PopupDialog;
import org.eclipse.jface.resource.JFaceResources;
import org.eclipse.jface.text.IInformationControl;
import org.eclipse.jface.text.IInformationControlExtension2;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.forms.FormColors;
import org.eclipse.ui.forms.widgets.FormText;


public class DDEInformationControl extends PopupDialog implements IInformationControl,
        IInformationControlExtension2 {
  private Font boldTextFont;

  /** ********************************************************************** */

  public DDEInformationControl(Shell parent, int shellStyle, int treeStyle) {
    this(parent, shellStyle, treeStyle, null, false);
  }

  public DDEInformationControl(Shell parent, int shellStyle, int treeStyle,
          String invokingCommandId, boolean showStatusField) {
    super(parent, shellStyle | SWT.NO_FOCUS | SWT.ON_TOP, false, false, false, false, null,
            null);
    create();
    boldTextFont = createBoldFont(getShell().getDisplay(), JFaceResources.getTextFont());
  }

  public void setVisible(boolean visible) {

    if (visible) {
      open();
    } else {
      saveDialogBounds(getShell());
      getShell().setVisible(false);
    }
  }

  /** ********************************************************************** */

  public void addDisposeListener(DisposeListener listener) {
    // TODO Auto-generated method stub

  }

  public void addFocusListener(FocusListener listener) {
    // TODO Auto-generated method stub

  }

  public Point computeSizeHint() {
    int widthHint = 400; // SWT.DEFAULT;

    if (getShell() != null) {
      Point pt = getShell().computeSize(widthHint, SWT.DEFAULT, true);
      // Trace.err("computeSizeHint: " + pt.toString());
      return pt;
    } else {
      // Trace.err("getShell() == null");
      return new Point(200, 100);
    }
  }

  public void dispose() {
    if (boldTextFont != null) {
      boldTextFont.dispose();
    }
  }

  public boolean isFocusControl() {
    return false;
  }

  public void removeDisposeListener(DisposeListener listener) {
    // TODO Auto-generated method stub

  }

  public void removeFocusListener(FocusListener listener) {
    // TODO Auto-generated method stub

  }

  public void setBackgroundColor(Color background) {
    // TODO Auto-generated method stub

  }

  public void setFocus() {
    // TODO Auto-generated method stub

  }

  public void setForegroundColor(Color foreground) {
    // TODO Auto-generated method stub

  }

  public void setInformation(String information) {
    formText.setText(information, true, false);
  }

  public void setLocation(Point location) {
    // Trace.err(location.toString());
    getShell().setLocation(location);
  }

  public void setSize(int width, int height) {
    getShell().setSize(width, height);
    // Trace.err("width: " + width + " height:" + height);
  }

  public void setSizeConstraints(int maxWidth, int maxHeight) {
  }

  public void setInput(Object input) {
    StringBuffer buf = new StringBuffer();
    if (input != null) {
      // Trace.err("input: " + input.getClass().getName());
      if (input instanceof String) {
        formText.setColor("header", formColors.getColor(FormColors.TITLE));
        // formText.setFont("header", JFaceResources.getHeaderFont());
        formText
                .setFont("header", JFaceResources.getFontRegistry().get(JFaceResources.WINDOW_FONT));
        formText.setFont("code", JFaceResources.getTextFont());
        formText.setText(buf.toString(), true, false);
      } else if (input instanceof TreeItem && ((TreeItem) input).getData() != null) {
//        TreeItem item = (TreeItem) input;
        Object obj = ((TreeItem) input).getData();
        String text = null;
        if (obj instanceof AEDeploymentMetaData) {
          text = toStringFromAEDeploymentMetaData((AEDeploymentMetaData) obj);

        } else if (obj instanceof RemoteAEDeploymentMetaData) {
          text = toStringFromRemoteAEDeploymentMetaData((RemoteAEDeploymentMetaData) obj);

        } else if (obj instanceof AEDeploymentDescription) {
          text = toStringFromAEDeploymentDescription((AEDeploymentDescription) obj);
        
        } else if (obj instanceof KeyValuePair) {
          AEDeploymentDescription dd = (AEDeploymentDescription) ((KeyValuePair) obj).getValue();
          text = toStringFromAEDeploymentDescription(dd);
        }
        if (text != null) {
          formText.setColor("header", formColors.getColor(FormColors.TITLE));
          formText.setFont("header", boldTextFont);
          formText.setText(text, true, false);
        }
      }
    }
  }

  protected String toStringFromAEDeploymentDescription(AEDeploymentDescription obj) {
    StringBuffer buf = new StringBuffer();
    buf.append("<form><p><span color=\"header\" font=\"header\">");
    buf.append(obj.getName() + "</span>");
    buf.append("</p>");
    
    // Description
    if (obj.getDescription().trim().length() > 0) {
      buf.append("<p>" + obj.getDescription());
      buf.append("</p>");
    }

    buf.append("</form>");
    return buf.toString();
  }


  protected String toStringFromAEDeploymentMetaData(AEDeploymentMetaData obj) {
    StringBuffer buf = new StringBuffer();
    buf.append("<form><p><span color=\"header\" font=\"header\">");
    buf.append(obj.getKey() + "</span>");
    if (obj.isAsync()) {
      buf.append(" - Async Aggregate");
    }
    buf.append("</p>");
    
    if (obj.isAsync() && obj.getResourceSpecifier() instanceof AnalysisEngineDescription) {
      AnalysisEngineDescription aed = (AnalysisEngineDescription) obj.getResourceSpecifier();
      if (!aed.isPrimitive()) {
        buf.append("<p><b>Delegates</b>\n");
        Map map = aed.getDelegateAnalysisEngineSpecifiersWithImports();
        Set<String> keys = map.keySet();
        for (String key: keys) {
          buf.append(" - " + key);
        }
        buf.append("</p>");
      }
    }

    // Number of Replicated Instances
    if (!obj.isTopAnalysisEngine()) {
      buf.append("<p>Number of Replicated Instances: " + obj.getNumberOfInstances());
      buf.append("</p>");
    }
    
    if (obj.isCasMultiplier()) {
      buf.append("<p>CasMultiplierPoolSize: " + obj.getCasMultiplierPoolSize());
      buf.append("</p>");
    }

    // Error Config
    toStringFromErrorConfig(buf, obj.getAsyncAEErrorConfiguration());

    buf.append("</form>");
    return buf.toString();
  }
  
  protected String toStringFromRemoteAEDeploymentMetaData(RemoteAEDeploymentMetaData obj) {
    StringBuffer buf = new StringBuffer();
    buf.append("<form><p><span color=\"header\" font=\"header\">");
    buf.append(obj.getKey() + "</span> - Remote Service</p>");
    buf.append("<li><b>Broker URL:</b> "
            + (obj.getInputQueue() != null ? obj.getInputQueue().getBrokerURL() : "") + "</li>");
    buf.append("<li><b>Queue Name:</b> "
            + (obj.getInputQueue() != null ? obj.getInputQueue().getEndPoint() : "") + "</li>");
    if (obj.getRemoteReplyQueueScaleout() > 0) {
      buf.append("<li><b>Concurrent Consumers for reply queue:</b> " + obj.getRemoteReplyQueueScaleout() + "</li>");
    }
    // Error Config
    toStringFromErrorConfig(buf, obj.getAsyncAEErrorConfiguration());

    buf.append("</form>");
    return buf.toString();
  }

  protected void toStringFromErrorConfig(StringBuffer buf, AsyncAEErrorConfiguration obj) {
    if (obj == null) {
      return;
    }
    boolean isAsyncAggErrorConfig = (obj instanceof AsyncAggregateErrorConfiguration);
    
    GetMetadataErrors getGetMetadataErrors = obj.getGetMetadataErrors();
    if (getGetMetadataErrors != null) {
      buf.append("<li><b>GetMetadataErrors</b></li>");
      buf.append("<li bindent=\"20\">MaxRetries: " + getGetMetadataErrors.getMaxRetries()
              + " ; Timeout: " + getGetMetadataErrors.getTimeout() + " ; ErrorAction: "
              + getGetMetadataErrors.getErrorAction() + "</li>");
    }

    ProcessCasErrors processCasErrors = obj.getProcessCasErrors();
    if (processCasErrors != null) {
      buf.append("<li><b>ProcessCasErrors</b></li>");
      if (isAsyncAggErrorConfig) {
        buf.append("<li bindent=\"20\">CAS Max Retries: " + processCasErrors.getMaxRetries()
              + " ; CAS Timeout: " + processCasErrors.getTimeout()
              + " ; CAS Continue On Failure: " + processCasErrors.isContinueOnRetryFailure() + "</li>");
        buf.append("<li bindent=\"20\">Delegate Threshold Count: " + processCasErrors.getThresholdCount()
                + " ; Delegate Threshold Window: " + processCasErrors.getThresholdWindow()
                + " ; Delegate Threshold Action: " + processCasErrors.getThresholdAction() + "</li>");
      } else {
        buf.append("<li bindent=\"20\">Threshold Count: " + processCasErrors.getThresholdCount()
                + " ; Threshold Window: " + processCasErrors.getThresholdWindow()
                + " ; Threshold Action: " + processCasErrors.getThresholdAction() + "</li>");
        
      }
    }

    CollectionProcessCompleteErrors collProcessCompleteErrors = obj
            .getCollectionProcessCompleteErrors();
    if (collProcessCompleteErrors != null) {
      buf.append("<li><b>CollectionProcessCompleteErrors</b></li>");
      buf.append("<li bindent=\"20\">Timeout: " + collProcessCompleteErrors.getTimeout()
              + " ; Additional Error Action: "
              + collProcessCompleteErrors.getAdditionalErrorAction() + "</li>");
    }

  }

  public static Font createBoldFont(Display display, Font regularFont) {
    FontData[] fontDatas = regularFont.getFontData();
    for (int i = 0; i < fontDatas.length; i++) {
      fontDatas[i].setStyle(fontDatas[i].getStyle() | SWT.BOLD);
    }
    return new Font(display, fontDatas);
  }

  /** ********************************************************************** */

  private static final int INNER_BORDER = 1;

  private FormText formText;

  private FormColors formColors;

  protected Control createDialogArea(Composite parent) {
    formColors = new FormColors(parent.getDisplay());
    parent.setLayout(new GridLayout());
    // Text field
    formText = new FormText(parent, SWT.MULTI | SWT.READ_ONLY);
    GridData gd = new GridData(GridData.BEGINNING | GridData.FILL_BOTH);
    gd.horizontalIndent = INNER_BORDER;
    gd.verticalIndent = INNER_BORDER;
    formText.setLayoutData(gd);
    formText.addKeyListener(new KeyListener() {

      public void keyPressed(KeyEvent e) {
        if (e.character == 0x1B) // ESC
          close();
      }

      public void keyReleased(KeyEvent e) {
      }
    });
    return formText;
  }

}
