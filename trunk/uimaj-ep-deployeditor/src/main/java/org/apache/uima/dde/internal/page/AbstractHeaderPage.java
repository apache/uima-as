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

package org.apache.uima.dde.internal.page;

import org.apache.uima.taeconfigurator.TAEConfiguratorPlugin;
import org.apache.uima.tools.images.internal.ImageLoader;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.forms.IManagedForm;
import org.eclipse.ui.forms.editor.FormEditor;
import org.eclipse.ui.forms.editor.FormPage;
import org.eclipse.ui.forms.events.HyperlinkAdapter;
import org.eclipse.ui.forms.events.HyperlinkEvent;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.ImageHyperlink;
import org.eclipse.ui.forms.widgets.ScrolledForm;
import org.eclipse.ui.forms.widgets.Section;


/**
 * Abstract class inherited by all editor pages
 * 
 */
public abstract class AbstractHeaderPage extends FormPage {
  static public final int STATUS_IS_VALID = 0;

  static public final int STATUS_IS_INVALID = -1;

  static public final int STATUS_FILE_NOT_SPECIFIED = 1;

  static public final int STATUS_FILE_NOT_EXIST = 2;

  /**
   * @param editor
   * @param id
   * @param title
   */
  public AbstractHeaderPage(FormEditor editor, String id, String title) {
    super(editor, id, title);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param id
   * @param title
   */
  public AbstractHeaderPage(String id, String title) {
    super(id, title);
    // TODO Auto-generated constructor stub
  }

  public void setFormPageTitle(ScrolledForm sform, String title) {
    sform.setText(title);
    // sform.setBackgroundImage(RepositoryPlugin.getDefault().getImage(
    // RepositoryPlugin.IMG_FORM_BG));
  }

  static public void createExpandOrCollapseAllMenu(FormToolkit toolkit, Section section,
          Composite toolbarComposite, final TreeViewer treeViewer, boolean expand) {
    String toolTipText;
    String iconKey;

    if (expand) {
      toolTipText = "Expand All";
      iconKey = ImageLoader.ICON_SEARCH_EXPANDALL;
    } else {
      toolTipText = "Collapse All";
      iconKey = ImageLoader.ICON_SEARCH_COLLAPSEALL;
    }
    ImageHyperlink info = new ImageHyperlink(toolbarComposite, SWT.NULL);
    toolkit.adapt(info, true, true);
    Image image = ImageLoader.getInstance().getImage(iconKey);
    info.setImage(image);
    info.setToolTipText(toolTipText);
    // info.setBackground(section.getTitleBarGradientBackground());
    if (expand) {
      info.addHyperlinkListener(new HyperlinkAdapter() {
        public void linkActivated(HyperlinkEvent e) {
          treeViewer.expandAll();
        }
      });
    } else {
      info.addHyperlinkListener(new HyperlinkAdapter() {
        public void linkActivated(HyperlinkEvent e) {
          treeViewer.collapseAll();
        }
      });
    }
  } // createExpandOrCollapseAllMenu

  static protected void createToolBarActions(IManagedForm managedForm, final SashForm sashForm,
          boolean selectHorizontal) {
    final ScrolledForm form = managedForm.getForm();

    Action haction = new Action("hor", Action.AS_RADIO_BUTTON) { //$NON-NLS-1$
      public void run() {
        sashForm.setOrientation(SWT.HORIZONTAL);
        form.reflow(true);
      }
    };
    haction.setChecked(selectHorizontal);
    haction.setToolTipText("Horizontal Orientation");
    haction.setImageDescriptor(TAEConfiguratorPlugin.getImageDescriptor(TAEConfiguratorPlugin.IMAGE_TH_HORIZONTAL));
    haction.setDisabledImageDescriptor(TAEConfiguratorPlugin
            .getImageDescriptor(TAEConfiguratorPlugin.IMAGE_TH_HORIZONTAL));

    Action vaction = new Action("ver", Action.AS_RADIO_BUTTON) { //$NON-NLS-1$
      public void run() {
        sashForm.setOrientation(SWT.VERTICAL);
        form.reflow(true);
      }
    };
    vaction.setChecked(!selectHorizontal);
    vaction.setToolTipText("Vertical Orientation");
    vaction.setImageDescriptor(TAEConfiguratorPlugin.getImageDescriptor(TAEConfiguratorPlugin.IMAGE_TH_VERTICAL));
    vaction.setDisabledImageDescriptor(TAEConfiguratorPlugin
            .getImageDescriptor(TAEConfiguratorPlugin.IMAGE_TH_VERTICAL));
    form.getToolBarManager().add(haction);
    form.getToolBarManager().add(vaction);
    form.updateToolBar();
  }

} // AbstractHeaderPage
