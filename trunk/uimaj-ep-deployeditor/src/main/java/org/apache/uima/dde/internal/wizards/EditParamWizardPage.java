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
package org.apache.uima.dde.internal.wizards;

import org.apache.uima.tools.internal.ui.forms.FormSection;
import org.eclipse.jface.dialogs.Dialog;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.Section;

public class EditParamWizardPage extends WizardPage {

  private String name = "";

  private String value = "";

  private Text _name;

  private Text _value;

  private boolean _complete = false;

  public EditParamWizardPage(String title, String description, String name, String value) {
    super(title);
    setTitle(title);
    setDescription(description);
    this.name = name;
    this.value = value;

    _complete = true;
  }

  public String getName() {
    return _name.getText().trim();
  }

  public String getValue() {
    return _value.getText().trim();
  }

  /** *********************************************************************** */

  private void updatePageComplete(boolean complete) {
    setPageComplete(complete);
  } // updatePageComplete

  public void createControl(Composite parent) {
    Composite composite = new Composite(parent, SWT.NULL);
    GridLayout layout = new GridLayout();
    layout.marginWidth = 5;
    layout.marginHeight = 5;
    composite.setLayout(layout);
    composite.setLayoutData(new GridData(GridData.VERTICAL_ALIGN_FILL
            | GridData.HORIZONTAL_ALIGN_FILL));

    FormToolkit toolkit = new FormToolkit(composite.getDisplay());
    createContents(composite, toolkit);

    toolkit.paintBordersFor(composite);
    setControl(composite);
    Dialog.applyDialogFont(composite);

    updatePageComplete(_complete);
  } // createControl

  public void createContents(Composite parent, FormToolkit toolkit) {
    GridLayout layout = new GridLayout();
    layout.numColumns = 1;
    layout.marginWidth = 5;
    layout.marginHeight = 5;
    parent.setLayout(layout);

    createNameValueFields(parent, toolkit);
  }

  private Section createNameValueFields(Composite parent, FormToolkit toolkit) {
    GridData gd = new GridData(GridData.FILL_BOTH);
    Composite info = toolkit.createComposite(parent);
    info.setLayoutData(gd);
    GridLayout gl = new GridLayout(2, false);
    info.setLayout(gl);

    // Name
    _name = FormSection.createLabelAndText(toolkit, info, "Name:", name, SWT.SINGLE, 10, 0);
    _name.setEnabled(true);

    _value = FormSection.createLabelAndText(toolkit, info, "Value:", value, SWT.SINGLE, 10, 0);
    _value.setEnabled(true);
    
    _name.setFocus();

    toolkit.paintBordersFor(info);

    return null;
  }

} // EditParamWizardPage
