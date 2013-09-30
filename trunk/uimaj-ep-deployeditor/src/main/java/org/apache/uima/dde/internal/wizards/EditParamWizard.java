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

import org.apache.uima.dde.internal.Messages;
import org.eclipse.jface.wizard.IWizard;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;

public class EditParamWizard extends Wizard implements IWizard {
  private String _title;

  private String _description;

  private String name = "";

  private String value = "";

  private EditParamWizardPage wizardPage;

  /**
   * Creates a wizard-like to set name/value
   */
  public EditParamWizard(String title, String description, String name, String value) {
    _title = title;
    _description = description;
    this.name = name;
    this.value = value;
    setWindowTitle(Messages.DDE_EnvVariable_Wizard_Window_Title);
  }

  public void addPages() {
    super.addPages();
    wizardPage = new EditParamWizardPage(_title, _description, name, value);
    addPage(wizardPage);
    Rectangle rc = Display.getCurrent().getBounds();
    this.getShell().setBounds((rc.width-480)/2, (rc.height-300)/2, 480, 300);
  }

  public String getName () {
    return name;
  }
  public String getValue() {
    return value;
  }

  public boolean performCancel() {
    return true;
  }

  public boolean performFinish() {
    name = wizardPage.getName().trim();
    value = wizardPage.getValue().trim();
    return true;
  } // performFinish

}