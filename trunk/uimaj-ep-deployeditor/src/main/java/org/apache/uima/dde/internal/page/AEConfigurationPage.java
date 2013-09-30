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

import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.dde.internal.DeploymentDescriptorEditor;
import org.eclipse.ui.forms.IManagedForm;
import org.eclipse.ui.forms.widgets.ScrolledForm;


/**
 * 
 * 
 */
public class AEConfigurationPage extends AbstractHeaderPage {

  static public final String PAGE_TITLE = "Deployment Configurations";

  private ScrolledForm form;

  private MasterDetails md;

  private DeploymentDescriptorEditor multiPageEditor;

  private AEDeploymentDescription input;

  /**
   * @param editor
   * @param id
   * @param title
   */
  public AEConfigurationPage(DeploymentDescriptorEditor editor, String id, String title) {
    super(editor.cde, id, title);
    multiPageEditor = editor;

  }

  /**
   * @param id
   * @param title
   */
  public AEConfigurationPage(String id, String title) {
    super(id, title);
    // TODO Auto-generated constructor stub
  }

  public void refresh() {
    md.refresh();
  }

  public void setInput(Object input) {
    if (input instanceof AEDeploymentDescription) {
      if (md != null) {
        md.setInput(input);
      } else {
        this.input = (AEDeploymentDescription) input;
      }
    }
  }

  /**
   * Called by the framework to fill in the contents
   */
  protected void createFormContent(IManagedForm managedForm) {
    super.createFormContent(managedForm);
    form = managedForm.getForm();
    form.setText(PAGE_TITLE);

    md = new MasterDetails(multiPageEditor, managedForm, getSite());
    md.createContent(managedForm);
    md.init(); // will trigger the display of the details page
    if (input != null) {
      md.setInput(input);
    }
  }
}
