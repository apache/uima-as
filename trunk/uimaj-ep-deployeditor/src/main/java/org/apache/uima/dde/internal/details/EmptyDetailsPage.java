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

package org.apache.uima.dde.internal.details;

import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.tools.internal.ui.forms.FormSection;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.ui.forms.IDetailsPage;
import org.eclipse.ui.forms.IFormPart;
import org.eclipse.ui.forms.IManagedForm;
import org.eclipse.ui.forms.SectionPart;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.ScrolledPageBook;
import org.eclipse.ui.forms.widgets.Section;
import org.eclipse.ui.forms.widgets.TableWrapData;
import org.eclipse.ui.forms.widgets.TableWrapLayout;


public class EmptyDetailsPage implements IDetailsPage {
  private Object selectedObject = null;

  private IManagedForm mform;

  private ScrolledPageBook myScrolledPageBook = null; // used to support Section

  /** ********************************************************************** */

  public EmptyDetailsPage(IManagedForm mform) {
    this.mform = mform;
  }

  /** ********************************************************************** */

  //
  // Note: "parent" is a "LayoutComposite" created by "ScrolledPageBook pageBook"
  // in "DetailsPart".
  // The parent of "parent" is "WrappedPageBook pageBook".
  // The grand-parent of "parent" is ScrolledPageBook which needs to be
  // "reflowed" when Section is expanded/collapsed.
  public void createContents(Composite parent) {
    Trace.err();
    // Get ScrolledPageBook
    myScrolledPageBook = (ScrolledPageBook) parent.getParent().getParent();

    // Set Layout for "parent"
    TableWrapLayout layout = new TableWrapLayout();
    layout.topMargin = 0;
    layout.leftMargin = 5;
    layout.rightMargin = 2;
    layout.bottomMargin = 2;
    parent.setLayout(layout);

    FormToolkit toolkit = mform.getToolkit();

    createIdentitySection(parent, toolkit);
  }

  private Section createIdentitySection(Composite parent, FormToolkit toolkit) {
    Section section = FormSection.createTableWrapDataSection(toolkit, parent, Section.DESCRIPTION
            | Section.TWISTIE | Section.EXPANDED, "No Details are available", "No information is available.",
            10, 5, TableWrapData.FILL_GRAB, TableWrapData.FILL_GRAB, 1, 1);
    final SectionPart spart = new SectionPart(section);
    mform.addPart(spart);
    spart.initialize(mform); // Need this code. Otherwise, exception in SectionPart !!!
    section.setExpanded(true);

    // /////////////////////////////////////////////////////////////////////

    Composite sectionClient = toolkit.createComposite(section);
    section.setClient(sectionClient);
    TableWrapLayout tl = new TableWrapLayout();
    tl.numColumns = 2;
    tl.leftMargin = 10;
    tl.rightMargin = 10;
    tl.topMargin = 10;
    tl.bottomMargin = 10;
    sectionClient.setLayout(tl);

    // /////////////////////////////////////////////////////////////////////

    return section;
  }

  protected void displayDetails(RemoteAEDeploymentMetaData obj) {
    // textBrokerURL.setText(obj.getInputQueue().getBrokerURL());
    // textEndPoint.setText(obj.getInputQueue().getEndPoint());
    //        
    // if (obj.getReplyQueueLocation() != null) {
    // int i = remoteQueueLocation.indexOf(obj.getReplyQueueLocation());
    // if (i >= 0) {
    // remoteQueueLocation.select(i);
    // }
    // }
  }

  /** ********************************************************************** */

  public void commit(boolean onSave) {
    // TODO Auto-generated method stub

  }

  public void dispose() {
    // TODO Auto-generated method stub

  }

  public void initialize(IManagedForm form) {
    this.mform = form;
  }

  public boolean isDirty() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isStale() {
    // TODO Auto-generated method stub
    return false;
  }

  public void refresh() {
    // TODO Auto-generated method stub

  }

  public void setFocus() {
    // TODO Auto-generated method stub

  }

  public boolean setFormInput(Object input) {
    // TODO Auto-generated method stub
    return false;
  }

  public void selectionChanged(IFormPart part, ISelection selection) {
    if (selection == null || !(selection instanceof IStructuredSelection)) {
      return;
    }
    IStructuredSelection ssel = (IStructuredSelection) selection;
    if (ssel.size() != 1) {
      return;
    }

    selectedObject = ssel.getFirstElement();
    if (selectedObject instanceof RemoteAEDeploymentMetaData) {
      displayDetails((RemoteAEDeploymentMetaData) selectedObject);
    }
  }

}
