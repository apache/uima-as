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

import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData;
import org.apache.uima.aae.deployment.impl.DeploymentMetaData_Impl;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.dde.internal.DeploymentDescriptorEditor;
import org.apache.uima.dde.internal.Messages;
import org.apache.uima.dde.internal.hover.GenericHoverManager;
import org.apache.uima.dde.internal.hover.IGenericHoverOwner;
import org.apache.uima.dde.internal.page.MasterDetails;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.MetaDataObject;
import org.apache.uima.taeconfigurator.editors.ui.Utility;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.tools.internal.ui.forms.FormSection;
import org.apache.uima.tools.internal.ui.forms.FormSection2;
import org.apache.uima.tools.internal.uima.util.AETreeBuilder;
import org.apache.uima.tools.internal.uima.util.FormMessage;
import org.apache.uima.util.InvalidXMLException;
import org.eclipse.jface.dialogs.IMessageProvider;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.fieldassist.DecoratedField;
import org.eclipse.jface.fieldassist.FieldDecoration;
import org.eclipse.jface.fieldassist.FieldDecorationRegistry;
import org.eclipse.jface.text.DefaultInformationControl;
import org.eclipse.jface.text.IInformationControl;
import org.eclipse.jface.text.IInformationControlCreator;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.forms.AbstractFormPart;
import org.eclipse.ui.forms.FormColors;
import org.eclipse.ui.forms.IDetailsPage;
import org.eclipse.ui.forms.IFormPart;
import org.eclipse.ui.forms.IManagedForm;
import org.eclipse.ui.forms.SectionPart;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.ScrolledPageBook;
import org.eclipse.ui.forms.widgets.Section;
import org.eclipse.ui.forms.widgets.TableWrapData;
import org.eclipse.ui.forms.widgets.TableWrapLayout;


public class AEMetaDataDetailsPage extends AbstractFormPart implements IDetailsPage {
  private DeploymentDescriptorEditor multiPageEditor;
  
  private MasterDetails masterPart;

  private boolean isDelegate;

  private Object selectedObject = null;

  private IManagedForm mform;

  private Section sectionAEMetaDataDetails;

  private Button asMode;

  private Spinner scaleout;

  private Label casMultiplierLabelRemote;

  private Spinner casMultiplierRemote;

  private Label casMultiplierLabel;

  private Spinner casMultiplier;

  private Label initialFsHeapSizeLabel;

  private Spinner initialFsHeapSize;

  private Label initialFsHeapSizeLabelRemote;

  private Spinner initialFsHeapSizeRemote;
  
  // private Label processParentCASLastLabel;

  protected Button processParentCASLast;

  private ErrorConfigDetailsPage errorConfigDetails;

  private Button deploymentCoLocated;

  private Button deploymentRemote;

  private StackLayout stackLayout;

  private Composite stackLayoutComposite;

  private Composite compositeCoLocatedSetting;

  private Composite compositeRemoteSetting;

  private MetaDataObject currentMetaDataObject;

  protected Label labelInputQueueScaleout;

  protected Spinner inputQueueScaleout;

  protected Label labelReplyQueueForCoLocated;

  protected Spinner replyQueueListenersForCoLocated;
  
  // For Remote Deployment

  private DecoratedField brokerUrlDecoField;
  
  private DecoratedField endPointDecoField;

  private FieldDecoration decorationBrokerUrl;

  private FieldDecoration decorationEndPoint;

  protected Text brokerUrl;

  protected Text endPoint;

  protected CCombo remoteQueueLocation;
  
  protected Label labelRemoteReplyQueueScaleout;

  protected Spinner remoteReplyQueueScaleout;

  protected Label serializerMethod;

  private boolean ignoreUpdate = false; // Used to update Text without setting dirty flag
  
  /** ********************************************************************** */

  private ModifyListener fModifyListener = new ModifyListener() {

    public void modifyText(ModifyEvent e) {
      Object source = e.getSource();

      if (source == brokerUrl) {
        ((RemoteAEDeploymentMetaData) currentMetaDataObject).getInputQueue().setBrokerURL(brokerUrl.getText().trim());

      } else if (source == endPoint) {
        ((RemoteAEDeploymentMetaData) currentMetaDataObject).getInputQueue().setEndPoint(endPoint.getText().trim());

      }
      isValid();
      if (!ignoreUpdate) {
        multiPageEditor.setFileDirty();
      }
    }
  };

  protected SelectionListener asynAggregateListener = new SelectionAdapter() {

    public void widgetSelected(SelectionEvent e) {
      if (e.getSource() == asMode) {
        if (asMode.getSelection()) {
          changeToAsyncAggregate(true);
        } else {
          changeToAsyncAggregate(false);
        }
      } else if (e.getSource() == scaleout) {
        // Update the number of instances
        // Trace.err("scaleout: " + scaleout.getSelection());
        updateScaleOut(scaleout.getSelection());

      } else if (e.getSource() == casMultiplier) {
        updateCasMultiplierPoolSize(casMultiplier.getSelection());

      } else if (e.getSource() == casMultiplierRemote) {
        updateCasMultiplierPoolSize(casMultiplierRemote.getSelection());
        
      } else if (e.getSource() == processParentCASLast) {
          updateProcessParentCASLast(processParentCASLast.getSelection());

      } else if (e.getSource() == initialFsHeapSize) {
          updateInitialFsHeapSize(initialFsHeapSize.getSelection());

      } else if (e.getSource() == initialFsHeapSizeRemote) {
          updateInitialFsHeapSize(initialFsHeapSizeRemote.getSelection());

      } else if (e.getSource() == remoteReplyQueueScaleout) {
        ((RemoteAEDeploymentMetaData) currentMetaDataObject).setRemoteReplyQueueScaleout(remoteReplyQueueScaleout.getSelection());

      } else if (e.getSource() == inputQueueScaleout) {
        ((AEDeploymentMetaData) currentMetaDataObject).setInputQueueScaleout(inputQueueScaleout.getSelection());

      } else if (e.getSource() == replyQueueListenersForCoLocated) {
        ((AEDeploymentMetaData) currentMetaDataObject).setInternalReplyQueueScaleout(replyQueueListenersForCoLocated.getSelection());
      }
      multiPageEditor.setFileDirty();
    } 
  };

  protected SelectionListener deploymentListener = new SelectionAdapter() {

    public void widgetSelected(SelectionEvent e) {
      Composite switchTo = null;
      if (e.getSource() == deploymentCoLocated) {
        if (deploymentCoLocated.getSelection()) {
          // Trace.err("deploymentCoLocated");
          switchTo = compositeCoLocatedSetting;
        }
      } else if (e.getSource() == deploymentRemote) {
        if (deploymentRemote.getSelection()) {
          // Trace.err("deploymentRemote");
          switchTo = compositeRemoteSetting;
        }
      }
      if (switchTo != null && stackLayout.topControl != switchTo) {
        if (currentMetaDataObject instanceof AEDeploymentMetaData) {
          // Switch to Remote
          AEDeploymentMetaData parent = ((AEDeploymentMetaData) currentMetaDataObject).getParent();
          if (parent == null) {
            Trace.err("parent == null");
            return;
          }
          int index = parent.getDelegates()
                  .indexOf((DeploymentMetaData_Impl) currentMetaDataObject);

          currentMetaDataObject = (MetaDataObject) AETreeBuilder
                  .createRemoteAEDeploymentMetaData((AEDeploymentMetaData) currentMetaDataObject);
          index = parent.getDelegates().replaceDelegate(index,
                  (DeploymentMetaData_Impl) currentMetaDataObject);

        } else if (currentMetaDataObject instanceof RemoteAEDeploymentMetaData) {
          AEDeploymentMetaData parent = ((RemoteAEDeploymentMetaData) currentMetaDataObject)
                  .getParent();
          if (parent == null) {
            Trace.err("parent == null");
            return;
          }
          int index = parent.getDelegates()
                  .indexOf((DeploymentMetaData_Impl) currentMetaDataObject);

          currentMetaDataObject = (MetaDataObject) AETreeBuilder
                  .createAEDeploymentMetaData((RemoteAEDeploymentMetaData) currentMetaDataObject);
          index = parent.getDelegates().replaceDelegate(index,
                  (DeploymentMetaData_Impl) currentMetaDataObject);
        }
        masterPart.refresh();

        stackLayout.topControl = switchTo;
        stackLayoutComposite.layout();

        multiPageEditor.setFileDirty();
      }
    }

  };

  /** ********************************************************************** */

  public AEMetaDataDetailsPage(DeploymentDescriptorEditor editor, IManagedForm mform,
          MasterDetails master, boolean isDelegate) {
    this.multiPageEditor = editor;
    this.mform = mform;
    this.masterPart = master;
    this.isDelegate = isDelegate;
  }

  /** ********************************************************************** */

  //
  // Note: "parent" is a "LayoutComposite" created by "ScrolledPageBook pageBook"
  // in "DetailsPart".
  // The parent of "parent" is "WrappedPageBook pageBook".
  // The grand-parent of "parent" is ScrolledPageBook which needs to be
  // "reflowed" when Section is expanded/collapsed.
  public void createContents(Composite parent) {
    // Get ScrolledPageBook
    Control c = parent.getParent();
    while (!(c instanceof ScrolledPageBook)) {
      c = c.getParent();
    }

    // Set Layout for "parent"
    TableWrapLayout layout = new TableWrapLayout();
    layout.topMargin = 0;
    layout.leftMargin = 5;
    layout.rightMargin = 2;
    layout.bottomMargin = 2;
    parent.setLayout(layout);

    FormToolkit toolkit = mform.getToolkit();

    createIdentitySection(parent, toolkit);
    errorConfigDetails = new ErrorConfigDetailsPage(multiPageEditor, mform, isDelegate);
    errorConfigDetails.createConfigurationsSection(parent, toolkit);
  }

  private Section createIdentitySection(Composite parent, FormToolkit toolkit) {
    TableWrapData td;

    sectionAEMetaDataDetails = FormSection.createTableWrapDataSection(toolkit, parent, 
            Section.TWISTIE,
            Messages.DDE_AEConfigPage_AEConfig_Section_Title, 
            "Set the properties of ...", 10, 5,
            TableWrapData.FILL_GRAB, TableWrapData.FILL_GRAB, 1, 1);
    final SectionPart spart = new SectionPart(sectionAEMetaDataDetails);
    mform.addPart(spart);
    spart.initialize(mform); // Need this code. Otherwise, exception in SectionPart !!!
    sectionAEMetaDataDetails.setExpanded(true);

    // /////////////////////////////////////////////////////////////////////

    Composite sectionClient = toolkit.createComposite(sectionAEMetaDataDetails);
    TableWrapLayout tl = new TableWrapLayout();
    tl.numColumns = 2;
    tl.leftMargin = 0;
    tl.rightMargin = 0;
    tl.topMargin = 10;
    tl.bottomMargin = 0;
    sectionClient.setLayout(tl);
    sectionAEMetaDataDetails.setClient(sectionClient);

    // Co-located or Remote
    // toolkit.createLabel(sectionClient, "", SWT.NONE);
    Composite colocatedOrRemote = toolkit.createComposite(sectionClient, SWT.NONE);
    GridLayout gl = new GridLayout(3, true);
    gl.marginWidth = 0;
    colocatedOrRemote.setLayout(gl);
    td = new TableWrapData(TableWrapData.FILL);
    td.colspan = 2;
    td.grabHorizontal = true;
    td.indent = 0;
    colocatedOrRemote.setLayoutData(td);
    Label label = toolkit.createLabel(colocatedOrRemote, "Deployment:", SWT.NONE);
    label.setForeground(toolkit.getColors().getColor(FormColors.TITLE));
    deploymentCoLocated = toolkit.createButton(colocatedOrRemote, "Co-located", SWT.RADIO);
    deploymentRemote = toolkit.createButton(colocatedOrRemote, "Remote", SWT.RADIO);
    deploymentCoLocated.setSelection(true);
    deploymentCoLocated.addSelectionListener(deploymentListener);
    deploymentRemote.addSelectionListener(deploymentListener);

    stackLayoutComposite = new Composite(sectionClient, SWT.NONE);
    stackLayout = new StackLayout();
    stackLayoutComposite.setLayout(stackLayout);
    td = new TableWrapData(TableWrapData.FILL);
    td.colspan = 2;
    td.grabHorizontal = true;
    td.indent = 0;
    stackLayoutComposite.setLayoutData(td);

    compositeCoLocatedSetting = toolkit.createComposite(stackLayoutComposite, SWT.NONE);
    gl = new GridLayout(2, false);
    gl.marginWidth = 0;
    compositeCoLocatedSetting.setLayout(gl);
    GridData gd = new GridData(GridData.FILL_BOTH);
    compositeCoLocatedSetting.setLayoutData(gd);

    compositeRemoteSetting = toolkit.createComposite(stackLayoutComposite, SWT.NONE);
    gl = new GridLayout(2, false);
    gl.marginWidth = 2;
    gl.marginHeight = 2;
    compositeRemoteSetting.setLayout(gl);
    gd = new GridData(GridData.FILL_BOTH);
    compositeRemoteSetting.setLayoutData(gd);

    // /////////////////////////////////////////////////////////////////////

    // Run in AS mode
    asMode = toolkit.createButton(compositeCoLocatedSetting, Messages.DDE_AEMetaDataDetails_RunAsASAggregate, SWT.CHECK);
    gd = new GridData();
    gd.horizontalSpan = 2;
    asMode.setLayoutData(gd);
    asMode.addSelectionListener(asynAggregateListener);

    // <scaleout numberOfInstances="1"/> <!-- optional -->
    Label labelScaleout = toolkit.createLabel(compositeCoLocatedSetting, Messages.DDE_AEMetaDataDetails_NumberOfReplicatedInstances);
    scaleout = FormSection2.createLabelAndSpinner(toolkit, compositeCoLocatedSetting,
            labelScaleout, SWT.BORDER, 1, Integer.MAX_VALUE, false);
    scaleout.setSelection(1);
    scaleout.addSelectionListener(asynAggregateListener);
    GenericHoverManager hover = new GenericHoverManager(new IGenericHoverOwner() {
      public void computeInformation(GenericHoverManager hoverManager, Point ptHoverEventLocation) {
        if (!scaleout.isEnabled()) {
          hoverManager.setDisplayedInformation(Messages.Hover_Disable_NumberOfReplicatedInstances, 
                new Rectangle(1, ptHoverEventLocation.y, 1, 1));  
        } else {
          hoverManager.setDisplayedInformation(null, null);
        }
      }     
    }, getPresenterControlCreator("commandId"));
    hover.install(labelScaleout);

    labelInputQueueScaleout = toolkit.createLabel(compositeCoLocatedSetting, Messages.DDE_InputQueueScaleout);
    inputQueueScaleout = FormSection2.createLabelAndSpinner(toolkit, compositeCoLocatedSetting,
            labelInputQueueScaleout, SWT.BORDER, 1, 
            Integer.MAX_VALUE, false);
    inputQueueScaleout.setSelection(1);
    inputQueueScaleout.addSelectionListener(asynAggregateListener);
    hover = new GenericHoverManager(new IGenericHoverOwner() {
      public void computeInformation(GenericHoverManager hoverManager, Point ptHoverEventLocation) {
        hoverManager.setDisplayedInformation(Messages.Hover_InputQueueScaleout, 
                new Rectangle(1, ptHoverEventLocation.y, 1, 1));        
      }      
    }, getPresenterControlCreator("commandId"));
    hover.install(labelInputQueueScaleout);
    
    labelReplyQueueForCoLocated = toolkit.createLabel(compositeCoLocatedSetting, Messages.DDE_ReplyQueueListenersForCoLocated);
    replyQueueListenersForCoLocated = FormSection2.createLabelAndSpinner(toolkit, compositeCoLocatedSetting,
            labelReplyQueueForCoLocated, SWT.BORDER, 1, 
            Integer.MAX_VALUE, false);
    replyQueueListenersForCoLocated.setSelection(1);
    replyQueueListenersForCoLocated.addSelectionListener(asynAggregateListener);
    hover = new GenericHoverManager(new IGenericHoverOwner() {
      public void computeInformation(GenericHoverManager hoverManager, Point ptHoverEventLocation) {
        hoverManager.setDisplayedInformation(Messages.Hover_ReplyQueueListenersForCoLocated, 
                new Rectangle(1, ptHoverEventLocation.y, 1, 1));        
      }     
    }, getPresenterControlCreator("commandId"));
    hover.install(labelReplyQueueForCoLocated);

    // <casMultiplier poolSize="5"/> <!-- optional -->
    // Only for AS primitive CAS Multiplier
    casMultiplierLabel = toolkit.createLabel(compositeCoLocatedSetting,
            Messages.DDE_AEMetaDataDetails_PoolSizeOfCM);
    casMultiplier = FormSection2.createLabelAndSpinner(toolkit, compositeCoLocatedSetting,
            casMultiplierLabel, SWT.BORDER, 0, Integer.MAX_VALUE, false);
    casMultiplier.setSelection(0);
    casMultiplier.addSelectionListener(asynAggregateListener);
    
    // initialFsHeapSize (default size is 2M)
    initialFsHeapSizeLabel = toolkit.createLabel(compositeCoLocatedSetting,
            Messages.DDE_AEMetaDataDetails_InitalSizeOfCASHeap);
    initialFsHeapSize = FormSection2.createLabelAndSpinner(toolkit, compositeCoLocatedSetting,
            initialFsHeapSizeLabel, SWT.BORDER, 1, 
            Integer.MAX_VALUE, false, FormSection2.MAX_DECORATION_WIDTH);
    gd = (GridData) initialFsHeapSize.getLayoutData();
    gd.minimumWidth = 80;
    gd.widthHint = 80;
    initialFsHeapSize.setLayoutData(gd);
    initialFsHeapSize.setSelection(0);
    initialFsHeapSize.addSelectionListener(asynAggregateListener);

    // ProcessParentCASLast (default false)
    processParentCASLast = toolkit.createButton(compositeCoLocatedSetting, Messages.DDE_AEMetaDataDetails_ProcessParentCASLast, SWT.CHECK);
    gd = new GridData();
    gd.horizontalSpan = 2;
    processParentCASLast.setLayoutData(gd);
    processParentCASLast.addSelectionListener(asynAggregateListener);

    
    // /////////////////////////////////////////////////////////////////////

    // Note: Need to add SWT.BORDER style to make the border VISIBLE in Linux
    brokerUrlDecoField = FormSection2.createLabelAndDecoratedText(toolkit, 
            compositeRemoteSetting, Messages.DDE_AEMetaDataDetails_BrokerURLForRemote, 
            currentMetaDataObject == null ?
                    "":((RemoteAEDeploymentMetaData) currentMetaDataObject).getInputQueue().getBrokerURL(), 
                    SWT.WRAP | SWT.BORDER, 100, 0, true, 0);
    
    // Create an error decoration
    decorationBrokerUrl = FormSection2.registerFieldDecoration("brokerUrl2",
                  "The broker URL cannot be empty");
    brokerUrlDecoField.addFieldDecoration(decorationBrokerUrl, SWT.LEFT | SWT.TOP, false);    
    brokerUrl = (Text) brokerUrlDecoField.getControl();
    FormData fd = (FormData) brokerUrl.getLayoutData();
    fd.top.offset += 2; // make border visible in Linux
    fd.left.offset += 2;   // make border visible in Linux
    brokerUrl.addModifyListener(fModifyListener);  

    // Note: Need to add SWT.BORDER style to make the border VISIBLE in Linux
    endPointDecoField = FormSection2.createLabelAndDecoratedText(toolkit, 
            compositeRemoteSetting, Messages.DDE_AEMetaDataDetails_QueueNameForRemote, 
            currentMetaDataObject == null ?
                    "":((RemoteAEDeploymentMetaData) currentMetaDataObject).getInputQueue().getEndPoint(), 
                    SWT.WRAP | SWT.BORDER, 100, 0, true, 0);
    endPoint = (Text) endPointDecoField.getControl();
    endPoint.addModifyListener(fModifyListener);
    decorationEndPoint =
      FieldDecorationRegistry.getDefault().getFieldDecoration(FieldDecorationRegistry.DEC_ERROR);
    decorationEndPoint.setDescription("The name of the queue cannot be empty");
    endPointDecoField.addFieldDecoration(decorationEndPoint, SWT.LEFT | SWT.TOP, false);    
   
    labelRemoteReplyQueueScaleout = toolkit.createLabel(compositeRemoteSetting, Messages.DDE_RemoteReplyQueueScaleout);
    remoteReplyQueueScaleout = FormSection2.createLabelAndSpinner(toolkit, compositeRemoteSetting,
            labelRemoteReplyQueueScaleout, SWT.BORDER, 1, 
            Integer.MAX_VALUE, false);
    remoteReplyQueueScaleout.setSelection(1);
    remoteReplyQueueScaleout.addSelectionListener(asynAggregateListener);
    hover = new GenericHoverManager(new IGenericHoverOwner() {

      public void computeInformation(GenericHoverManager hoverManager, Point ptHoverEventLocation) {
        hoverManager.setDisplayedInformation(Messages.Hover_RemoteReplyQueueScaleout, 
                new Rectangle(1, ptHoverEventLocation.y, 1, 1));        
      }
      
    }, getPresenterControlCreator("commandId"));
    hover.install(labelRemoteReplyQueueScaleout);

    // <casMultiplier poolSize="5"/> <!-- optional -->
    casMultiplierLabelRemote = toolkit.createLabel(compositeRemoteSetting,
            Messages.DDE_AEMetaDataDetails_PoolSizeOfCM);
    casMultiplierRemote = FormSection2.createLabelAndSpinner(toolkit, compositeRemoteSetting,
            casMultiplierLabelRemote, SWT.BORDER, 0, Integer.MAX_VALUE, false);
    casMultiplierRemote.setSelection(0);
    casMultiplierRemote.addSelectionListener(asynAggregateListener);

    // initialFsHeapSize (default size is 2M)
    initialFsHeapSizeLabelRemote = toolkit.createLabel(compositeRemoteSetting,
            Messages.DDE_AEMetaDataDetails_InitalSizeOfCASHeap);
    initialFsHeapSizeRemote = FormSection2.createLabelAndSpinner(toolkit, compositeRemoteSetting,
            initialFsHeapSizeLabelRemote, SWT.BORDER, 1, 
            Integer.MAX_VALUE, false, FormSection2.MAX_DECORATION_WIDTH);
    gd = (GridData) initialFsHeapSizeRemote.getLayoutData();
    gd.minimumWidth = 80;
    gd.widthHint = 80;
    initialFsHeapSizeRemote.setLayoutData(gd);
    initialFsHeapSizeRemote.setSelection(0);
    initialFsHeapSizeRemote.addSelectionListener(asynAggregateListener);
    
    stackLayout.topControl = compositeCoLocatedSetting;

    return sectionAEMetaDataDetails;
  }

  protected void displayDetails(AEDeploymentMetaData obj) {    
    sectionAEMetaDataDetails.setRedraw(false);
    sectionAEMetaDataDetails.setText(Messages.DDE_AEConfigPage_AEConfig_Section_Title
            + " - " + obj.getKey());
    sectionAEMetaDataDetails.setExpanded(false);
    sectionAEMetaDataDetails.setExpanded(true);
    sectionAEMetaDataDetails.setRedraw(true);
    
    if (obj.isTopAnalysisEngine()) {
      deploymentRemote.setEnabled(false);
      deploymentCoLocated.setEnabled(false);
    } else {
      deploymentRemote.setEnabled(true);
      deploymentCoLocated.setEnabled(true);
      if (!deploymentCoLocated.getSelection()) {
        deploymentRemote.setSelection(false);
        deploymentCoLocated.setSelection(true);
      }
    }

    if (stackLayout.topControl != compositeCoLocatedSetting) {
      stackLayout.topControl = compositeCoLocatedSetting;
      stackLayoutComposite.layout();
    }
    if (obj.isAsync()) {
      scaleout.setSelection(1);
      scaleout.setEnabled(false);
      scaleout.setToolTipText("For AS Aggregate, only 1 instance is allowed.");
      labelInputQueueScaleout.setVisible(true);
      inputQueueScaleout.setVisible(true);
      labelReplyQueueForCoLocated.setVisible(true);
      replyQueueListenersForCoLocated.setVisible(true);
    } else {
      scaleout.setEnabled(true);
      scaleout.setToolTipText(null);
      scaleout.setSelection(obj.getNumberOfInstances());
      labelInputQueueScaleout.setVisible(false);
      inputQueueScaleout.setVisible(false);
      labelReplyQueueForCoLocated.setVisible(false);
      replyQueueListenersForCoLocated.setVisible(false);
    }
    
    ResourceSpecifier rs = obj.getResourceSpecifier();
    showStatus(null, IMessageProvider.NONE);
    if (rs != null) {
      // Is AnalysisEngineDescription ?
      if (rs instanceof AnalysisEngineDescription) {
        AnalysisEngineDescription aed = (AnalysisEngineDescription) rs;

        // Is CAs Multiplier ?
        // Only for AS primitive CAS Multiplier
        if (!obj.isAsync() && AETreeBuilder.isCASMultiplier(aed)) {
          // Trace.err("CASMultiplier pool size: " + obj.getCasMultiplierPoolSize());
          casMultiplierLabel.setVisible(true);
          casMultiplier.setVisible(true);
          casMultiplier.setSelection(obj.getCasMultiplierPoolSize());
          initialFsHeapSizeLabel.setVisible(true);
          initialFsHeapSize.setVisible(true);
          initialFsHeapSize.setSelection(obj.getInitialFsHeapSize());
          processParentCASLast.setVisible(true);
          processParentCASLast.setSelection(obj.getProcessParentCASLast());
        } else {
          casMultiplierLabel.setVisible(false);
          casMultiplier.setVisible(false);
          initialFsHeapSizeLabel.setVisible(false);
          initialFsHeapSize.setVisible(false);
          processParentCASLast.setVisible(false);
        }
        // Is Primitive ?
        if (aed.isPrimitive()) {
          asMode.setEnabled(false);
          asMode.setSelection(false);
        } else {
          asMode.setEnabled(true);
          asMode.setSelection(obj.isAsync());
          if (obj.isAsync()) {
            if (obj.getInputQueueScaleout() > 0) {
              inputQueueScaleout.setSelection(obj.getInputQueueScaleout());
            }
            if (obj.getInternalReplyQueueScaleout() > 0) {
              replyQueueListenersForCoLocated.setSelection(obj.getInternalReplyQueueScaleout());
            }
          }
        }
      } else if (rs instanceof CollectionReaderDescription) {
        // Handle as CAS Multiplier
        casMultiplierLabel.setVisible(true);
        casMultiplier.setVisible(true);
        casMultiplier.setSelection(obj.getCasMultiplierPoolSize());
        initialFsHeapSizeLabel.setVisible(true);
        initialFsHeapSize.setVisible(true);
        initialFsHeapSize.setSelection(obj.getInitialFsHeapSize());
        
        asMode.setEnabled(false);
        asMode.setSelection(false);
      } else {
        // CAS Consumer, ...
        casMultiplierLabel.setVisible(false);
        casMultiplier.setVisible(false);
        initialFsHeapSizeLabel.setVisible(false);
        initialFsHeapSize.setVisible(false);
        asMode.setEnabled(false);
        asMode.setSelection(false);
      }
    } else {
      String parentKey = null;
      if (obj.getParent() != null) {
        parentKey = obj.getParent().getKey();          
      } else {
        if (obj.isTopAnalysisEngine()) {
          showStatus("The top descriptor is not specified.", IMessageProvider.ERROR);
          return;
        }
      }
      if (parentKey == null) {
        parentKey = "Top Analysis Engine";
      }
      Trace.err("The analysis engine's key=\"" + obj.getKey()
              + "\" is not valid for the aggregate \"" + parentKey + "\"");
      showStatus("The analysis engine's key=\"" + obj.getKey()
              + "\" is not valid for the aggregate \"" + parentKey + "\"", IMessageProvider.ERROR);
    }
    // casMultiplier.setVisible(show);

    // Error Config
    AsyncAEErrorConfiguration errorConfig = obj.getAsyncAEErrorConfiguration();
    if (errorConfig != null) {
      errorConfigDetails.displayDetails((DeploymentMetaData_Impl)obj, errorConfig);
    } else {
      Trace.bug("errorConfig == null");
    }
    
  }

  protected void displayDetails(RemoteAEDeploymentMetaData obj) {
    ignoreUpdate = true;
    sectionAEMetaDataDetails.setRedraw(false);
    sectionAEMetaDataDetails.setText(Messages.DDE_AEConfigPage_AEConfig_Section_Title
            + " - " + obj.getKey());
    sectionAEMetaDataDetails.setExpanded(false);
    sectionAEMetaDataDetails.setExpanded(true);
    sectionAEMetaDataDetails.setRedraw(true);
    
    deploymentRemote.setEnabled(true);
    deploymentCoLocated.setEnabled(true);
    if (!deploymentRemote.getSelection()) {
      deploymentCoLocated.setSelection(false);
      deploymentRemote.setSelection(true);
    }

    if (stackLayout.topControl != compositeRemoteSetting) {
      stackLayout.topControl = compositeRemoteSetting;
      stackLayoutComposite.layout();
    }
    if (obj.getInputQueue() != null) {
      brokerUrl.setText(obj.getInputQueue().getBrokerURL());
      endPoint.setText(obj.getInputQueue().getEndPoint());
    }

    int n = obj.getRemoteReplyQueueScaleout() > 0 ? obj.getRemoteReplyQueueScaleout() : 1;
    remoteReplyQueueScaleout.setSelection(n);

    if (obj.getResourceSpecifier() != null) {
      // Is AnalysisEngineDescription ?
      if (obj.getResourceSpecifier() instanceof AnalysisEngineDescription) {
        showStatus(null, IMessageProvider.ERROR); // clear error msg
        AnalysisEngineDescription aed = (AnalysisEngineDescription) obj.getResourceSpecifier();
        // Is CAs Multiplier ?
        if (AETreeBuilder.isCASMultiplier(aed)) {
          // Trace.err("CASMultiplier");
          casMultiplierLabelRemote.setVisible(true);
          casMultiplierRemote.setVisible(true);
          casMultiplierRemote.setSelection(obj.getCasMultiplierPoolSize());
          initialFsHeapSizeLabelRemote.setVisible(true);
          initialFsHeapSizeRemote.setVisible(true);
          initialFsHeapSizeRemote.setSelection(obj.getInitialFsHeapSize());
        } else {
          casMultiplierLabelRemote.setVisible(false);
          casMultiplierRemote.setVisible(false);
          initialFsHeapSizeLabelRemote.setVisible(false);
          initialFsHeapSizeRemote.setVisible(false);
        }
      }
    }

    // Error Config
    AsyncAEErrorConfiguration errorConfig = obj.getAsyncAEErrorConfiguration();
    if (errorConfig != null) {
      errorConfigDetails.displayDetails((DeploymentMetaData_Impl) obj, errorConfig);
    }
    ignoreUpdate = false;
  }

  protected void isValid() {
    if (brokerUrl.getText().trim().length() == 0) {
      brokerUrlDecoField.showDecoration(decorationBrokerUrl);
      // return;
    } else {
      brokerUrlDecoField.hideDecoration(decorationBrokerUrl);      
    }

    if (endPoint.getText().trim().length() == 0) {
      endPointDecoField.showDecoration(decorationEndPoint);
      // return;
    } else {
      endPointDecoField.hideDecoration(decorationEndPoint);
    }
  }

  protected void showStatus(String msg, int msgType) {
    FormMessage.setMessage(mform.getForm().getForm(), msg, msgType);    
  }

  private IInformationControlCreator getPresenterControlCreator(final String commandId) {
    return new IInformationControlCreator() {
      public IInformationControl createInformationControl(Shell parent) {
        return new DefaultInformationControl(parent, SWT.WRAP, null);
      }
    };
  }


  /** ********************************************************************** */

  public void commit(boolean onSave) {

    super.commit(onSave);
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

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.forms.IDetailsPage#refresh()
   */
  public void refresh() {
    super.refresh();
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
    if (selectedObject instanceof AEDeploymentMetaData) {
      currentMetaDataObject = (MetaDataObject) selectedObject;
      displayDetails((AEDeploymentMetaData) selectedObject);
    } else if (selectedObject instanceof RemoteAEDeploymentMetaData) {
      currentMetaDataObject = (MetaDataObject) selectedObject;
      displayDetails((RemoteAEDeploymentMetaData) selectedObject);
    } else {
      selectedObject = null;
    }
  }

  /**
   * Update the number of instances
   * 
   * @param value
   * @return void
   */
  private void updateScaleOut(int value) {
    if (currentMetaDataObject instanceof AEDeploymentMetaData) {
      ((AEDeploymentMetaData) currentMetaDataObject).setNumberOfInstances(value);

      // Set CAS pool size to the number of instances
      multiPageEditor.getOverviewPage().setCasPoolSize(value);

    }
    masterPart.refresh();
    multiPageEditor.setFileDirty();
  }
  
  private void updateCasMultiplierPoolSize(int value) {
    if (currentMetaDataObject instanceof AEDeploymentMetaData) {
      ((AEDeploymentMetaData) currentMetaDataObject).setCasMultiplierPoolSize(value);

    } else if (currentMetaDataObject instanceof RemoteAEDeploymentMetaData) {
      ((RemoteAEDeploymentMetaData) currentMetaDataObject).setCasMultiplierPoolSize(value);

    }
    multiPageEditor.setFileDirty();
  }

  private void updateProcessParentCASLast(boolean value) {
    if (currentMetaDataObject instanceof AEDeploymentMetaData) {
      ((AEDeploymentMetaData) currentMetaDataObject).setProcessParentCASLast(value);

//    } else if (currentMetaDataObject instanceof RemoteAEDeploymentMetaData) {
//      ((RemoteAEDeploymentMetaData) currentMetaDataObject).setProcessParentCASLast(value);
//
    }
    multiPageEditor.setFileDirty();
  }

  private void updateInitialFsHeapSize(int value) {
	    if (currentMetaDataObject instanceof AEDeploymentMetaData) {
	      ((AEDeploymentMetaData) currentMetaDataObject).setInitialFsHeapSize(value);

	    } else if (currentMetaDataObject instanceof RemoteAEDeploymentMetaData) {
	      ((RemoteAEDeploymentMetaData) currentMetaDataObject).setInitialFsHeapSize(value);

	    }
	    multiPageEditor.setFileDirty();
	  }

  protected void changeToAsyncAggregate(boolean toAsyncAggreagte) {
    AEDeploymentMetaData metaData = (AEDeploymentMetaData) currentMetaDataObject;
    if (toAsyncAggreagte) {
      // Resolve delegates
      try {
        metaData.resolveDelegates(multiPageEditor.cde.createResourceManager(), false);
      } catch (InvalidXMLException e) {
        e.printStackTrace();
        Utility.popMessage(Messages.getString("InvalidXMLException"), //$NON-NLS-1$
                multiPageEditor.cde.getMessagesToRootCause(e), MessageDialog.ERROR);
        multiPageEditor.switchToBadSource(false);
        return;
      }
      if (metaData.getNumberOfInstances() != 1) {
        metaData.setNumberOfInstances(1);
      }
    }
    metaData.setAsync(toAsyncAggreagte);
    
    // Enable/Disable CAS Pool Size Control
    multiPageEditor.getOverviewPage().enableCasPoolSizeSettings(toAsyncAggreagte);
    if (!toAsyncAggreagte) {
      // Set CAS pool size to the number of instances
      multiPageEditor.getOverviewPage().setCasPoolSize(metaData.getNumberOfInstances());

      // Set number of listeners to default value (=1)
      inputQueueScaleout.setSelection(1);
      replyQueueListenersForCoLocated.setSelection(1);
    }

    masterPart.refresh();
    multiPageEditor.setFileDirty();
  }

}
