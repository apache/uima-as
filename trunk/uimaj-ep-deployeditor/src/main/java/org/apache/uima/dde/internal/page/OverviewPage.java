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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.AEService;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.impl.AEDelegates_Impl;
import org.apache.uima.aae.deployment.impl.AEDeploymentMetaData_Impl;
import org.apache.uima.aae.deployment.impl.NameValue;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.dde.internal.DeploymentDescriptorEditor;
import org.apache.uima.dde.internal.Messages;
import org.apache.uima.dde.internal.wizards.EditParamWizard;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.impl.Import_impl;
import org.apache.uima.taeconfigurator.editors.ui.FileAndShortName;
import org.apache.uima.taeconfigurator.editors.ui.Utility;
import org.apache.uima.taeconfigurator.files.MultiResourceSelectionDialog;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.tools.internal.cde.uima.util.UimaDescriptionUtils;
import org.apache.uima.tools.internal.ui.forms.FormSection;
import org.apache.uima.tools.internal.ui.forms.FormSection2;
import org.apache.uima.util.InvalidXMLException;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.Path;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.dialogs.IMessageProvider;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.fieldassist.DecoratedField;
import org.eclipse.jface.fieldassist.FieldDecoration;
import org.eclipse.jface.fieldassist.FieldDecorationRegistry;
import org.eclipse.jface.viewers.ColumnLayoutData;
import org.eclipse.jface.viewers.ColumnWeightData;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.IStructuredContentProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITableLabelProvider;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TableLayout;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerSorter;
import org.eclipse.jface.window.Window;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TraverseEvent;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.forms.FormColors;
import org.eclipse.ui.forms.IManagedForm;
import org.eclipse.ui.forms.events.ExpansionAdapter;
import org.eclipse.ui.forms.events.ExpansionEvent;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.ScrolledForm;
import org.eclipse.ui.forms.widgets.Section;
import org.eclipse.ui.forms.widgets.TableWrapData;
import org.eclipse.ui.forms.widgets.TableWrapLayout;


/**
 * 
 * 
 */
public class OverviewPage extends AbstractHeaderPage {

  static private final int  SERVICE_PANE_INDENT = 8; // indent for elements of service section_

  static private final Object[] EMPTY_ARRAY = new Object[0];

  private IManagedForm mForm;
  
  private ScrolledForm form;
  
  private DecoratedField brokerUrlDecoField;
  
  private DecoratedField endPointDecoField;

  private DecoratedField topDescriptorDecoField;

  private FieldDecoration decorationBrokerUrl;

  private FieldDecoration decorationEndPoint;

  private FieldDecoration decorationTopDescriptor;
  
  private Text brokerUrl;

  private Text endPoint;
  
  private Spinner initialFsHeapSize;

  private Spinner casPoolSize;
  
  private Spinner prefetch;

  private Text topDescriptorField;

  private Label importByNameOrLocation;
  
  private boolean isImportByLocation = true; // default

  private DeploymentDescriptorEditor multiPageEditor;

  private AEDeploymentDescription aeDeploymentDescription;

  private AEService aeService;
  
  private Button customButton; // Customization for C++
  
  private Composite customComposite;
  
  private Composite serviceSectionClient;

  private Text nameText;
  private Text description;
  private Text version;
  private Text vendor;
  private boolean ignoreUpdate = false; // Used to update Text without setting dirty flag
  private Action openAction;    // Open top-level Xml with CDE
  private Action notInWSAction; // Pop-up "File is not in workspace" when try to open 
  
  private TableViewer envTableViewer;
  private Table envTable;
  private Map<String,NameValue> envName2NameValueMap = new TreeMap<String,NameValue>();
  private Button addButton;
  private Button editButton;
  private Button removeButton;

  private ColumnLayoutData[] envTableColumnLayouts = {
    new ColumnWeightData(200),
    new ColumnWeightData(300)
  };

  /** ********************************************************************** */
  
  // Used by AEMetaDataDetailsPage
  public void enableCasPoolSizeSettings (boolean enable) {
    casPoolSize.setEnabled(enable);
  }
 
  // Used by AEMetaDataDetailsPage
  public void setCasPoolSize (int number) {
    try {
      if (!aeDeploymentDescription.getAeService().getAnalysisEngineDeploymentMetaData().isAsync()) {
        casPoolSize.setSelection(number); // Update control
        aeDeploymentDescription.setCasPoolSize(number); // Update descriptor
      }
    } catch (InvalidXMLException e) {
      e.printStackTrace();
    }
  }
  
  /** ********************************************************************** */
  
  /**
   * Content provider for the environment var table
   */
  protected class EnvVariableContentProvider implements IStructuredContentProvider {
    
    public Object[] getElements(Object inputElement) {
      Map<String,NameValue> m = (Map<String,NameValue>) inputElement;
      if (!m.isEmpty()) {
        return m.values().toArray();
      }
      return EMPTY_ARRAY;
    }
    public void dispose() {
    }
    public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
      if (newInput == null || !(newInput instanceof Map)){
        return;
      }
      if (viewer instanceof TableViewer){
        TableViewer tableViewer= (TableViewer) viewer;
        if (tableViewer.getTable().isDisposed()) {
          return;
        }
        tableViewer.setSorter(new ViewerSorter() {
          public int compare(Viewer iviewer, Object e1, Object e2) {
            if (e1 == null) {
              return -1;
            } else if (e2 == null) {
              return 1;
            } else {
              return ((NameValue)e1).getName().compareToIgnoreCase(((NameValue)e2).getName());
            }
          }
        });
      }
    }
  }
  
  /**
   * Label provider for the environment var table
   */
  public class EnvVariableLabelProvider extends LabelProvider implements ITableLabelProvider {
    public String getColumnText(Object element, int columnIndex)  {
      String result = null;
      if (element != null) {
        NameValue var = (NameValue) element;
        switch (columnIndex) {
          case 0: // variable
            result = var.getName();
            break;
          case 1: // value
            result = var.getValue();
            break;
        }
      }
      return result;
    }
    
    public Image getColumnImage(Object element, int columnIndex) {
      return null;
    }
  }
  
  /** ********************************************************************** */

  private ModifyListener fModifyListener = new ModifyListener() {

    public void modifyText(ModifyEvent e) {
      Object source = e.getSource();

      if (source == brokerUrl) {
        aeService.setBrokerURL(brokerUrl.getText().trim());

      } else if (source == endPoint) {
        aeService.setEndPoint(endPoint.getText().trim());

      } else if (source == topDescriptorField) {

      } else if (source == nameText) {
        aeDeploymentDescription.setName(nameText.getText().trim());
        
      } else if (source == description) {
        aeDeploymentDescription.setDescription(description.getText().trim());
        
      } else if (source == version) {
        aeDeploymentDescription.setVersion(version.getText().trim());
        
      } else if (source == vendor) {
        aeDeploymentDescription.setVendor(vendor.getText().trim());       
      }
      isValid();

      if (!ignoreUpdate) {
        multiPageEditor.setFileDirty();
      }
    }
  };
  
  
    
  protected SelectionListener selectionListener = new SelectionAdapter() {

    public void widgetSelected(SelectionEvent event) {
      if (event.getSource() == casPoolSize) {
        aeDeploymentDescription.setCasPoolSize(casPoolSize.getSelection());

      } else if (event.getSource() == initialFsHeapSize) {
        aeDeploymentDescription.setInitialFsHeapSize(initialFsHeapSize.getSelection());

      } else if (event.getSource() == prefetch) {
        aeService.setPrefetch(prefetch.getSelection());

      } else if (event.getSource() == customButton) {
        // Customization of C++
        if (customComposite == null) {
          aeService.setCustomValue(AEDeploymentConstants.TAG_CUSTOM_ATTR_NAME);
          customComposite = createCustomizationSection(serviceSectionClient);
          updateEnvironmentVariables();
        } else {
          aeService.setCustomValue(null);
          if (aeService.getEnvironmentVariables() != null) {
            aeService.getEnvironmentVariables().clear();
          }
          customComposite.dispose();
          customComposite = null;
        }
        serviceSectionClient.layout(true, true);
        mForm.reflow(true);
      }
      multiPageEditor.setFileDirty();
    }
  };
  

  /** ********************************************************************** */

  /**
   * @param editor
   * @param id
   * @param title
   */
  public OverviewPage(DeploymentDescriptorEditor editor, String id, String title) {
    super(editor.cde, id, title);
    multiPageEditor = editor;
    aeDeploymentDescription = multiPageEditor.getAEDeploymentDescription();
    aeService = aeDeploymentDescription.getAeService();
  }

  /**
   * Called by the framework to fill in the contents
   */
  protected void createFormContent(IManagedForm managedForm) {
    super.createFormContent(managedForm);
    mForm = managedForm;
    form = managedForm.getForm();
    FormToolkit toolkit = managedForm.getToolkit();
    form.setText(Messages.DDE_OverviewPage_Title);

//    ScrolledForm form = managedForm.getForm();
//    FormColors colors = toolkit.getColors();
//    colors.initializeSectionToolBarColors();
//    Color gbg = colors.getColor(FormColors.TB_GBG);
//    Color bg = colors.getBackground();
//    form.getForm().setTextBackground(new Color[] { bg, gbg }, new int[] { 100 }, true);

    fillBody(managedForm, toolkit);
  }

  private void fillBody(IManagedForm managedForm, FormToolkit toolkit) {
    Composite body = managedForm.getForm().getBody();
    body.setLayout(new GridLayout(1, false)); // this is required !
    
    Composite compTop = toolkit.createComposite(body);
    compTop.setLayout(new GridLayout(1, false));
    compTop.setLayoutData(new GridData(GridData.FILL_BOTH));
    Control c = compTop.getParent();
    while (!(c instanceof ScrolledComposite)) {
      c = c.getParent();
    }
    ((GridData) compTop.getLayoutData()).widthHint = c.getSize().x;
    ((GridData) compTop.getLayoutData()).heightHint = c.getSize().y;

    compTop.setLayoutData(new GridData(GridData.FILL_BOTH)); // needed

    ScrolledComposite leftScrolled = new ScrolledComposite(compTop, SWT.H_SCROLL | SWT.V_SCROLL); // toolkit.createComposite(sashFormOverview);
    leftScrolled.setExpandHorizontal(true);
    leftScrolled.setExpandVertical(true);
    leftScrolled.setLayoutData(new GridData(GridData.FILL_BOTH));
    leftScrolled.setLayout(new GridLayout(1, false));
    Composite servicePane = toolkit.createComposite(leftScrolled);
    TableWrapLayout layout = new TableWrapLayout();
    servicePane.setLayout(layout);
    servicePane.setLayoutData(new GridData(GridData.FILL_BOTH));
    leftScrolled.setContent(servicePane);

    ScrolledComposite rightScrolled = new ScrolledComposite(compTop, SWT.H_SCROLL | SWT.V_SCROLL); // toolkit.createComposite(sashFormOverview);
    rightScrolled.setExpandHorizontal(true);
    rightScrolled.setExpandVertical(true);
    rightScrolled.setLayoutData(new GridData(GridData.FILL_BOTH));
    rightScrolled.setLayout(new GridLayout(1, false));
    Composite rightPane = toolkit.createComposite(rightScrolled);
    layout = new TableWrapLayout();
    rightPane.setLayout(layout);
    rightPane.setLayoutData(new GridData(GridData.FILL_BOTH));
    rightScrolled.setContent(rightPane);
    
    createIdentitySection(rightPane, toolkit);
    createServiceSection(servicePane, toolkit);
    
    leftScrolled.setMinSize(servicePane.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    rightScrolled.setMinSize(rightPane.computeSize(SWT.DEFAULT, SWT.DEFAULT));
  }
  
  /** ********************************************************************** */

  private Section createIdentitySection(Composite parent, FormToolkit toolkit) {
    Section section = FormSection.createTableWrapDataSection(toolkit, parent, Section.TREE_NODE,
            Messages.DDE_OverviewPage_General_Section_Title,
            Messages.DDE_OverviewPage_General_Section_Description, 10, 3, TableWrapData.FILL_GRAB,
            TableWrapData.FILL_GRAB, 1, 1);

    section.setExpanded(true);
    section.addExpansionListener(new ExpansionAdapter() {
      public void expansionStateChanged(ExpansionEvent e) {
        form.reflow(true);
      }
    });

    // //////////////////

    Composite sectionClient = toolkit.createComposite(section);
    section.setClient(sectionClient);
    TableWrapLayout tl = new TableWrapLayout();
    tl.numColumns = 2;
    tl.bottomMargin = 6;
    sectionClient.setLayout(tl);

    // Name
    nameText = FormSection.createLabelAndText(toolkit, sectionClient, "Name:",
            aeDeploymentDescription.getName(), SWT.WRAP, 10, 0);

    // Description
    description = FormSection.createLabelAndText(toolkit, sectionClient, "Description:",
            aeDeploymentDescription.getDescription(), SWT.V_SCROLL | SWT.MULTI | SWT.WRAP, 10, 60);

    version = FormSection.createLabelAndText(toolkit, sectionClient, "Version:",
            aeDeploymentDescription.getVersion(), SWT.WRAP, 10, 0);

    vendor = FormSection.createLabelAndText(toolkit, sectionClient, "Vendor:",
            aeDeploymentDescription.getVendor(), SWT.WRAP, 10, 0);
    
    nameText.addModifyListener(fModifyListener);
    description.addModifyListener(fModifyListener);
    version.addModifyListener(fModifyListener);
    vendor.addModifyListener(fModifyListener);

    // ////////////////////////////////////////

    toolkit.paintBordersFor(sectionClient);

    return section;
  } // createIdentitySection

  private Composite createCustomizationSection(Composite parent) {
    FormToolkit toolkit = mForm.getToolkit();
    Composite customComposite = toolkit.createComposite(parent);
    TableWrapLayout tl = new TableWrapLayout();
    tl.numColumns = 1;
    tl.numColumns = 1;
    tl.leftMargin = 0;
    tl.rightMargin = 0;
    tl.topMargin = 0;
    tl.bottomMargin = 0;
    customComposite.setLayout(tl);
    GridData gd = new GridData(GridData.FILL_HORIZONTAL);
    gd.horizontalIndent = SERVICE_PANE_INDENT;
    gd.grabExcessHorizontalSpace = true;
    customComposite.setLayoutData(gd);
    
    envTable = createCustomizationTable(customComposite, toolkit);

    envTableViewer = new TableViewer(envTable);
    envTableViewer.setContentProvider(new EnvVariableContentProvider());
    envTableViewer.setLabelProvider(new EnvVariableLabelProvider());
    envTableViewer.addSelectionChangedListener(new ISelectionChangedListener() {
      public void selectionChanged(SelectionChangedEvent event) {
        // handleTableSelectionChanged(event);
        int size = ((IStructuredSelection)event.getSelection()).size();
        editButton.setEnabled(size == 1);
        removeButton.setEnabled(size > 0);
      }
    });
    envTableViewer.addDoubleClickListener(new IDoubleClickListener() {
      public void doubleClick(DoubleClickEvent event) {
        if (!envTableViewer.getSelection().isEmpty()) {
          handleEditButton();
        }
      }
    });
    envTableViewer.setInput(envName2NameValueMap);
        
    toolkit.paintBordersFor(customComposite);

    return customComposite;
  } // createCustomizationSection

  private Table createCustomizationTable(Composite parent, FormToolkit toolkit) {
    Composite tableComposite = toolkit.createComposite(parent, SWT.NONE);
    TableWrapData td = new TableWrapData(TableWrapData.FILL_GRAB, TableWrapData.FILL_GRAB);
    td.grabHorizontal = true;
    td.valign = TableWrapData.FILL;
    tableComposite.setLayoutData(td);
    
    GridLayout glayout = new GridLayout();
    glayout.marginTop = 0;
    glayout.marginLeft = 0;
    glayout.numColumns = 3;
    tableComposite.setLayout(glayout);
    
    // Add SWT.BORDER style to make table's border visible in Linux
    final Table table = toolkit.createTable(tableComposite, SWT.BORDER | SWT.HIDE_SELECTION | SWT.FULL_SELECTION
            | SWT.H_SCROLL | SWT.V_SCROLL);
    table.setLinesVisible(true);
    table.setHeaderVisible(true);
    String headers[] = { Messages.DDE_EnvVariable_Table_NAME, Messages.DDE_EnvVariable_Table_VALUE };
    
    TableLayout tableLayout = new TableLayout();
    table.setLayout(tableLayout);

    for (int i = 0; i < headers.length; i++) {
      tableLayout.addColumnData(envTableColumnLayouts[i]);
      TableColumn tc = new TableColumn(table, SWT.NONE, i);
      tc.setResizable(true);
      tc.setText(headers[i]);
    }

    GridData gd = new GridData(GridData.FILL_BOTH|GridData.VERTICAL_ALIGN_BEGINNING);
    gd.horizontalSpan = 2;
    gd.heightHint = table.getHeaderHeight() + (table.getItemHeight() * 4); // +1
    table.setLayoutData(gd);
    
    createTableButtons(tableComposite, toolkit);

    return table;
  }
  
  protected void createTableButtons(Composite parent, FormToolkit toolkit) {
    // Create button composite
    Composite buttonComposite = toolkit.createComposite(parent, SWT.NONE);
    GridLayout glayout = new GridLayout();
    glayout.marginHeight = 0;
    glayout.marginWidth = 0;
    glayout.numColumns = 1;
    GridData gdata = new GridData(GridData.VERTICAL_ALIGN_BEGINNING | GridData.HORIZONTAL_ALIGN_END);
    buttonComposite.setLayout(glayout);
    buttonComposite.setLayoutData(gdata);
    buttonComposite.setFont(parent.getFont());

    createVerticalSpacer(buttonComposite, 1);
    /**************************************/
    /*            Create buttons          */
    /**************************************/
    addButton = createPushButton(buttonComposite, "Add...", null, toolkit); 
    addButton.addSelectionListener(new SelectionAdapter()
    {
        public void widgetSelected(SelectionEvent event) {
          handleAddButton();
        }            
    });

    editButton = createPushButton(buttonComposite, "Edit...", null, toolkit); 
    editButton.addSelectionListener(new SelectionAdapter()
    {
        public void widgetSelected(SelectionEvent event) {
          handleEditButton();
        }
    });
    
    removeButton = createPushButton(buttonComposite, "Remove", null, toolkit); 
    removeButton.addSelectionListener(new SelectionAdapter()
    {
        public void widgetSelected(SelectionEvent event) {
          handleDeleteButton();
        }
    });
    
  } // createTableButtons

  public Button createPushButton(Composite parent, String label, Image image, FormToolkit toolkit) {
    Button button = toolkit.createButton(parent, label, SWT.PUSH);
    GridData gd = new GridData(GridData.CENTER|GridData.FILL_HORIZONTAL);
    button.setLayoutData(gd); 
    button.setEnabled(true);

    return button;
  }
  
  /**
   * Create some empty space.
   */
  protected void createVerticalSpacer(Composite comp, int colSpan) {
    Label label = new Label(comp, SWT.NONE);
    GridData gd = new GridData();
    gd.horizontalSpan = colSpan;
    label.setLayoutData(gd);
    label.setFont(comp.getFont());
  } 
  
  private Section createServiceSection(Composite parent, final FormToolkit toolkit) {

    Section section = FormSection.createTableWrapDataSection(toolkit, parent, Section.TREE_NODE
            | Section.DESCRIPTION, Messages.DDE_OverviewPage_Service_Section_Title,
            Messages.DDE_OverviewPage_Service_Section_Description, 10, 3, TableWrapData.FILL_GRAB,
            TableWrapData.FILL_GRAB, 1, 1);
    section.setExpanded(true);
    section.addExpansionListener(new ExpansionAdapter() {
      public void expansionStateChanged(ExpansionEvent e) {
        form.reflow(true);
      }
    });
    Composite sectionClient = toolkit.createComposite(section);
    serviceSectionClient = sectionClient;
    GridLayout gl = new GridLayout(1, false);
    gl.marginWidth = 0;
    sectionClient.setLayout(gl);
    section.setClient(sectionClient);
    
    // Top part
    Composite topComposite = toolkit.createComposite(sectionClient);
    TableWrapLayout tl = new TableWrapLayout();
    tl.numColumns = 2;
    tl.bottomMargin = 10;
    topComposite.setLayout(tl);
    GridData gd = new GridData(GridData.FILL_HORIZONTAL);
    gd.grabExcessHorizontalSpace = true;
    topComposite.setLayoutData(gd); 

    // <deployment protocol="jms" provider="activemq">
    // Note: Need to add SWT.BORDER style to make the border VISIBLE in Linux
    brokerUrlDecoField = FormSection2.createLabelAndDecoratedText(toolkit, topComposite, 
            "Broker URL for input queue:", 
            aeService.getBrokerURL(), SWT.WRAP | SWT.BORDER, 200, 0, true, 0);
      
    // Create an error decoration
    decorationBrokerUrl = FormSection2.registerFieldDecoration("brokerUrl",
                  "The broker URL cannot be empty");
    brokerUrlDecoField.addFieldDecoration(decorationBrokerUrl, SWT.LEFT | SWT.TOP, false);    
    
    brokerUrl = (Text) brokerUrlDecoField.getControl();
    brokerUrl.setToolTipText("Enter the URL for the input queue");
    brokerUrl.addModifyListener(fModifyListener);    

    // Note: Need to add SWT.BORDER style to make the border VISIBLE in Linux
    endPointDecoField = FormSection2.createLabelAndDecoratedText(toolkit, topComposite, 
            "Name for input queue:", aeService.getEndPoint(), SWT.WRAP | SWT.BORDER, 200, 0, true, 0);
    endPoint = (Text) endPointDecoField.getControl();
    // ((FormData) endPoint.getLayoutData()).width = 200;
    endPoint.setToolTipText("Enter the name for the input queue");
    endPoint.addModifyListener(fModifyListener);
    decorationEndPoint =
      FieldDecorationRegistry.getDefault().getFieldDecoration(FieldDecorationRegistry.DEC_ERROR);
    decorationEndPoint.setDescription("The name for the queue cannot be empty");
    endPointDecoField.addFieldDecoration(decorationEndPoint, SWT.LEFT | SWT.TOP, false);    
    
    // <..  prefetch=="0" /> <!-- optional -->
    prefetch = FormSection2.createLabelAndSpinner(toolkit, topComposite,
            "Number of requests that might be prefetched:", SWT.BORDER, 0, 
            Integer.MAX_VALUE, false, FormSection2.MAX_DECORATION_WIDTH);
    prefetch.setSelection(aeService.getPrefetch());
    prefetch.addSelectionListener(selectionListener);

    // <casPool numberOfCASes="3"/> <!-- optional -->
    casPoolSize = FormSection2.createLabelAndSpinner(toolkit, topComposite,
            "Number of CASes in CAS pool:", SWT.BORDER, 1, 
            Integer.MAX_VALUE, false, FormSection2.MAX_DECORATION_WIDTH);
    casPoolSize.setSelection(aeDeploymentDescription.getCasPoolSize());
    casPoolSize.addSelectionListener(selectionListener);
    try {
      if (!aeDeploymentDescription.getAeService().getAnalysisEngineDeploymentMetaData().isAsync()) {
        // Top AE is not Async (it is a UIMA-AS Primitive)
        casPoolSize.setEnabled(false);
        // Display warning if CAS pool size is not equal to the number of instances
        int instances = aeDeploymentDescription.getAeService().getAnalysisEngineDeploymentMetaData().getNumberOfInstances();
        if (casPoolSize.getSelection() != instances) {
          if (Window.OK == Utility.popOkCancel("Warning - CAS Pool Size", MessageFormat.format(
                "The CAS pool size (={0}) is not equal to the number of instances (={1}).\n\n"
                  + "Set the CAS pool size to " + instances + "?",
                new Object[] { casPoolSize.getSelection(), instances }), 
                MessageDialog.WARNING)) {
            setCasPoolSize(instances);
            multiPageEditor.setFileDirty();
          }
        }
      }
    } catch (InvalidXMLException e) {
      e.printStackTrace();
    }

    // initialFsHeapSize (default size is 2M)
    initialFsHeapSize = FormSection2.createLabelAndSpinner(toolkit, topComposite,
            "Initial size of CAS heap (in bytes):", SWT.BORDER, 1, 
            Integer.MAX_VALUE, false, FormSection2.MAX_DECORATION_WIDTH);
    initialFsHeapSize.setSelection(aeDeploymentDescription.getInitialFsHeapSize());
    initialFsHeapSize.addSelectionListener(selectionListener);
    
    // ////////////////////////////////////////

    // Top descriptor
    topDescriptorDecoField = FormSection2.createLabelAndGridLayoutDecoratedContainer(toolkit, 
            topComposite, "Top analysis engine descriptor", 2, 0, 0);
    Composite browsingGroup = (Composite) topDescriptorDecoField.getControl();
    decorationTopDescriptor = FormSection2.registerFieldDecoration("topDescriptorField",
        "Top descriptor cannot be empty");
    topDescriptorDecoField.addFieldDecoration(decorationTopDescriptor, SWT.LEFT | SWT.TOP, false);    

    toolkit.paintBordersFor(browsingGroup);

    topDescriptorField = toolkit.createText(browsingGroup, "", SWT.READ_ONLY);
    topDescriptorField.setLayoutData(new GridData(GridData.FILL_HORIZONTAL
            | GridData.GRAB_HORIZONTAL));
    String topDescriptor = null;
    Import imp = aeService.getImportDescriptor();
    if (imp != null) {
      String descr = imp.getLocation();
      if (descr == null) {
        isImportByLocation = false;
        descr = imp.getName();
      } else {
        isImportByLocation = true;
      }
      topDescriptor = descr;      
    }
    topDescriptorField.setText(topDescriptor != null ? topDescriptor : "");
    topDescriptorField.addModifyListener(fModifyListener);

    // Button for browsing AE Xml descriptor
    Button browseDirectoriesButton = toolkit.createButton(browsingGroup, "B&rowse...", SWT.PUSH);
    browseDirectoriesButton.setLayoutData(new GridData());
    browseDirectoriesButton.addSelectionListener(new SelectionAdapter() {

      public void widgetSelected(SelectionEvent e) {
        MultiResourceSelectionDialog dialog = new MultiResourceSelectionDialog(
                form.getShell(), multiPageEditor.getFile().getProject().getParent(),
                "Component Engine Selection", multiPageEditor.getFile().getLocation(), multiPageEditor.cde);
        dialog.setTitle("Component Engine Selection");
        dialog.setMessage("Select one component engine from the workspace:");
        dialog.open();
        Object[] files = dialog.getResult();

        if (files != null && files.length > 0) {
          for (int i = 0; i < files.length; i++) {
            FileAndShortName fsn = new FileAndShortName(files[i]);
            updateTopDescriptor(fsn.fileName, !dialog.isImportByName);
          }
        }
      }
    });

    topDescriptorField.addTraverseListener(new TraverseListener() {

      public void keyTraversed(TraverseEvent e) {
        if (e.detail == SWT.TRAVERSE_RETURN) {
          e.doit = false;
          // updateFileList(inputDocPathField.getText().trim());
        }
      }
    });

    topDescriptorField.addFocusListener(new FocusAdapter() {
      public void focusLost(org.eclipse.swt.events.FocusEvent e) {
        // updateFileList(inputDocPathField.getText().trim());
      }
    });
    
    createContextMenu();

    // Import by Name or by Location
    toolkit.createLabel(topComposite, "", SWT.NONE);
    Composite nameOrLocation = toolkit.createComposite(topComposite, SWT.NONE);
    nameOrLocation.setLayout(new GridLayout(2, false));
    TableWrapData td = new TableWrapData();
    td.colspan = 1;
    td.grabHorizontal = true;
    td.indent = 0;
    nameOrLocation.setLayoutData(td);
    toolkit.paintBordersFor(nameOrLocation);    
    importByNameOrLocation = FormSection.createLabelAndLabel(toolkit, nameOrLocation, "Import by:",
            "(Name or Location)", 200, 20);
    if (imp != null) {
      if (isImportByLocation) {
        importByNameOrLocation.setText("Location");
      } else {
        importByNameOrLocation.setText("Name");
      }
    }
    
    // Run C++ component in a separate process
    customButton = toolkit.createButton(sectionClient, "Run top level C++ component in a separate process", SWT.CHECK);
    gd = new GridData(GridData.FILL_HORIZONTAL);
    gd.horizontalIndent = SERVICE_PANE_INDENT;
    gd.grabExcessHorizontalSpace = true;
    customButton.setLayoutData(gd);
    customButton.addSelectionListener(selectionListener);
    if (imp != null) {
      // Check if C++ AE
      customButton.setEnabled(aeService.isCPlusPlusTopAE());
      if (aeService.getCustomValue() != null) {
        customButton.setSelection(true);
        if (aeService.getEnvironmentVariables() != null) {
          for (NameValue nv: aeService.getEnvironmentVariables()) {
            envName2NameValueMap.put(nv.getName(), nv);
          }
          customComposite = createCustomizationSection(serviceSectionClient);
        }
      }
    } else {
      customButton.setEnabled(false);
    }

    toolkit.paintBordersFor(topComposite);

    return section;
  } // createServiceSection
  
  private void createContextMenu() {
    // Create Open Action
    openAction = new Action(Messages.DDE_POPUP_ACTION_OPEN_IN_NEW_WINDOW) {
      public void run() {
        multiPageEditor.openTopLevelXmlDescriptor(aeService.getImportDescriptor());
      }
    };
    // Action when trying to open a file outside workspace
    notInWSAction = new Action(Messages.DDE_POPUP_ACTION_NOT_IN_WORKSPACE) {
      public void run() {
      }
    };
    notInWSAction.setEnabled(false);
    
    //  Create menu manager.
    MenuManager menuMgr = new MenuManager();
    menuMgr.setRemoveAllWhenShown(true);
    menuMgr.addMenuListener(new IMenuListener() {
      public void menuAboutToShow(IMenuManager mgr) {
        fillContextMenu(mgr);
      }
    });
    //  Create menu.
    Menu menu = menuMgr.createContextMenu(topDescriptorField);
    topDescriptorField.setMenu(menu);
    
    //  Register menu for extension.
    // getSite().registerContextMenu(menuMgr, topDescriptorField);
  }
 
  private void fillContextMenu(IMenuManager mgr) {
    String xml = topDescriptorField.getText().trim();
    if (xml.length() > 0) {
      
      String path = multiPageEditor.cde.getAbsolutePathFromImport(aeService.getImportDescriptor());
      IPath iPath = new Path(path);
      IFile[] files = multiPageEditor.cde.getProject().getWorkspace().getRoot().findFilesForLocation(iPath);
      if (null == files || files.length != 1) {
        mgr.add(notInWSAction);
        return;
      }

      mgr.add(openAction);
    }
  }
  
  /** ********************************************************************** */

  protected void updateTopDescriptor(String fileFullPath, boolean byLocation) {
    // Import by Location or by Name ?
    Import importDescriptor = null;
    try {
      if (byLocation) {
        // Import by Location
        importDescriptor = createLocationImport(fileFullPath);
      } else {
        // Import by Name
        importDescriptor = UimaDescriptionUtils.createByNameImport(fileFullPath,
                multiPageEditor.cde.createResourceManager());        
      }
    } catch (MalformedURLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return;
    }
    aeService.setImportDescriptor(importDescriptor);
    String relativeFile = UimaDescriptionUtils.getDescriptorFromImport(importDescriptor);
    try {
      // Set Async=false (default value) to handle the case of reloading the Top AE's descriptor
      aeService.getAnalysisEngineDeploymentMetaData().setAsync(false);
      
      ResourceSpecifier rs = aeService.resolveTopAnalysisEngineDescription(multiPageEditor.cde.createResourceManager(), false);
      if (rs != null) { 
        if (rs instanceof AnalysisEngineDescription) {
          AnalysisEngineDescription aed = (AnalysisEngineDescription) rs;
          
          AEDelegates_Impl delegates = new AEDelegates_Impl(aeService
                  .getAnalysisEngineDeploymentMetaData(multiPageEditor.cde.createResourceManager()));
          Map map = aed.getDelegateAnalysisEngineSpecifiers(multiPageEditor.cde.createResourceManager());
          for (Object m : map.entrySet()) {
            Map.Entry entry = (Map.Entry) m;
            // Trace.err("key: " + entry.getKey() + " ; " + entry.getValue().getClass().getName());
            AEDeploymentMetaData_Impl meta = new AEDeploymentMetaData_Impl();
            meta.setKey((String) entry.getKey());
  
            // Create a new Error Config
            if (entry.getValue() instanceof ResourceSpecifier) {
                Object obj = UIMAFramework.getResourceSpecifierFactory()
                                .createObject(AsyncAggregateErrorConfiguration.class);
                ((AsyncAEErrorConfiguration) obj).getGetMetadataErrors().setTimeout(0);
                meta.setAsyncAEErrorConfiguration((AsyncAEErrorConfiguration) obj);
            } else {
              Trace.bug("ResourceSpecifier == null for " + meta.getKey());
            }
  
            delegates.addDelegate(meta);
          }
  
          aeService.getAnalysisEngineDeploymentMetaData(multiPageEditor
                  .cde.createResourceManager()).setDelegates(delegates);
          
        }
        // If C++ descriptor, activate C++ settings
        updateCPlusPlusSettings ();
      } else {
        Trace.err("Cannot resolve: " + relativeFile);
      }
    } catch (InvalidXMLException e) {
      // e.printStackTrace();
      Utility.popMessage(Messages.getString("InvalidXMLException"), //$NON-NLS-1$
              multiPageEditor.cde.getMessagesToRootCause(e), MessageDialog.ERROR);
      multiPageEditor.switchToBadSource(true);
      return;
    }

    topDescriptorField.setText(relativeFile);
    isImportByLocation = byLocation;
    if (isImportByLocation) {
      importByNameOrLocation.setText("Location");
    } else {
      importByNameOrLocation.setText("Name");
    }

    multiPageEditor.refresh();
    multiPageEditor.setFileDirty();
  }

  /**
   * @param location
   * @return
   * @throws MalformedURLException
   */
  public Import createLocationImport(String location) throws MalformedURLException {

    String sDescriptorRelativePath = multiPageEditor.cde.getDescriptorRelativePath(location);
    // If relative path is not "relative", on Windows might get back
    // an absolute path starting with C: or something like it.
    // If a path starts with "C:", it must be preceeded by
    // file:/ so the C: is not interpreted as a "scheme".
    if (sDescriptorRelativePath.indexOf("file:/") == -1 //$NON-NLS-1$
            && sDescriptorRelativePath.indexOf(":/") > -1) { //$NON-NLS-1$
      sDescriptorRelativePath = "file:/" + sDescriptorRelativePath; //$NON-NLS-1$
    }

    Import imp = new Import_impl();
    // fails on unix? URL url = new URL("file:/" + getDescriptorDirectory());
    // Set relative Path Base
    // a version that might work on all platforms
    URL url = new File(multiPageEditor.cde.getDescriptorDirectory()).toURL();
    ((Import_impl) imp).setSourceUrl(url);

    imp.setLocation(sDescriptorRelativePath);
    return imp;
  }
  
  /** ********************************************************************** */

  public void setActive(boolean active) {
    if (active) {
      super.setActive(active);
      isValid();
    }
  }

  protected void isValid() {
    if (brokerUrl.getText().trim().length() == 0) {
      showStatus("The queue Broker Url is not specified", IMessageProvider.ERROR);
      brokerUrlDecoField.showDecoration(decorationBrokerUrl);
      // return;
    } else {
      brokerUrlDecoField.hideDecoration(decorationBrokerUrl);      
    }

    if (endPoint.getText().trim().length() == 0) {
      showStatus("The queue name is not specified", IMessageProvider.ERROR);
      endPointDecoField.showDecoration(decorationEndPoint);
      // return;
    } else {
      endPointDecoField.hideDecoration(decorationEndPoint);
    }

    if (topDescriptorField.getText().trim().length() == 0) {
      showStatus("The top-level analysis engine is not specified", IMessageProvider.ERROR);
      topDescriptorDecoField.showDecoration(decorationTopDescriptor);
      // return;
    } else {
      topDescriptorDecoField.hideDecoration(decorationTopDescriptor);
    }
    // showStatus(null, IMessageProvider.ERROR);
  }

  protected void showStatus(String msg, int msgType) {
    // Message.setMessage(form.getForm(), msg, msgType);
  }

  static public int validateFilePath(String filePath) {
    String path = filePath == null ? "" : filePath.trim();
    if (path.length() > 0) {
      File f = new File(path);
      if (f.exists() && f.isFile()) {
        return STATUS_IS_VALID;
      }
      // setErrorMessage("Working directory does not exist");
      return STATUS_FILE_NOT_EXIST;
    } else if (path.length() == 0) {
      return STATUS_FILE_NOT_SPECIFIED;
    }
    return STATUS_IS_VALID;
  }
  
  public void setInput (AEDeploymentDescription aeDD) {
    ignoreUpdate = true;
    aeDeploymentDescription = aeDD;
    aeService = aeDeploymentDescription.getAeService();
    
    nameText.setText(aeDeploymentDescription.getName());
    description.setText(aeDeploymentDescription.getDescription());
    version.setText(aeDeploymentDescription.getVersion());
    vendor.setText(aeDeploymentDescription.getVendor());

    brokerUrl.setText(aeService.getBrokerURL());
    endPoint.setText(aeService.getEndPoint());
    
    prefetch.setSelection(aeService.getPrefetch());
    casPoolSize.setSelection(aeDeploymentDescription.getCasPoolSize());
    initialFsHeapSize.setSelection(aeDeploymentDescription.getInitialFsHeapSize());
    
    String topDescriptor = null;
    Import imp = aeService.getImportDescriptor();
    if (imp != null) {
      isImportByLocation = true;
      String descr = imp.getLocation();
      if (descr == null) {
        isImportByLocation = false;
        descr = imp.getName();
      }
      topDescriptor = descr;
      if (isImportByLocation) {
        importByNameOrLocation.setText("Location");
      } else {
        importByNameOrLocation.setText("Name");
      }
      
      // Check if C++ AE
      updateCPlusPlusSettings ();
    } else {
      // Import is NOT defined
      importByNameOrLocation.setText("(Name or Location)");
    
    }

    topDescriptorField.setText(topDescriptor != null ? topDescriptor : "");
    ignoreUpdate = false;
  }
    
  /***************************************************************************/
  
  /**
   * Creates an editor for the value of the selected environment variable.
   */
  private void handleEditButton() {
    IStructuredSelection sel= (IStructuredSelection) envTableViewer.getSelection();
    NameValue var= (NameValue) sel.getFirstElement();
    if (var == null) {
      return;
    }
    String oldName  = var.getName();
    EditParamWizard wizard = new EditParamWizard(Messages.DDE_EnvVariable_Wizard_EDIT_Title, 
            Messages.DDE_EnvVariable_Wizard_EDIT_Description, var.getName(), var.getValue());
    WizardDialog dialog = new WizardDialog(form.getShell(), wizard);
    
    if (dialog.open() != Window.OK) {
      return;
    }
    String name  = wizard.getName();
    String value = wizard.getValue();
    
    // Delete OLD name/value from Map
    envName2NameValueMap.remove(oldName);
    
    if (name.length() == 0) {
      // Delete OLD name/value from Table
      envTableViewer.remove(var);
      envTableViewer.refresh();
      updateEnvironmentVariables ();
      return;
    }

    if (!oldName.equals(name)) {
      envTableViewer.remove(var);
      var = new NameValue(name, value);
      addAndCheckVariable(var);
    } else {
      var.setValue(value);
      envTableViewer.update(var, null);
    }
    envName2NameValueMap.put(name, var);
    updateEnvironmentVariables ();
  }

  private void handleAddButton () {
    EditParamWizard wizard = new EditParamWizard(Messages.DDE_EnvVariable_Wizard_ADD_Title, 
            Messages.DDE_EnvVariable_Wizard_ADD_Description, "", "");
    WizardDialog dialog = new WizardDialog(form.getShell(), wizard);
    if (dialog.open() != Window.OK) {
      return;
    }
    String name  = wizard.getName();
    String value = wizard.getValue();
    if (name.length() == 0) {
      return;
    }
    NameValue nv = new NameValue(name, value);
    addAndCheckVariable(nv);
    envName2NameValueMap.put(name, nv);
    updateEnvironmentVariables ();
  }
  
  private void handleDeleteButton() {
    IStructuredSelection sel= (IStructuredSelection) envTableViewer.getSelection();
    NameValue var= (NameValue) sel.getFirstElement();
    if (var == null) {
      return;
    }
    envName2NameValueMap.remove(var.getName());
    envTableViewer.remove(var);
    envTableViewer.refresh();
    updateEnvironmentVariables ();
  }
  
  protected boolean addAndCheckVariable(NameValue variable) {
    String name= variable.getName();
    TableItem[] items = envTableViewer.getTable().getItems();
    for (int i = 0; i < items.length; i++) {
      NameValue existingVariable = (NameValue) items[i].getData();
      // Duplicate name ?
      if (existingVariable.getName().equals(name)) {
        envTableViewer.remove(existingVariable);
        break;
      }
    }
    envTableViewer.add(variable);
    return true;
  }

  // TODO Need to optimize
  protected void updateEnvironmentVariables () {
    aeService.getEnvironmentVariables().clear();
    for (NameValue nv: envName2NameValueMap.values()) {
      aeService.getEnvironmentVariables().add(nv);
    }
  }
  
  /**
   * Enable C++ settings if top-level AE is C++
   * 
   * @return void
   */
  protected void updateCPlusPlusSettings () {
    if (aeService.isCPlusPlusTopAE()) {
      customButton.setEnabled(true);
      if (aeService.getCustomValue() != null) {
        customButton.setSelection(true);
        
        envName2NameValueMap.clear();
        if (aeService.getEnvironmentVariables() != null) {
          for (NameValue nv: aeService.getEnvironmentVariables()) {
            envName2NameValueMap.put(nv.getName(), nv);
          }
        }
        if (customComposite == null) {
          customComposite = createCustomizationSection(serviceSectionClient);
        } else {
          envTableViewer.setInput(envName2NameValueMap);
          // envTableViewer.refresh();
        }
        serviceSectionClient.layout(true, true);
        mForm.reflow(true);

      } else {
        customButton.setSelection(false);
        // Clear Env vars
        if (aeService.getEnvironmentVariables() != null) {
          aeService.getEnvironmentVariables().clear();
        }
        if (customComposite != null) {
          customComposite.dispose();
          customComposite = null;
        }
      }
    } else {
      customButton.setSelection(false);
      customButton.setEnabled(false);
      aeService.setCustomValue(null);
      if (aeService.getEnvironmentVariables() != null) {
        aeService.getEnvironmentVariables().clear();
      }
      if (customComposite != null) {
        customComposite.dispose();
        customComposite = null;
      }

    }
  }
}
