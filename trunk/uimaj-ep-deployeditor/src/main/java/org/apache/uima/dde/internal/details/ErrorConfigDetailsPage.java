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

import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration;
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.aae.deployment.impl.AEDeploymentMetaData_Impl;
import org.apache.uima.aae.deployment.impl.DeploymentMetaData_Impl;
import org.apache.uima.dde.internal.DeploymentDescriptorEditor;
import org.apache.uima.dde.internal.Messages;
import org.apache.uima.dde.internal.provider.ErrorConfigContentProvider;
import org.apache.uima.dde.internal.provider.ErrorsConfigLabelProvider;
import org.apache.uima.dde.internal.provider.NameValuePair;
import org.apache.uima.tools.debug.util.Trace;
import org.apache.uima.tools.internal.ui.forms.FormSection;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.TableEditor;
import org.eclipse.swt.custom.TreeEditor;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.ui.forms.FormColors;
import org.eclipse.ui.forms.IDetailsPage;
import org.eclipse.ui.forms.IFormPart;
import org.eclipse.ui.forms.IManagedForm;
import org.eclipse.ui.forms.SectionPart;
import org.eclipse.ui.forms.events.ExpansionAdapter;
import org.eclipse.ui.forms.events.ExpansionEvent;
import org.eclipse.ui.forms.widgets.FormToolkit;
import org.eclipse.ui.forms.widgets.ScrolledPageBook;
import org.eclipse.ui.forms.widgets.Section;
import org.eclipse.ui.forms.widgets.TableWrapData;
import org.eclipse.ui.forms.widgets.TableWrapLayout;


public class ErrorConfigDetailsPage implements IDetailsPage {
  private DeploymentDescriptorEditor multiPageEditor;

  private boolean isDelegate;

  private Object selectedObject = null;

  private IManagedForm mform;

  private FormToolkit toolkit;

//  private ScrolledPageBook myScrolledPageBook = null; // used to support Section

  private Section sectionErrorConfigDetails;

  protected TableViewer getMetaDataViewer;

  protected TableViewer processCasErrorsViewer;

  protected TableViewer collProcessCompleteErrorsViewer;

  protected Button buttonGetMetaData;

  protected Button buttonProcessCasErrors;

  protected Button buttonCollCompleteErrors;

  protected Table tableGetMetaData;

  protected Table tableProcessCasErrors;

  protected Table tableCollCompleteErrors;

  /** ********************************************************************** */

  private class WidgetListener implements ModifyListener, SelectionListener {
    public void modifyText(ModifyEvent e) {
    }

    public void widgetSelected(SelectionEvent e) {
      Object source = e.getSource();

      if (source == buttonGetMetaData) {
        tableGetMetaData.setEnabled(buttonGetMetaData.getSelection());

      } else if (source == buttonProcessCasErrors) {
        tableProcessCasErrors.setEnabled(buttonProcessCasErrors.getSelection());

      } else if (source == buttonCollCompleteErrors) {
        tableCollCompleteErrors.setEnabled(buttonCollCompleteErrors.getSelection());
      } else {

      }
    }

    public void widgetDefaultSelected(SelectionEvent e) {
    }
  }

//  private WidgetListener fListener = new WidgetListener();

  /** ********************************************************************** */

  public ErrorConfigDetailsPage(DeploymentDescriptorEditor editor, IManagedForm mform,
          boolean isDelegate) {
    this.multiPageEditor = editor;
    this.mform = mform;
    this.toolkit = mform.getToolkit();
    this.isDelegate = isDelegate;
  }

  //
  // Note: "parent" is a "LayoutComposite" created by "ScrolledPageBook pageBook"
  // in "DetailsPart".
  // The parent of "parent" is "WrappedPageBook pageBook".
  // The grand-parent of "parent" is ScrolledPageBook which needs to be
  // "reflowed" when Section is expanded/collapsed.
  public void createContents(Composite parent) {
    // Get ScrolledPageBook
//    myScrolledPageBook = (ScrolledPageBook) parent.getParent().getParent();

    // Set Layout for "parent"
    TableWrapLayout layout = new TableWrapLayout();
    layout.topMargin = 0;
    layout.leftMargin = 5;
    layout.rightMargin = 2;
    layout.bottomMargin = 2;
    parent.setLayout(layout);

    createConfigurationsSection(parent, toolkit);
  }

  public Section createConfigurationsSection(Composite parent, FormToolkit toolkit) {
    sectionErrorConfigDetails = FormSection.createTableWrapDataSection(toolkit, parent, 
            Section.TWISTIE | Section.EXPANDED, 
            Messages.DDE_AEConfigPage_ErrorConfig_Section_Title,
            "The following is the information about the ...", 10, 5, TableWrapData.FILL_GRAB,
            TableWrapData.FILL_GRAB, 1, 1);
    final SectionPart spart = new SectionPart(sectionErrorConfigDetails);
    mform.addPart(spart);
    spart.initialize(mform); // Need this code. Otherwise, exception in SectionPart !!!
    sectionErrorConfigDetails.setExpanded(true);

    // /////////////////////////////////////////////////////////////////////

    Composite sectionClient = toolkit.createComposite(sectionErrorConfigDetails);
    TableWrapLayout tl = new TableWrapLayout();
    tl.numColumns = 1;
    tl.leftMargin = 2;
    tl.rightMargin = 2;
    tl.topMargin = 2;
    tl.bottomMargin = 6;    // cannot be < 6 (otherwise, lost bottom-border of table)
    sectionClient.setLayout(tl);
    sectionErrorConfigDetails.setClient(sectionClient);
    toolkit.paintBordersFor(sectionClient);
    
    // /////////////////////////////////////////////////////////////////////

    // GetMetadataError
    if (isDelegate) {
      toolkit.createLabel(sectionClient, "Get Metadata Error:");
      tableGetMetaData = createErrorsTable(sectionClient, 3);
      getMetaDataViewer = new TableViewer(tableGetMetaData);
      getMetaDataViewer.setContentProvider(new ErrorConfigContentProvider());
      getMetaDataViewer.setLabelProvider(new ErrorsConfigLabelProvider(getMetaDataViewer));
      createEditorsForTableColumn(tableGetMetaData);
      // Vertical Spacing
      new Label(sectionClient, SWT.NONE).setText("");
    }

    // ProcessCasErrors
    // buttonProcessCasErrors = toolkit.createButton(sectionClient, "ProcessCasErrors:", SWT.CHECK);
    toolkit.createLabel(sectionClient, "Process CAS Errors:");
    tableProcessCasErrors = createErrorsTable(sectionClient, isDelegate ? 6 : 3);
    processCasErrorsViewer = new TableViewer(tableProcessCasErrors);
    processCasErrorsViewer.setContentProvider(new ErrorConfigContentProvider());
    processCasErrorsViewer.setLabelProvider(new ErrorsConfigLabelProvider(processCasErrorsViewer));
    createEditorsForTableColumn(tableProcessCasErrors);

    // Vertical Spacing
    new Label(sectionClient, SWT.NONE).setText("");

    // CollectionProcessCompleteError
    // buttonCollCompleteErrors = toolkit.createButton(sectionClient,
    // "CollectionProcessCompleteError:", SWT.CHECK);
    toolkit.createLabel(sectionClient, "Collection Process Complete Error:");
    tableCollCompleteErrors = createErrorsTable(sectionClient, 2);
    collProcessCompleteErrorsViewer = new TableViewer(tableCollCompleteErrors);
    collProcessCompleteErrorsViewer.setContentProvider(new ErrorConfigContentProvider());
    collProcessCompleteErrorsViewer.setLabelProvider(new ErrorsConfigLabelProvider(collProcessCompleteErrorsViewer));
    createEditorsForTableColumn(tableCollCompleteErrors);

    return sectionErrorConfigDetails;
  } // createConfigurationsSection

  private Table createErrorsTable(Composite parent, int totalItems) {
    final Table table = toolkit.createTable(parent, SWT.HIDE_SELECTION | SWT.FULL_SELECTION
            | SWT.H_SCROLL | SWT.V_SCROLL);
    table.setLinesVisible(true);
    table.setHeaderVisible(true);
    String headers[] = { "Name", "Value" };
    final TableColumn columns[] = new TableColumn[headers.length];
    for (int i = 0; i < headers.length; i++) {
      TableColumn tc = new TableColumn(table, SWT.NONE, i);
      tc.setResizable(true);
      tc.setText(headers[i]);
      if (i == 0) {
        tc.setWidth(200);
      } else {
        tc.setWidth(100);
      }
      columns[i] = tc;
    }
    TableWrapData gd = new TableWrapData(TableWrapData.FILL_GRAB, TableWrapData.FILL_GRAB);
    gd.grabHorizontal = true;
    gd.valign = TableWrapData.FILL;
    gd.heightHint = table.getHeaderHeight() + (table.getItemHeight() * (totalItems + 1)); // +1
    table.setLayoutData(gd);

    return table;
  }

  public void displayDetails(DeploymentMetaData_Impl meta, AsyncAEErrorConfiguration obj) {
    sectionErrorConfigDetails.setText(Messages.DDE_AEConfigPage_ErrorConfig_Section_Title
            + " - " + meta.getKey());

    if (getMetaDataViewer != null && obj.getGetMetadataErrors() != null) {
      getMetaDataViewer.setInput(obj.getGetMetadataErrors());
    }

    if (obj.getProcessCasErrors() != null) {
      processCasErrorsViewer.setInput(obj.getProcessCasErrors());
    }

    if (obj.getCollectionProcessCompleteErrors() != null) {
      collProcessCompleteErrorsViewer.setInput(obj.getCollectionProcessCompleteErrors());
    }
  }

  private void createEditorsForTree(final Tree tree) {
    final TreeEditor editor1 = new TreeEditor(tree);
    editor1.horizontalAlignment = SWT.CENTER;
    editor1.minimumWidth = 60;

    final TreeEditor editor2 = new TreeEditor(tree);
    editor2.horizontalAlignment = SWT.CENTER;
    editor2.grabHorizontal = true;
    editor2.minimumWidth = 60;

    tree.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        // Clean up any previous editor control
        Control oldEditor = editor1.getEditor();
        if (oldEditor != null)
          oldEditor.dispose();

        oldEditor = editor2.getEditor();
        if (oldEditor != null)
          oldEditor.dispose();

        // Identify the selected row
        final TreeItem item = (TreeItem) e.item;
        // if (!isEditable(item))
        // return;

        final Spinner spinner = new Spinner(tree, SWT.BORDER);
        spinner.setMinimum(0);
        String level = item.getText(1);
        int defaultLevel = level.length() == 0 || "default".equals(level) ? 0 : Integer.parseInt(level); //$NON-NLS-1$
        spinner.setSelection(defaultLevel);
        spinner.addModifyListener(new ModifyListener() {
          public void modifyText(ModifyEvent e) {
            if (item.getChecked()) {
              int selection = spinner.getSelection();
              item.setText(1, selection == 0 ? "default" //$NON-NLS-1$
                      : Integer.toString(selection));
              // fTab.updateLaunchConfigurationDialog();
            }
          }
        });
        editor1.setEditor(spinner, item, 1);

        final CCombo combo = new CCombo(tree, SWT.BORDER | SWT.READ_ONLY);
        combo.setItems(new String[] { "default", Boolean.toString(true), Boolean.toString(false) }); //$NON-NLS-1$
        combo.setText(item.getText(2));
        combo.pack();
        combo.addSelectionListener(new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {
            if (item.getChecked()) {
              item.setText(2, combo.getText());
              // fTab.updateLaunchConfigurationDialog();
            }
          }
        });
        editor2.setEditor(combo, item, 2);

      }
    });
  }

  private void createEditorsForTableColumn(final Table table) {
    final TableEditor editor1 = new TableEditor(table);
    editor1.horizontalAlignment = SWT.CENTER;
    editor1.grabHorizontal = true;
    editor1.minimumWidth = 60;

    final TableEditor editor2 = new TableEditor(table);
    editor2.horizontalAlignment = SWT.CENTER;
    editor2.grabHorizontal = true;
    editor2.minimumWidth = 60;
    table.addSelectionListener(new SelectionAdapter() {

      public void widgetSelected(SelectionEvent e) {
        // Clean up any previous editor control
        Control oldEditor = editor1.getEditor();
        if (oldEditor != null) {
          oldEditor.dispose();
        }

        oldEditor = editor2.getEditor();
        if (oldEditor != null) {
          oldEditor.dispose();
        }
        
        // Identify the selected row
        final TableItem item = (TableItem) e.item;
        if (!isEditable(item)) {
          table.setSelection(new TableItem[0]);
          return;
        }
      }

      public void widgetDefaultSelected(SelectionEvent e) {
        // Clean up any previous editor control
        Control oldEditor = editor1.getEditor();
        if (oldEditor != null) {
          oldEditor.dispose();
        }

        oldEditor = editor2.getEditor();
        if (oldEditor != null) {
          oldEditor.dispose();
        }

        // Identify the selected row
        final TableItem item = (TableItem) e.item;
        if (!isEditable(item)) {
          table.setSelection(new TableItem[0]);
          return;
        }

        final NameValuePair nvp = (NameValuePair) item.getData();
        if (nvp == null) {
          Trace.err("No item's object");
          return;
        }
        final Object obj = nvp.getParent();

        if (nvp.getType() == Integer.class) {
          // For Integer Editing
          final Spinner spinner = new Spinner(table, SWT.BORDER);
          spinner.setMinimum(0);
          spinner.setMaximum(Integer.MAX_VALUE);
          String value = item.getText(1);
          if (value.equals(AEDeploymentConstants.ERROR_KIND_STRING_NO_TIMEOUT)
                  || value.equals(AEDeploymentConstants.ERROR_KIND_STRING_NO_RETRIES)
                  || value.equals(AEDeploymentConstants.ERROR_KIND_STRING_NO_THRESHOLD_COUNT)
                  || value.equals(AEDeploymentConstants.ERROR_KIND_STRING_NO_THRESHOLD_WINDOW)) {
            value = "0";
          }
          int level = Integer.parseInt(value);
          spinner.setSelection(level);
          spinner.setFocus();
          spinner.addFocusListener(new FocusListener() {

            public void focusGained(FocusEvent e) {
            }

            public void focusLost(FocusEvent e) {
              // Clean up any previous editor control
              Control oldEditor = editor1.getEditor();
              if (oldEditor != null)
                oldEditor.dispose();

              oldEditor = editor2.getEditor();
              if (oldEditor != null)
                oldEditor.dispose();
              spinner.dispose();
            }

          });
          spinner.addModifyListener(new ModifyListener() {
            public void modifyText(ModifyEvent e) {
              int selection = spinner.getSelection();
              item.setText(1, Integer.toString(selection));
              int id = nvp.getId();
              if (obj instanceof GetMetadataErrors) {
                GetMetadataErrors getMetadataErrors = (GetMetadataErrors) obj;
                getMetadataErrors.setValueById(id, selection);
                getMetaDataViewer.refresh();

              } else if (obj instanceof ProcessCasErrors) {
                ProcessCasErrors processCasErrors = (ProcessCasErrors) obj;
                processCasErrors.setValueById(id, selection);
                processCasErrorsViewer.refresh();

              } else if (obj instanceof CollectionProcessCompleteErrors) {
                CollectionProcessCompleteErrors completeErrors = (CollectionProcessCompleteErrors) obj;
                completeErrors.setValueById(id, selection);
                collProcessCompleteErrorsViewer.refresh();
              }
              multiPageEditor.setFileDirty();
            }
          });
          editor1.setEditor(spinner, item, 1);

        } else if (nvp.getType() == String.class) {
          final CCombo combo = new CCombo(table, SWT.BORDER | SWT.READ_ONLY);
          combo.setItems(new String[] { "terminate", "disable" }); //$NON-NLS-1$
          combo.setText(item.getText(1));
          combo.pack();
          combo.setFocus();
          combo.addFocusListener(new FocusListener() {

            public void focusGained(FocusEvent e) {
            }

            public void focusLost(FocusEvent e) {
              // Clean up any previous editor control
              Control oldEditor = editor1.getEditor();
              if (oldEditor != null)
                oldEditor.dispose();

              oldEditor = editor2.getEditor();
              if (oldEditor != null)
                oldEditor.dispose();
              combo.dispose();
            }

          });
          combo.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              String selection = combo.getText();
              item.setText(1, selection);
              int id = nvp.getId();
              if (obj instanceof GetMetadataErrors) {
                GetMetadataErrors getMetadataErrors = (GetMetadataErrors) obj;
                getMetadataErrors.setValueById(id, selection);
                getMetaDataViewer.refresh();

              } else if (obj instanceof ProcessCasErrors) {
                ProcessCasErrors processCasErrors = (ProcessCasErrors) obj;
                processCasErrors.setValueById(id, selection);
                processCasErrorsViewer.refresh();

              } else if (obj instanceof CollectionProcessCompleteErrors) {
                CollectionProcessCompleteErrors completeErrors = (CollectionProcessCompleteErrors) obj;
                completeErrors.setValueById(id, selection);
                collProcessCompleteErrorsViewer.refresh();
              }
              multiPageEditor.setFileDirty();
            }
          });
          editor2.setEditor(combo, item, 1);

        } else if (nvp.getType() == Boolean.class) {
          // Only for ProcessCasErrors
          if (obj instanceof ProcessCasErrors) {
            final CCombo combo = new CCombo(table, SWT.BORDER | SWT.READ_ONLY);
            combo.setItems(new String[] { Boolean.toString(true), Boolean.toString(false) }); //$NON-NLS-1$
            combo.setText(item.getText(1));
            combo.pack();
            combo.setFocus();
            combo.addFocusListener(new FocusListener() {

              public void focusGained(FocusEvent e) {
              }

              public void focusLost(FocusEvent e) {
                // Clean up any previous editor control
                Control oldEditor = editor1.getEditor();
                if (oldEditor != null)
                  oldEditor.dispose();

                oldEditor = editor2.getEditor();
                if (oldEditor != null)
                  oldEditor.dispose();
                combo.dispose();
              }

            });
            combo.addSelectionListener(new SelectionAdapter() {
              public void widgetSelected(SelectionEvent e) {
                String selection = combo.getText();
                int id = nvp.getId();
                item.setText(1,selection);
                ProcessCasErrors processCasErrors = (ProcessCasErrors) obj;
                processCasErrors.setValueById(id, selection);
                processCasErrorsViewer.refresh();
                multiPageEditor.setFileDirty();
              }
            });
            editor2.setEditor(combo, item, 1);
          }
        } else {
          Trace.err("nvp.getType(): " + nvp.getType().getClass().getName());
        }
      }
    });
  }

  private void createEditorsForTable_NOT_USED(final Table table) {
    final TableEditor editor1 = new TableEditor(table);
    editor1.horizontalAlignment = SWT.CENTER;
    editor1.grabHorizontal = true;
    editor1.minimumWidth = 60;

    final TableEditor editor2 = new TableEditor(table);
    editor2.horizontalAlignment = SWT.CENTER;
    editor2.grabHorizontal = true;
    editor2.minimumWidth = 60;

    table.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected(SelectionEvent e) {
        // Clean up any previous editor control
        Control oldEditor = editor1.getEditor();
        if (oldEditor != null)
          oldEditor.dispose();

        oldEditor = editor2.getEditor();
        if (oldEditor != null)
          oldEditor.dispose();

        // Identify the selected row
        final TableItem item = (TableItem) e.item;
        // if (!isEditable(item))
        // return;

        final NameValuePair nvp = (NameValuePair) item.getData();
        if (nvp == null) {
          Trace.err("No item's object");
          return;
        }
        final Object obj = nvp.getParent();

        if (nvp.getType() == Integer.class) {
          // For Integer Editing
          final Spinner spinner = new Spinner(table, SWT.BORDER);
          spinner.setMinimum(0);
          spinner.setMaximum(Integer.MAX_VALUE);
          int level = Integer.parseInt(item.getText(1));
          spinner.setSelection(level);

          spinner.addModifyListener(new ModifyListener() {
            public void modifyText(ModifyEvent e) {
              int selection = spinner.getSelection();
              item.setText(1, Integer.toString(selection));
              int id = nvp.getId();
              if (obj instanceof GetMetadataErrors) {
                GetMetadataErrors getMetadataErrors = (GetMetadataErrors) obj;
                getMetadataErrors.setValueById(id, selection);
                getMetaDataViewer.refresh();

              } else if (obj instanceof ProcessCasErrors) {
                ProcessCasErrors processCasErrors = (ProcessCasErrors) obj;
                processCasErrors.setValueById(id, selection);
                processCasErrorsViewer.refresh();

              } else if (obj instanceof CollectionProcessCompleteErrors) {
                CollectionProcessCompleteErrors completeErrors = (CollectionProcessCompleteErrors) obj;
                completeErrors.setValueById(id, selection);
                collProcessCompleteErrorsViewer.refresh();
              }
            }
          });
          editor1.setEditor(spinner, item, 1);

        } else if (nvp.getType() == String.class) {
          final CCombo combo = new CCombo(table, SWT.BORDER | SWT.READ_ONLY);
          combo.setItems(new String[] { "terminate", "disable" }); //$NON-NLS-1$
          combo.setText(item.getText(1));
          combo.pack();
          combo.addSelectionListener(new SelectionAdapter() {
            public void widgetSelected(SelectionEvent e) {
              String selection = combo.getText();
              item.setText(1, selection);
              int id = nvp.getId();
              if (obj instanceof GetMetadataErrors) {
                GetMetadataErrors getMetadataErrors = (GetMetadataErrors) obj;
                getMetadataErrors.setValueById(id, selection);
                getMetaDataViewer.refresh();

              } else if (obj instanceof ProcessCasErrors) {
                ProcessCasErrors processCasErrors = (ProcessCasErrors) obj;
                processCasErrors.setValueById(id, selection);
                processCasErrorsViewer.refresh();

              } else if (obj instanceof CollectionProcessCompleteErrors) {
                CollectionProcessCompleteErrors completeErrors = (CollectionProcessCompleteErrors) obj;
                completeErrors.setValueById(id, selection);
                collProcessCompleteErrorsViewer.refresh();
              }
            }
          });
          editor2.setEditor(combo, item, 1);

        } else if (nvp.getType() == Boolean.class) {
          // Only for ProcessCasErrors
          if (obj instanceof ProcessCasErrors) {
            final CCombo combo = new CCombo(table, SWT.BORDER | SWT.READ_ONLY);
            combo.setItems(new String[] { Boolean.toString(true), Boolean.toString(false) }); //$NON-NLS-1$
            combo.setText(item.getText(1));
            combo.pack();
            combo.addSelectionListener(new SelectionAdapter() {
              public void widgetSelected(SelectionEvent e) {
                int id = nvp.getId();
                item.setText(1, combo.getText());
                ProcessCasErrors processCasErrors = (ProcessCasErrors) obj;
                processCasErrors.setValueById(id, combo.getText());
                processCasErrorsViewer.refresh();
              }
            });
            editor2.setEditor(combo, item, 1);
          }
        } else {
          Trace.err("nvp.getType(): " + nvp.getType().getClass().getName());
        }
      }
    });
  }

  private boolean isEditable (TableItem item) {
    final NameValuePair nvp = (NameValuePair) item.getData();
    if (nvp == null) {
      Trace.err("No item's object");
      return false;
    }
    return !(nvp.getStatusFlags() == NameValuePair.STATUS_NON_EDITABLE);
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
    if (selectedObject instanceof AsyncAggregateErrorConfiguration) {
      // displayDetails((AsyncAggregateErrorConfiguration) selectedObject);
    } else {
      Trace.err("selectedObject: " + selectedObject.getClass().getName());
    }
  }

}
