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

package org.apache.uima.dde.internal;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.uima.aae.deployment.AEDeploymentDescription;
import org.apache.uima.aae.deployment.UimaParserExtender;
import org.apache.uima.aae.deployment.impl.AEDeploymentDescription_Impl;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.dde.internal.page.AEConfigurationPage;
import org.apache.uima.dde.internal.page.OverviewPage;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.taeconfigurator.editors.MultiPageEditor;
import org.apache.uima.taeconfigurator.editors.MultiPageEditorContributor;
import org.apache.uima.taeconfigurator.editors.point.IUimaEditorExtension;
import org.apache.uima.taeconfigurator.editors.point.IUimaMultiPageEditor;
import org.apache.uima.taeconfigurator.editors.ui.AbstractSection;
import org.apache.uima.taeconfigurator.editors.ui.Utility;
import org.apache.uima.taeconfigurator.editors.xml.XMLEditor;
import org.apache.uima.tools.internal.cde.uima.util.UimaDescriptionUtils;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLizable;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IWorkspace;
import org.eclipse.core.resources.IWorkspaceRoot;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jface.dialogs.ErrorDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.text.IDocument;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.IFileEditorInput;
import org.eclipse.ui.ISaveablePart;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.part.FileEditorInput;


public class DeploymentDescriptorEditor implements IUimaMultiPageEditor, IUimaEditorExtension {

  protected boolean isBadXML = true;

  static public final String ID = "com.ibm.apache.uima.dde.internal.MultiPageEditor";

  static public final String TITLE = "UIMA DDE";
  static public final String VERSION = "0.1.2";
  
  public MultiPageEditor cde;

//  private String inputFileName = null;

  private IFile inputIFile;

  private AEDeploymentDescription aeDD;

  private OverviewPage overviewPage;
  
  private AEConfigurationPage aeConfigPage;

  /** The text editor used in page 0. */
  private XMLEditor sourceTextEditor;

  protected int sourceIndex = -1;
  
  /** ********************************************************************** */

  public DeploymentDescriptorEditor() {
    super();
  }
  
  public void init () {
    UimaParserExtender.initUimaApplicationFramework();
  }
  
  public boolean canEdit(MultiPageEditor cde, XMLizable xmlizable) {
    if (xmlizable instanceof AEDeploymentDescription) {
      return true;
    }
    return false;
  }

  public void activateEditor(IEditorSite site, IEditorInput editorInput, MultiPageEditor cde, 
          XMLizable xmlizable) throws PartInitException {
    this.cde = cde;
    aeDD = (AEDeploymentDescription) xmlizable;
    inputIFile = ((IFileEditorInput) cde.getEditorInput()).getFile();
    cde.setPartNameSuper(inputIFile.getName() + " (" + VERSION + ")");

    isBadXML = false;
    // init(site, editorInput);
    
    // XML source editor may be opened by CDE 
    // when the source is "initially" invalid
    if (cde.getSourceEditor() != null) {
      sourceTextEditor = cde.getSourceEditor();
    }
  }

/*  protected void init(IEditorSite site, IEditorInput input) throws PartInitException {
    if (!(input instanceof IFileEditorInput)) {
      throw new PartInitException("Invalid Input: " + input.getClass().getName() + " (Must be IFileEditorInput)");
    }

    cde.initSuper(site, input);

    String filePathName = null;
    inputIFile = ((IFileEditorInput) input).getFile();
    filePathName = inputIFile.getLocation().toOSString();
    cde.setPartNameSuper(inputIFile.getName() + " (" + VERSION + ")");

    try {
      aeDD = (AEDeploymentDescription) UimaXmlParsingUtil.parseUimaXmlDescriptor(filePathName);
      isBadXML = false;
      ResourceSpecifier aed;
      try {
        aed = aeDD.getAeService()
                    .resolveTopAnalysisEngineDescription(cde.createResourceManager(), false);
        if (aed instanceof AnalysisEngineDescription
                && ( ! ((AnalysisEngineDescription) aed).isPrimitive()) ) {
          // if (aeDD.getAeService().getAnalysisEngineDeploymentMetaData().isAsync()) {
          // Resolve delegates
          aeDD.getAeService().getAnalysisEngineDeploymentMetaData()
                                .resolveDelegates(cde.createResourceManager(), true);
        }
      } catch (InvalidXMLException e) {
        e.printStackTrace();
        isBadXML = true;
        Utility.popMessage(Messages.getString("InvalidXMLException"), //$NON-NLS-1$
                cde.getMessagesToRootCause(e), MessageDialog.ERROR);
      }

    } catch (InvalidXMLException e) {
      Utility.popMessage(
              Messages.getString("MultiPageEditor.XMLerrorInDescriptorTitle"), //$NON-NLS-1$
              Messages.getString("MultiPageEditor.XMLerrorInDescriptor") + "\n" + cde.getMessagesToRootCause(e), //$NON-NLS-1$ //$NON-NLS-2$
              MessageDialog.ERROR);
    } catch (IOException e) {
      e.printStackTrace();
      Utility.popMessage(Messages.getString("IOException"), //$NON-NLS-1$
              cde.getMessagesToRootCause(e), MessageDialog.ERROR);
    }
  } // init
*/  
  public void setFileDirty() {
    cde.setFileDirty();
  }

  public AEDeploymentDescription getAEDeploymentDescription() {
    return aeDD;
  }

  public void refresh() {
    aeConfigPage.setInput(aeDD);
  }

  public void setInput () {
    overviewPage.setInput(aeDD);
    aeConfigPage.setInput(aeDD);
  }
  
  // AEMetaDataDetailsPage needs to access OverviewPage to enable/disable some controls
  // (e.g., CAS Pool Size)
  public OverviewPage getOverviewPage () {
    return overviewPage;
  }
  
  /** ********************************************************************** */

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.forms.editor.FormEditor#addPages()
   */
  public void addPagesForCurrentEditor() {
    try {

      // Resolve "import" (and Delegates if any);
      if (aeDD != null) {
        ResourceSpecifier aed;
        try {
          aed = aeDD.getAeService()
                      .resolveTopAnalysisEngineDescription(cde.createResourceManager(), false);
          if (aed instanceof AnalysisEngineDescription
                  && ( ! ((AnalysisEngineDescription) aed).isPrimitive()) ) {
            // if (aeDD.getAeService().getAnalysisEngineDeploymentMetaData().isAsync()) {
            // Resolve delegates
            aeDD.getAeService().getAnalysisEngineDeploymentMetaData()
                                  .resolveDelegates(cde.createResourceManager(), true);
          }
        } catch (InvalidXMLException e) {
          e.printStackTrace();
          isBadXML = true;
          Utility.popMessage(Messages.getString("InvalidXMLException"), //$NON-NLS-1$
                  cde.getMessagesToRootCause(e), MessageDialog.ERROR);
        }
      }
      
      if (!isBadXML) {
        cde.addPage(overviewPage = new OverviewPage(this, "123", "Overview"));
        cde.addPage(aeConfigPage = new AEConfigurationPage(this, "123", "Deployment Configurations"));
      }
      createSourcePage();
            
    } catch (PartInitException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (isBadXML) {
      pageChangeForCurrentEditor(sourceIndex);
    }
  }

  /**
   * Creates a page of the multi-page editor, which contains a text editor.
   */
  protected void createSourcePage () {
    // XML source editor may be opened by CDE 
    // when the source is "initially" invalid
    if (sourceTextEditor != null) {
      return;
    }
    try {
      sourceTextEditor = new XMLEditor(cde);
      sourceIndex = cde.addPage(sourceTextEditor, cde.getEditorInput());
      cde.setPageTextSuper(sourceIndex, "Source");
    } catch (PartInitException e) {
      ErrorDialog.openError(cde.getSite().getShell(), "Error creating nested text editor", null, e
              .getStatus());
    }
  }

  public String prettyPrintModel() {
    return ((AEDeploymentDescription_Impl) aeDD).prettyPrint(MultiPageEditorContributor.getXMLindent());
  }

//  boolean inside = false;
  boolean isPageChangeRecursion = false;
/*  public void pageChangeForCurrentEditor_OLD(int newPageIndex) {
    if (inside) {
      return;
    }
    int oldPageIndex = cde.getCurrentPageSuper();

    if (newPageIndex == sourceIndex) {
      // Switch to Source Page
      if (!isBadXML) {
        updateSourceFromModel();
      }
    } else {
      // Switch FROM Source Page
      if (oldPageIndex == sourceIndex) {
        // Validate source
        if ( ! validateSource() ) {
          inside = true;
          cde.setActivePageSuper(sourceIndex);
          inside = false;
          return ;
        }
        cde.pageChangeSuper(newPageIndex);
        setInput();
        return;
      }
    }
    cde.pageChangeSuper(newPageIndex);
  }
*/
  public void pageChangeForCurrentEditor(int newPageIndex) {
    if (/*inside ||*/ isPageChangeRecursion) {
      return;
    }
    int oldPageIndex = cde.getCurrentPageSuper();
    
    if (oldPageIndex != -1) {
      if (oldPageIndex == sourceIndex) {
        if (!validateSource()) {
          setActivePageWhileBlockingRecursion(sourceIndex);
          return;
        }
      } 
    }

    // Set the current page to source page
    cde.pageChangeSuper(newPageIndex);
    
    if (newPageIndex == sourceIndex) {
      // Switch to Source Page
      if (!isBadXML) {
        updateSourceFromModel();
      }
    } else {
      // Switch FROM Source Page
      if (oldPageIndex == sourceIndex) {
        // Validate source
        if ( ! validateSource() ) {
          // inside = true;
          setActivePageWhileBlockingRecursion(sourceIndex);
          // inside = false;
          return ;
        }
        cde.pageChangeSuper(newPageIndex);
        setInput();
        return;
      }
    }
    setActivePageWhileBlockingRecursion(newPageIndex);
  }

  protected void setActivePageWhileBlockingRecursion(int sourceIndex) {
    try {
      isPageChangeRecursion = true;
      // next call needed to be done but wasn't prior to
      // Eclipse 3.2
      // In Eclipse 3.2 they fixed this, but call this now
      // calls pageChange, and makes a recursive loop
      // We break that loop here.
      cde.setActivePageSuper(sourceIndex); // isn't being done otherwise?
    } finally {
      isPageChangeRecursion = false;
    }
  }

  
  public void updateSourceFromModel() {
    sourceTextEditor.setIgnoreTextEvent(true);
    IDocument doc = sourceTextEditor.getDocumentProvider().getDocument(
            sourceTextEditor.getEditorInput());
    doc.set(prettyPrintModel());
    sourceTextEditor.setIgnoreTextEvent(false);
  }

  public void switchToBadSource (boolean genSource) {
    isBadXML = true;
    if (genSource) {
      updateSourceFromModel();
    }
      
    cde.setActivePageSuper(sourceIndex);
  }

  /** ********************************************************************** */

  // From CDE MultiPageEditor.java
  protected boolean validateSource () {    
    isBadXML = true; // preset
    IDocument doc = sourceTextEditor.getDocumentProvider().getDocument(
            sourceTextEditor.getEditorInput());
    String text = doc.get();
    InputStream is;
    try {
      is = new ByteArrayInputStream(text.getBytes(cde.getCharSet(text)));
    } catch (UnsupportedEncodingException e2) {
      Utility.popMessage(Messages.getString("MultiPageEditor.19"), //$NON-NLS-1$
              cde.getMessagesToRootCause(e2), MessageDialog.ERROR);
      // super.setActivePage(sourceIndex);
      return false;
    }

    // Set the location of XML descriptor
    String filePathName = getFile().getLocation().toString();
    XMLInputSource input = new XMLInputSource(is, new File(filePathName));

    // Parse the XML descriptor
    try {
      XMLizable inputDescription = UimaDescriptionUtils.parseDescriptor(input);
      if (inputDescription instanceof AEDeploymentDescription) {
        aeDD = (AEDeploymentDescription) inputDescription;
        ResourceSpecifier aed = aeDD.getAeService()
              .resolveTopAnalysisEngineDescription(cde.createResourceManager(), false);
        if (aed instanceof AnalysisEngineDescription
                && ( ! ((AnalysisEngineDescription) aed).isPrimitive()) ) {
        // if (aeDD.getAeService().getAnalysisEngineDeploymentMetaData().isAsync()) {
          // Resolve delegates
          aeDD.getAeService().getAnalysisEngineDeploymentMetaData()
              .resolveDelegates(cde.createResourceManager(), true);
        }
        isBadXML = false;
      } else {
        String msg = Messages.getFormattedString(
                "Unrecognized descriptor type for file ''{0}''.", //$NON-NLS-1$
                new String[] { AbstractSection.maybeShortenFileName(filePathName) })
                + "\n"; //$NON-NLS-1$
        Utility.popMessage("Error", //$NON-NLS-1$
                msg, MessageDialog.ERROR);
        return false;
      }
    } catch (InvalidXMLException e) {
      // super.setActivePage(sourceIndex);
      Utility.popMessage(Messages.getString("InvalidXMLException"), //$NON-NLS-1$
              cde.getMessagesToRootCause(e), MessageDialog.ERROR);
      return false;
    }
    
    return true;
  }

  /** ****************************   Possible Merge with CDE  ***************************** */

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.part.EditorPart#doSave(org.eclipse.core.runtime.IProgressMonitor)
   */
  public void doSaveForCurrentEditor(IProgressMonitor monitor) {
    if (cde.getCurrentPageSuper() != sourceIndex) {
      updateSourceFromModel(); // Only when it is not in source page
    }
    sourceTextEditor.doSave(monitor);
    cde.setFileDirtyFlag(false);
    cde.firePropertyChangeSuper(ISaveablePart.PROP_DIRTY);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.ui.part.EditorPart#doSaveAs()
   */
  public void doSaveAsForCurrentEditor() {
    if (cde.getCurrentPageSuper() != sourceIndex) {
      updateSourceFromModel(); // Only when it is not in source page
    }
    
    cde.setSaveAsStatus(cde.SAVE_AS_STARTED);
    sourceTextEditor.doSaveAs();

    if (cde.m_nSaveAsStatus == cde.SAVE_AS_CANCELLED) {
      cde.m_nSaveAsStatus = cde.SAVE_AS_NOT_IN_PROGRESS;
      return;
    }
    // should only do if editorInput is new
    FileEditorInput newEditorInput = (FileEditorInput) sourceTextEditor.getEditorInput();

    // if(old)
    cde.setInputSuper(newEditorInput);
    cde.firePropertyChangeSuper(cde.PROP_INPUT);
    // setTitle(newEditorInput.getFile().getName());
    cde.setPartNameSuper(newEditorInput.getFile().getName());
    // this next does NOT seem to change the overall page title

    cde.firePropertyChangeSuper(cde.PROP_TITLE);
    cde.setFileDirtyFlag(false);
    cde.firePropertyChangeSuper(ISaveablePart.PROP_DIRTY);
  }

  /** *************************   From CDE  ********************************** */
  
  /**
   * @return current file being edited
   */
  public IFile getFile() {
    return inputIFile;
  }

  /**
   * Returns the workspace instance.
   */
  public static IWorkspace getWorkspace() {
    return ResourcesPlugin.getWorkspace();
  }

  private IJavaProject javaProject = null;

  public IJavaProject getJavaProject() {
    IFile iFile = getFile();
    if (null == javaProject && null != iFile) {
      javaProject = JavaCore.create(iFile.getProject());
    }
    return javaProject;
  }

  /**
   * Convenience method to get the workspace root.
   */
  public IWorkspaceRoot getWorkspaceRoot() {
    return ResourcesPlugin.getWorkspace().getRoot();
  }

    // Won't work for "linked" project !
//  public String getDescriptorRelativePath(String aFullOrRelativePath) {
//    String sEditorFileFullPath = getFile().getLocation().toString();
//    String sFullOrRelativePath = aFullOrRelativePath.replace('\\', '/');
//
//    // Won't work for "linked" project !
//    // Comment out !
//    //
//    // first, if not in workspace, or if a relative path, not a full path, return path
//    // String sWorkspacePath = getWorkspace().getRoot().getLocation().toString();
//    // if (sFullOrRelativePath.indexOf(sWorkspacePath) == -1) {
//    // return sFullOrRelativePath;
//    // }
//
//    String sFullPath = sFullOrRelativePath; // rename the var to its semantics
//
//    String commonPrefix = getCommonParentFolder(sEditorFileFullPath, sFullPath);
//    if (commonPrefix.length() < 2 || commonPrefix.indexOf(':') == commonPrefix.length() - 2) {
//      return sFullPath;
//    }
//
//    // now count extra slashes to determine how many ..'s are needed
//    int nCountBackDirs = 0;
//    String sRelativePath = ""; //$NON-NLS-1$
//    for (int i = commonPrefix.length(); i < sEditorFileFullPath.length(); i++) {
//      if (sEditorFileFullPath.charAt(i) == '/') {
//        sRelativePath += "../"; //$NON-NLS-1$
//        nCountBackDirs++;
//      }
//    }
//    sRelativePath += sFullPath.substring(commonPrefix.length());
//    return sRelativePath;
//  }

  /**
   * Open top level's Xml descriptor in new window
   */
  public void openTopLevelXmlDescriptor(Import importItem) {
    cde.open(cde.getAbsolutePathFromImport(importItem));
  }

}
