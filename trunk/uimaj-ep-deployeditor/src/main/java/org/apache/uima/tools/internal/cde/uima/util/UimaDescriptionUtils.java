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

package org.apache.uima.tools.internal.cde.uima.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.uima.UIMAFramework;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.impl.Import_impl;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.apache.uima.util.XMLizable;
import org.eclipse.core.resources.IFile;

/**
 * A uitility class for UIMA's description objects
 * 
 * see CDE AbstractSection
 */
public class UimaDescriptionUtils {
  

  // From CDE AbstractSection
  private static URL urlForResourceSpecifierSchema;
  static {
    try {
      urlForResourceSpecifierSchema = new URL("file:resourceSpecifierSchema.xsd");
    } catch (MalformedURLException e) {
      urlForResourceSpecifierSchema = null;
    }
  }

  // From CDE AbstractSection
  public static XMLizable parseDescriptor(XMLInputSource input) throws InvalidXMLException {
    // turn off environment variable expansion
    XMLParser.ParsingOptions parsingOptions = new XMLParser.ParsingOptions(false);
    XMLParser parser = UIMAFramework.getXMLParser();
    // disabled - error messages from XML validation not very helpful
    // parser.enableSchemaValidation(true);
    return parser.parse(input, "http://uima.apache.org/resourceSpecifier",
            urlForResourceSpecifierSchema, parsingOptions);
  }


  /**
   * @param location
   * @return
   * @throws MalformedURLException
   */
  static public Import createByLocationImport(IFile parentFile, String location)
          throws MalformedURLException {

    String sDescriptorRelativePath = getDescriptorRelativePath(parentFile.getLocation().toString(),
            location);
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
    URL url = new File(extractDirectory(parentFile)).toURL();
    ((Import_impl) imp).setSourceUrl(url);

    imp.setLocation(sDescriptorRelativePath);
    return imp;
  }

  static public Import createByNameImport(String fileName, ResourceManager rm) {
    if (fileName.endsWith(".xml"))
      fileName = fileName.substring(0, fileName.length() - 4);
    fileName = fileName.replace('\\', '/');
    fileName = fileName.replace('/', '.');
    int i = fileName.indexOf(":");
    if (i >= 0)
      fileName = fileName.substring(i + 1);
    if (fileName.charAt(0) == '.')
      fileName = fileName.substring(1);
    int partStart = 0;

    Import imp = UIMAFramework.getResourceSpecifierFactory().createImport();

    for (;;) {
      imp.setName(fileName.substring(partStart));
      try {
        imp.findAbsoluteUrl(rm);
      } catch (InvalidXMLException e) {
        partStart = fileName.indexOf('.', partStart) + 1;
        if (0 == partStart)
          return imp; // not found -outer code will catch error later
        continue;
      }
      return imp;
    }
  }

  static public String getDescriptorFromImport(Import imp) {
    String descr = null;
    if (imp != null) {
      descr = imp.getLocation();
      if (descr == null) {
        descr = imp.getName();
      }
    }
    return descr;
  }
  
  /** ********************************************************************** */

  static public String extractDirectory(IFile file) {
    String sDir = file.getParent().getLocation().toString();
    if (sDir.charAt(sDir.length() - 1) != '/') {
      sDir += '/';
    }
    return sDir;
  }

  static public String getDescriptorRelativePath(String sParentFileFullPath,
          String aFullOrRelativePath) {
    String sFullOrRelativePath = aFullOrRelativePath.replace('\\', '/');

    // first, if not in workspace, or if a relative path, not a full path, return path
    // String sWorkspacePath =
    // TAEConfiguratorPlugin.getWorkspace().getRoot().getLocation().toString();
    // if (sFullOrRelativePath.indexOf(sWorkspacePath) == -1) {
    // return sFullOrRelativePath;
    // }

    String sFullPath = sFullOrRelativePath; // rename the var to its semantics

    String commonPrefix = getCommonParentFolder(sParentFileFullPath, sFullPath);
    if (commonPrefix.length() < 2 || commonPrefix.indexOf(':') == commonPrefix.length() - 2) {
      return sFullPath;
    }

    // now count extra slashes to determine how many ..'s are needed
    int nCountBackDirs = 0;
    String sRelativePath = ""; //$NON-NLS-1$
    for (int i = commonPrefix.length(); i < sParentFileFullPath.length(); i++) {
      if (sParentFileFullPath.charAt(i) == '/') {
        sRelativePath += "../"; //$NON-NLS-1$
        nCountBackDirs++;
      }
    }
    sRelativePath += sFullPath.substring(commonPrefix.length());
    return sRelativePath;
  }

  private static String getCommonParentFolder(String sFile1, String sFile2) {
    if (sFile1 == null || sFile2 == null) {
      return ""; //$NON-NLS-1$
    }

    int maxLength = (sFile1.length() <= sFile2.length() ? sFile1.length() : sFile2.length());
    int commonPrefixLength = 0;
    for (int i = 0; i < maxLength; i++) {
      if (sFile1.charAt(i) != sFile2.charAt(i) || (i == maxLength - 1)) { // catch files which have
        // same prefix
        for (int j = i; j >= 0; j--) {
          if (sFile1.charAt(j) == '/' || sFile1.charAt(j) == '\\') {
            commonPrefixLength = j + 1;
            break;
          }
        }
        break;
      }
    }

    return sFile1.substring(0, commonPrefixLength);
  }

}
