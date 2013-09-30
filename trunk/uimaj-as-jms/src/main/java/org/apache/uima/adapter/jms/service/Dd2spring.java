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

package org.apache.uima.adapter.jms.service;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.util.Level;

public class Dd2spring {

  private static final Class[] mainArg = new Class[] { String[].class };

  private static final Class THIS_CLASS = Dd2spring.class;

  private ClassLoader saxonClassLoader;

  /**
   * Test driver arg = path_to_source, path_to_xslt, path_to_saxon_jar, uima-as-debug flag
   * 
   * @param args
   */
  public static void main(String[] args) {
    new Dd2spring().convertDd2Spring(args[0], args[1], args[2], args[3]);
  }

  public File convertDd2Spring(String ddFilePath, String dd2SpringXsltFilePath,
          String saxonClasspath, String uimaAsDebug) {

    URL urlForSaxonClassPath;
    try {
      urlForSaxonClassPath = new URL(saxonClasspath);
    } catch (MalformedURLException e) {
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_Cannot_convert_saxon_classpath_to_a_URL_SEVERE",
              new Object[] { saxonClasspath });
      return null;
    }

    File tempFile;
    try {
      tempFile = File.createTempFile("UIMAdd2springOutput", ".xml");
    } catch (IOException e) {
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_cant_create_temp_output_file_SEVERE");
      return null;
    }

    convertDd2Spring(tempFile, ddFilePath, dd2SpringXsltFilePath, urlForSaxonClassPath);

    // delete the file when terminating if
    // a) uimaAsDebug is not specified (is null) or
    // b) uimaAsDebug is specified, but is ""
    if (null == uimaAsDebug || uimaAsDebug.equals("")) {
      tempFile.deleteOnExit();
    }

    return tempFile;
  }

  /**
   * 
   * @param ddFilePath
   *          file path to UIMA Deployment Descriptor - passed to saxon
   * @param dd2SpringXsltFilePath
   *          file path to dd2spring.xslt transformation file - passed to saxon
   * @param saxonClasspathURL
   *          classpath for saxon8.jar
   * @return temp file with generated Spring from dd2spring transform
   */
  public void convertDd2Spring(File tempFile, String ddFilePath, String dd2SpringXsltFilePath,
          URL saxonClasspathURL) {

    if (null == saxonClassLoader) {
      URL[] classLoaderUrls = new URL[] { saxonClasspathURL };

      // ClassLoader cl = new URLClassLoader(classLoaderUrls);
      // use the bootstrap class loader as the parent

      saxonClassLoader = new URLClassLoader(classLoaderUrls, Object.class.getClassLoader());
    }
    Class mainStartClass = null;
    try {
      mainStartClass = Class.forName("net.sf.saxon.Transform", true, saxonClassLoader);
    } catch (ClassNotFoundException e) {
      System.err.println("Error - can't load Saxon jar from " + saxonClasspathURL
              + " for dd2spring transformation.");
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_saxon_missing_SEVERE");
      return;
    }

    // args for saxon
    // -l -s deployment_descriptor} -o output_file_path dd2spring.xsl_file_path

    List<String> argsForSaxon = new ArrayList<String>();
    argsForSaxon.add("-l"); // turn on line numbers
    argsForSaxon.add("-s"); // source file
    argsForSaxon.add(ddFilePath); // source file
    argsForSaxon.add("-o"); // output file
    argsForSaxon.add(tempFile.getAbsolutePath()); // output file
    argsForSaxon.add(dd2SpringXsltFilePath); // xslt transform to apply

    if (null != System.getProperty("uima-as.dd2spring.noTempQueues")) {
      argsForSaxon.add("noTempQueues=true"); // disable generate of temp queues (for testing)
    }

    Method mainMethod = null;
    try {
      mainMethod = mainStartClass.getMethod("main", mainArg);
    } catch (SecurityException e) {
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_security_exception_calling_saxon");
      return;
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_internal_error_calling_saxon");
      return;
    }
    try {
      mainMethod.invoke(null,
              new Object[] { argsForSaxon.toArray(new String[argsForSaxon.size()]) });
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_internal_error_calling_saxon");
      return;
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_internal_error_calling_saxon");
      return;
    } catch (InvocationTargetException e) {
      e.printStackTrace();
      UIMAFramework.getLogger(THIS_CLASS).logrb(Level.CONFIG, THIS_CLASS.getName(),
              "convertDD2Spring", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMA_dd2spring_internal_error_calling_saxon");
      return;
    }

    return;
  }

}
