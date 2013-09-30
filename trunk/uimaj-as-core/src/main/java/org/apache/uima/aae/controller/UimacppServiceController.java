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

package org.apache.uima.aae.controller;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.uima.UIMAException;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.jmx.JmxManagement;
import org.apache.uima.internal.util.JmxMBeanAgent;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.springframework.beans.factory.DisposableBean;

/**
 * This bean functions as a proxy for a Uima C++ service. It starts the Uima C++ service given a
 * UIMA descriptor, the input queue name, and environment settings required for the specific
 * annotator and the Uima C++ runtime. On start up a socket connection is established between this
 * instance of the proxy and the service. This connection is used for routing log message from the
 * C++ service to the UIMA framework logger and to allow the proxy to send JMX and administrative
 * requests such as shutdown.
 * 
 */
public class UimacppServiceController extends AnalysisEngineControllerAdapter implements
        ControllerLifecycle, DisposableBean {
  private static final Class CLASS_NAME = UimacppServiceController.class;

  private static final String STARTING_DIRECTORY = "UIMACPP_STARTING_DIRECTORY";

  protected ServerSocket server;

  private int port;

  private Socket loggerConnection;

  private Socket commandConnection;

  protected Process uimacppProcess;

  private LoggerHandler loggerHandler;

  private StdoutHandler stdoutHandler;

  private StderrHandler stderrHandler;

  private ProcessBuilder builder;

  private String startingDirectory;

  private String aeDesc;

  private String queueName;

  private int numInstances;

  private int processCasErrorThreshhold;

  private int processCasErrorWindow;

  private boolean terminateOnCPCError;

  private org.apache.uima.util.Logger uimaLogger;

  protected UimacppServiceManagement mbean;

  private JmxManagement jmxMgmt;

  private UimacppShutdownHook shutdownHook;

  private int initialFsHeapSize;

  private ArrayList<ControllerCallbackListener> listeners = new ArrayList<ControllerCallbackListener>();

  private Boolean InitializedState = false;

  private Exception InitializedStatus = null;
  
  private Boolean isStopped = false;

  private String uimacppHome;

  /**
   * Configure and start a Uima C++ service that connects to an ActiveMQ queue broker. This class
   * initializes the process environment and starts a process to deploy the C++ service.
   * Communication via sockets is established between this Controller and the C++ service through
   * which logging, JMX and administrative messages are transmitted.
   * 
   * @param aeDescriptorFileName
   *          - UIMA analysis engine descriptor file.
   * @param queueName
   *          - input queue name
   * @param brokerURL
   *          - queue broker URL
   * @param numInstances
   *          - number of instance of AEs to start in the service.
   * @param prefetchSize
   *          - number of message to prefetch
   * @param envVarMap
   *          - enviroment variables to be set. These settings are valid only for the new process in
   *          which C++ service will run.
   * @throws UIMAException
   */
  public UimacppServiceController(String aeDescriptorFileName, String queueName, String brokerURL,
          int numInstances, int prefetchSize, Map<String, String> envVarMap,
          int processCasErrorThreshhold, int processCasErrorWindow, boolean terminateOnCPCError,
          int initialFsHeapSize) throws ResourceInitializationException {

    try {
      this.uimaLogger = UIMAFramework.getLogger(this.getClass());
      this.aeDesc = aeDescriptorFileName;
      this.numInstances = numInstances;
      this.queueName = queueName;
      this.processCasErrorThreshhold = processCasErrorThreshhold;
      this.processCasErrorWindow = processCasErrorWindow;
      this.terminateOnCPCError = terminateOnCPCError;
      this.initialFsHeapSize = initialFsHeapSize;
      this.startingDirectory = envVarMap.get(STARTING_DIRECTORY);
      this.isStopped = false;

      /* start a listener */
      server = new ServerSocket(0);
      port = server.getLocalPort();
      server.setSoTimeout(10000);

      // System.out.println("Listening on port " + port);

      /* setup the command */
      ArrayList<String> commandArgs = new ArrayList<String>();
      buildCommandArgs(commandArgs, envVarMap, "deployCppService");

      if (brokerURL != null && brokerURL.length() > 0) {
        commandArgs.add("-b");
        commandArgs.add(brokerURL);
      }

      commandArgs.add("-p");
      commandArgs.add(Integer.toString(prefetchSize));

      /* construct the process builder */
      builder = new ProcessBuilder(commandArgs);
      setEnvironmentVariables(envVarMap);

      /* setup environment variables */

      // System.out.println("Uima C++ Service " + aeDesc + " configured.");
      // System.out.println("Listener started at port " + port + ".");
      // System.out.println(builder.command().toString());
      /* start the C++ service */
      // try {
      // start threads to accept two connections from the service

      /* start the service */
      this.uimaLogger.log(Level.INFO, "Starting C++ service: " + commandArgs.toString());
      this.uimaLogger.log(Level.INFO, " env params: " + envVarMap.toString());
      startService();

      /** register with JMX - for now register with the platform MBean server */
      mbean = new UimacppServiceManagement("org.apache.uima:type=ee.jms.services,",
              commandConnection, aeDesc, numInstances, brokerURL, queueName);
      JmxMBeanAgent.registerMBean(mbean, null);

      // Initialization looks good
      notifyInitializationStatus(null);

    } catch (IOException e) {
      notifyInitializationStatus(e);
      throw new ResourceInitializationException(e);
    } catch (UIMAException e) {
      notifyInitializationStatus(e);
      throw new ResourceInitializationException(e);
    }
  }

  private void notifyInitializationStatus(Exception e) {
    synchronized (this) {
      if (!this.InitializedState) {
        this.InitializedStatus = e;
        this.InitializedState = true;
      }

      for (int i = 0; i < this.listeners.size(); i++) {
        // If there is an exception, notify listener with failure
        if (e != null) {
          (this.listeners.get(i)).notifyOnInitializationFailure(e);
        }
        // else, Success!
        else {
          (this.listeners.get(i)).notifyOnInitializationSuccess();
        }
      }
    }
  }

  public UimacppServiceController(String aeDescriptorFileName, String queueName, String brokerURL,
          int numInstances, int prefetchSize, Map<String, String> envVarMap,
          int processCasErrorThreshhold, int processCasErrorWindow, boolean terminateOnCPCError)
          throws ResourceInitializationException {

    this(aeDescriptorFileName, queueName, brokerURL, numInstances, prefetchSize, envVarMap,
            processCasErrorThreshhold, processCasErrorWindow, terminateOnCPCError, 0);

  }

  /**
   * Configure and start a Uima C++ service that connects to an WebSphereMQ queue broker. This class
   * initializes the process environment and starts a process to deploy the C++ service.
   * Communication via sockets is established between this Controller and the C++ service through
   * which logging, JMX and administrative messages are transmitted.
   * 
   * @param uimaLogger
   * @param aeDescriptorFileName
   * @param mqQueueName
   * @param mqHostName
   * @param mqPort
   * @param mqChannel
   * @param mqQueueMgr
   * @param numInstances
   * @param envVarMap
   * @param processCasErrorThreshhold
   * @param processCasErrorWindow
   * @param terminateOnCPCError
   * @param mBeanServer
   * @throws ResourceInitializationException
   */
  public UimacppServiceController(org.apache.uima.util.Logger uimaLogger,
          String aeDescriptorFileName, String queueName, String mqHostName, int mqPort,
          String mqChannel, String mqQueueMgr, int numInstances, Map<String, String> envVarMap,
          int processCasErrorThreshhold, int processCasErrorWindow, boolean terminateOnCPCError,
          JmxManagement jmxManagement, int initialFsHeapSize)
          throws ResourceInitializationException {

    try {
      this.uimaLogger = UIMAFramework.getLogger(this.getClass());
      this.aeDesc = aeDescriptorFileName;
      this.numInstances = numInstances;
      this.queueName = queueName;
      this.processCasErrorThreshhold = processCasErrorThreshhold;
      this.processCasErrorWindow = processCasErrorWindow;
      this.terminateOnCPCError = terminateOnCPCError;
      this.initialFsHeapSize = initialFsHeapSize;
      this.startingDirectory = envVarMap.get(STARTING_DIRECTORY);
      this.jmxMgmt = jmxManagement;
      this.isStopped = false;
      
      /* start a listener */
      server = new ServerSocket(0);
      port = server.getLocalPort();
      server.setSoTimeout(10000);

      // System.out.println("Listening on port " + port);
      /* setup the command */
      ArrayList<String> commandArgs = new ArrayList<String>();
      buildCommandArgs(commandArgs, envVarMap, "deployWMQCppService");

      // add MQ specific args
      if (mqHostName != null && mqHostName.length() > 0) {
        commandArgs.add("-mqh");
        commandArgs.add(mqHostName);
      }

      commandArgs.add("-mqp");
      commandArgs.add(Integer.toString(mqPort));

      if (mqChannel != null && mqChannel.length() > 0) {
        commandArgs.add("-mqc");
        commandArgs.add(mqChannel);
      }

      if (mqQueueMgr != null && mqQueueMgr.length() > 0) {
        commandArgs.add("-mqm");
        commandArgs.add(mqQueueMgr);
      }

      /* construct the process builder */
      builder = new ProcessBuilder(commandArgs);
      setEnvironmentVariables(envVarMap);

      // System.out.println("Uima C++ Service " + aeDesc + " configured.");
      // System.out.println("Listener started at port " + port + ".");
      // System.out.println(builder.command().toString());

      /* start the service */
      this.uimaLogger.log(Level.INFO, "Starting C++ service: " + commandArgs.toString());
      this.uimaLogger.log(Level.INFO, " env params: " + envVarMap.toString());
      startService();

      mbean = new UimacppServiceManagement(null, commandConnection, aeDesc, numInstances,
              mqHostName + " " + mqPort + "//" + mqQueueMgr, "queue://" + /* mqChannel + */"/"
                      + queueName);

      /** register with JMX */

      if (jmxManagement == null) {
        throw new ResourceInitializationException(new IOException("JmxManagement object is null."));
      }

      mbean = new UimacppServiceManagement(jmxManagement.getJmxDomain(), commandConnection, aeDesc,
              numInstances, mqHostName + " " + mqPort + "//" + mqQueueMgr, queueName);
      ObjectName oname = new ObjectName(mbean.getUniqueMBeanName());
      jmxManagement.registerMBean(mbean, oname);

    } catch (IOException e) {
      throw new ResourceInitializationException(e);
    } catch (UIMAException e) {
      throw new ResourceInitializationException(e);
    } catch (MalformedObjectNameException e) {
      throw new ResourceInitializationException(e);
    } catch (NullPointerException e) {
      throw new ResourceInitializationException(e);
    } catch (Exception e) {
      throw new ResourceInitializationException(e);
    }
  }

  private void buildCommandArgs(ArrayList<String> commandArgs, Map<String, String> envVarMap,
          String exeName) throws ResourceInitializationException {

    // Get the UIMACPP_HOME value and save for use by setEnvironmentVariables
    uimacppHome = (String) envVarMap.get("UIMACPP_HOME");
    if (uimacppHome == null) {
      uimacppHome = System.getenv("UIMACPP_HOME");
      if (uimacppHome == null) {
        uimacppHome = System.getenv("UIMA_HOME") + "/uimacpp";
      }
    }

    if (!(new File(uimacppHome)).exists()) {
      throw new ResourceInitializationException(new IOException("Invalid location of UIMACPP_HOME "
              + uimacppHome));
    }

    // the Uima C++ service wrapper exe
    String cmd = uimacppHome + System.getProperty("file.separator") + "bin"
            + System.getProperty("file.separator") + exeName;

    commandArgs.add(cmd);

    // arguments
    // UIMA AE descriptor
    // eclipse likes URL formats starting with file:, but C++ doesn't
    if (this.aeDesc.regionMatches(true, 0, "file:", 0, 5)) {
      this.aeDesc = this.aeDesc.substring(5);
    }
    Pattern mSlashDosDrive = Pattern.compile("/[a-zA-Z]:");
    Matcher matcher = mSlashDosDrive.matcher(this.aeDesc);
    if (matcher.find(0)) {
      // "/c:" doesn't work either
      this.aeDesc = this.aeDesc.substring(1);
    }

    commandArgs.add(aeDesc);
    if (!(new File(aeDesc)).exists()) {
      throw new ResourceInitializationException(new IOException(
              "Invalid location of AE descriptor " + aeDesc));
    }

    // input queue name
    commandArgs.add(queueName);

    // port this server is listening at
    commandArgs.add("-jport");
    commandArgs.add(Integer.toString(port));

    // number of instances of consumers the
    // service should start
    commandArgs.add("-n");
    if (numInstances < 1) {
      numInstances = 1;
    }
    commandArgs.add(Integer.toString(numInstances));

    // logging level setting obtained from the UIMA framework
    // translated to UIMA C++ logging levels.
    commandArgs.add("-l");
    if (uimaLogger.isLoggable(Level.FINE) || uimaLogger.isLoggable(Level.CONFIG)
            || uimaLogger.isLoggable(Level.FINER) || uimaLogger.isLoggable(Level.FINEST)
            || uimaLogger.isLoggable(Level.INFO)) {
      commandArgs.add(Integer.toString(0));
    } else if (uimaLogger.isLoggable(Level.WARNING)) {
      commandArgs.add(Integer.toString(1));
    } else if (uimaLogger.isLoggable(Level.SEVERE)) {
      commandArgs.add(Integer.toString(2));
    } else {
      commandArgs.add(Integer.toString(-1));
    }

    // translate logger level to trace level 0-4.
    // set based Level = CONFIG, INFO, FINE, FINER, FINEST...
    commandArgs.add("-t");
    if (uimaLogger.isLoggable(Level.FINEST)) {
      commandArgs.add(Integer.toString(3));
    } else if (uimaLogger.isLoggable(Level.FINER)) {
      commandArgs.add(Integer.toString(2));
    } else if (uimaLogger.isLoggable(Level.FINE)) {
      commandArgs.add(Integer.toString(1));
    } else if (uimaLogger.isLoggable(Level.CONFIG) || uimaLogger.isLoggable(Level.INFO)) {
      commandArgs.add(Integer.toString(0));
    } else {
      commandArgs.add(Integer.toString(-1));
    }
    // data directory used to resolve location of
    // files used by annotator.
    String uimacppDataPath = (String) envVarMap.get("UIMACPP_DATAPATH");
    if (uimacppDataPath != null && uimacppDataPath.length() != 0) {
      commandArgs.add("-d");
      commandArgs.add(uimacppDataPath);
    }

    if (processCasErrorThreshhold > 0) {
      commandArgs.add("-e");
      commandArgs.add(Integer.toString(processCasErrorThreshhold));
      if (processCasErrorWindow > 0) {
        commandArgs.add("-w");
        commandArgs.add(Integer.toString(processCasErrorWindow));
      }
    }

    if (terminateOnCPCError) {
      commandArgs.add("-a");
      commandArgs.add("true");
    }

    if (initialFsHeapSize > 0) {
      commandArgs.add("-fsheapsz");
      commandArgs.add(Integer.toString(initialFsHeapSize));
    }

  }

  public UimacppServiceController(org.apache.uima.util.Logger uimaLogger,
          String aeDescriptorFileName, String queueName, String mqHostName, int mqPort,
          String mqChannel, String mqQueueMgr, int numInstances, Map<String, String> envVarMap,
          int processCasErrorThreshhold, int processCasErrorWindow, boolean terminateOnCPCError,
          JmxManagement jmxManagement) throws ResourceInitializationException {

    this(uimaLogger, aeDescriptorFileName, queueName, mqHostName, mqPort, mqChannel, mqQueueMgr,
            numInstances, envVarMap, processCasErrorThreshhold, processCasErrorWindow,
            terminateOnCPCError, jmxManagement, 0);

  }

  private void setEnvironmentVariables(Map<String, String> envVarMap) {

    // In case the standard UIMA launching scripts were not used, add the uimacpp library to
    // the appropriate path variables
    // Since Windows is case-insensitive but the ProcessBuilder environment map is not,
    // must search for the actual PATH key that is in the environment.
    String pathKey = "PATH";    // Other OS's have a sensible value!
    Map<String, String> environment = builder.environment();
    if (System.getProperty("os.name").startsWith("Windows")) {
      for ( String key : environment.keySet()) {
        if (key.equalsIgnoreCase("PATH")) {
          pathKey = key;
          break;
        }
      }
      // Windows DLLs are in the bin directory
      String uimacppLibDir = uimacppHome + System.getProperty("file.separator") + "bin";
      String value = environment.get(pathKey);
      if (value != null && value.length() > 0) {
        value = uimacppLibDir + System.getProperty("path.separator") + value;
      } else {
        value = uimacppLibDir;
      }
      environment.put(pathKey, value);
      
    } else {
      // Add the uimacpp lib directory to the Linux & Mac library paths
      String uimacppLibDir = uimacppHome + System.getProperty("file.separator") + "lib";
      String value = environment.get("LD_LIBRARY_PATH");
      if (value != null && value.length() > 0) {
        value = uimacppLibDir + System.getProperty("path.separator") + value;
      } else {
        value = uimacppLibDir;
      }
      environment.put("LD_LIBRARY_PATH", value);

      value = environment.get("DYLD_LIBRARY_PATH");
      if (value != null && value.length() > 0) {
        value = uimacppLibDir + System.getProperty("path.separator") + value;
      } else {
        value = uimacppLibDir;
      }
      environment.put("DYLD_LIBRARY_PATH", value);
    }

    // Copy user specified environment variables from the deployment descriptor
    for ( Entry<String,String> entry : envVarMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      if (value != null && value.length() > 0) {
        // special handling for PATH and LD_LIBRARY_PATH and DYLD_LIBRARY_PATH
        // for these we prepend the values to the existing values.
        // For backward compatibility accept any casing for the Windows PATH
        if (key.equalsIgnoreCase("PATH") || key.equals("LD_LIBRARY_PATH") || key.equals("DYLD_LIBRARY_PATH")) {
          if (key.equalsIgnoreCase("PATH")) {
            key = pathKey;
          }
          String origValue = environment.get(key);
          if (origValue != null) {
            value = value + System.getProperty("path.separator") + origValue;
          }
        }
        environment.put(key, value);
      }
    }

  }

  private void startService() throws UIMAException {
    /* start the C++ service */
    try {
      // start threads to accept two connections from the service
      ConnectionHandler handler1 = new ConnectionHandler(this);
      Thread t1 = new Thread(handler1);
      t1.start();
      ConnectionHandler handler2 = new ConnectionHandler(this);
      Thread t2 = new Thread(handler2);
      t2.start();

      // setup starting directory if specified.
      if (this.startingDirectory != null && this.startingDirectory.length() > 0) {
        File startingDir = new File(this.startingDirectory);
        if (!startingDir.exists()) {
          throw new ResourceInitializationException(new IOException(this.startingDirectory
                  + " Uimacpp Starting Directory not found. + "));
        }
        builder.directory(startingDir);
      }
      /* add the shutdown hook */
      shutdownHook = new UimacppShutdownHook(this, commandConnection, uimaLogger);
      Runtime.getRuntime().addShutdownHook(shutdownHook);
      
      /* fork the C++ process */
      uimacppProcess = builder.start();
      
      if (uimacppProcess == null) {
        throw new UIMAException(new Throwable("Could not fork process."));
      }

      stdoutHandler = new StdoutHandler(uimacppProcess, uimaLogger);
      Thread t3 = new Thread(stdoutHandler);
      t3.start();

      stderrHandler = new StderrHandler(uimacppProcess, uimaLogger);
      Thread t4 = new Thread(stderrHandler);
      t4.start();

      // wait for connection handler threads to complete
      t1.join();
      t2.join();
      synchronized (this) {
        if (this.loggerConnection == null || this.loggerHandler == null
                || this.commandConnection == null) {
          throw new ResourceInitializationException(new IOException(
                  "Could not establish socket connection with C++ service."));
        }
      }

      if (uimacppProcess != null) {
        // wait for C++ process to report initialization status.
        System.out.println("Waiting for Uima C++ service to report init status...");
        BufferedReader in = new BufferedReader(new InputStreamReader(commandConnection
                .getInputStream()));

        StringBuffer sb = new StringBuffer();
        int c = in.read();
        while (c >= 0) {
          sb.append((char) c);
          c = in.read();
          if (c == '\n') {
            break;
          }
        }
        if (sb.toString().equalsIgnoreCase("0")) {
          System.out.println("Uima C++ service at " + queueName + " Ready to process...");
          WaitThread wt = new WaitThread(uimacppProcess, uimaLogger, this);
          Thread wThread = new Thread(wt);
          wThread.start();
        } else {
          System.out.println("UIMA C++ service at " + queueName + " failed to initialize.");
          System.out.println(sb.toString());
          uimacppProcess.destroy();
          throw new IOException(sb.toString());
        }
      } else {
        throw new ResourceInitializationException(new IOException(
                "Could not start the C++ service."));
      }
    } catch (IOException e) {
      throw new ResourceInitializationException(e);
    } catch (InterruptedException e) {
      throw new ResourceInitializationException(e);
    }

  }

  /**
   * Shuts down the UIMA C++ service process.
   * 
   * @param force
   *          - force or allow service to shutdown gracefully.
   * @throws IOException
   * @throws InterruptedException
   */
  public void shutdown() throws IOException, InterruptedException {
    mbean.shutdown();
    if (jmxMgmt != null) {
      try {
        this.jmxMgmt.destroy();
      } catch (Exception e) {
        throw new IOException(e.getMessage());
      }
    }
    if (listeners != null) {
      for (int i = 0; i < listeners.size(); i++) {
        ControllerCallbackListener listener = (ControllerCallbackListener) listeners.get(i);
        if (listener != null) {
          listener.notifyOnTermination("Uima C++ service shutdown.", EventTrigger.ExceededErrorThreshold);
        }
      }
      listeners.clear();
    }
    synchronized (this) {
      loggerConnection.close();
    }
    commandConnection.close();
    server.close();
    this.isStopped = true;
  }

  public String getStatistics() throws IOException {
    return mbean.getStatisticsAsString();
  }

  public void resetStatistics() throws IOException {
    mbean.resetStats();
  }

  synchronized protected void handleConnection(Socket inSock) throws IOException {
    if (this.loggerConnection == null) {
      this.loggerConnection = inSock;

      loggerHandler = new LoggerHandler(loggerConnection, uimaLogger);
      (new Thread(loggerHandler)).start();
      // System.out.println("Accepted logger connection.");
    } else {
      this.commandConnection = inSock;
      // System.out.println("Accepted commands connection.");
    }
  }

  /**
   * test
   * 
   * @param args
   */
  public static void main(String[] args) {
    HashMap<String, String> envVarMap = new HashMap<String, String>();

    try {
      if (System.getProperty("os.name").startsWith("Windows")) {
        envVarMap.put("UIMACPP_HOME", "c:\\uimacpp2.0\\uimacpp");
        envVarMap.put("UIMACPP_LOGFILE", "c:\\temp\\uimacppcontroller.log");
        envVarMap.put("Path", "c:\\cppExamples2.0\\src");

        UimacppServiceController controller = new UimacppServiceController(
                "c:/cppExamples2.0/descriptors/DaveDetector.xml", // AE descriptor
                "davedetector", // input queue
                "tcp://localhost:61616", // activemq broker url
                1, // num instances
                0, // prefetch,
                envVarMap, 0, // processCAS error threshhold
                0, // processCAS error window
                false, 2000000);
      } else {
        envVarMap.put("UIMACPP_HOME", "/opt/IBM/uimacpp");
        // envVarMap.put("Path", "/opt/IBM/uimacpp/bin");
        envVarMap.put("UIMACPP_LOGFILE", "/tmp/bhavani.log");
        // envVarMap.put("LD_LIBRARY_PATH", "/opt/IBM/uimacpp/lib:/opt/IBM/uimacpp/lib/xms");
        UimacppServiceController controller = new UimacppServiceController(UIMAFramework
                .getLogger(), "/home/bsiyer/cppExamples/descriptors/DaveDetector.xml",
                "ORANGE.QUEUE", "sith07.watson.ibm.com", 1414, null, null, 1, envVarMap, 0, 0,
                false, null, 0);
      }

      /**
       * try { Thread.sleep(2000); } catch (InterruptedException e) { // TODO Auto-generated catch
       * block e.printStackTrace(); }
       * 
       * System.out.println("MAIN getStats"); for (int i = 0; i < 3; i++) { String stats =
       * controller.getStatistics(); if (stats == null) System.out.println("NULL stats"); else
       * System.out.println(i + " " + stats); }
       * 
       * System.out.println("MAIN RESET STATS"); controller.resetStatistics(); String stats =
       * controller.getStatistics(); if (stats == null) System.out.println("NULL stats"); else
       * System.out.println("AFTER RESET " + stats);
       * 
       * try { controller.shutdown(); } catch (InterruptedException e1) { e1.printStackTrace(); }
       * catch (IOException e) { // TODO Auto-generated catch block e.printStackTrace(); }
       **/

    } catch (ResourceInitializationException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, UimacppServiceController.class.getName(),
                "main", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }

  }

  /**
   * 
   * Runs when UIMA EE client API undeploys this service.
   * 
   */
  public void terminate() {
    try {
      shutdown();
    } catch (IOException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "terminate", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    } catch (InterruptedException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "terminate", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }
  }

  /**
   * 
   * Runs when spring undeploys this bean.
   * 
   */
  public void destroy() {
    try {
      shutdown();
    } catch (IOException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "destroy", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    } catch (InterruptedException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "destroy", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }
  }

  public void addControllerCallbackListener(ControllerCallbackListener aListener) {
    synchronized (this) {
      this.listeners.add(aListener);
      // if already initialized, notify now
      if (this.InitializedState) {
        if (this.InitializedStatus == null) {
          aListener.notifyOnInitializationSuccess();
        } else {
          aListener.notifyOnInitializationFailure(this.InitializedStatus);
        }
      }
    }
  }

  public void removeControllerCallbackListener(ControllerCallbackListener aListener) {
    this.listeners.remove(aListener);
  }

  public void quiesceAndStop() {
		try {
		  String msg = this.mbean.quiesceAndStop();
		  //System.out.println("UimacppServiceController service reports QuiesceAndStop " + msg);
		  this.uimaLogger.log(Level.INFO, "Service reports QuiesceAndStop " + msg );
		} catch (IOException e ) {
		  this.uimaLogger.log(Level.SEVERE, e.getMessage());
		}
  }

  public boolean isStopped() {
    return this.isStopped;
  }
  
  public void setStopped() {
    this.isStopped = true;
  }
  
  public ArrayList<ControllerCallbackListener> getCallbackListeners() {
	  return this.listeners;
  }

  public String getKey() {
    return "";
  }
  public void dumpState(StringBuffer buffer, String lbl1) {
    // TODO Auto-generated method stub
    
  }

  public String getPID() {
    // TODO Auto-generated method stub
    return null;
  }
}

/**
 * Handles C++ service logging requests. It receives messages sent through the logger socket
 * connection and writes it to the UIMA logger.
 * 
 * 
 */
class LoggerHandler implements Runnable {
  Socket socket;

  BufferedReader in;

  org.apache.uima.util.Logger logger;

  public LoggerHandler(Socket sock, org.apache.uima.util.Logger uimaLogger) throws IOException {

    socket = sock;
    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    logger = uimaLogger;
  }

  public void logMessage(String logMessage) {
    // determine the logging level of the message
    // translate uimacpp log level to java logger level
    Level level = Level.INFO; // default

    if (logMessage.startsWith("0")) {
      level = Level.INFO;
    } else if (logMessage.startsWith("1")) {
      level = Level.WARNING;
    } else if (logMessage.startsWith("2")) {
      level = Level.SEVERE;
    }

    logMessage = logMessage.trim();
    // log the message
    logger.log(level, logMessage);
    // System.out.println(logMessage);
  }

  public void run() {
    try {
      StringBuilder sb = new StringBuilder();

      int c = in.read();
      while (c >= 0) {
        sb.append((char) c);
        if (c == '\n') {
          logMessage(sb.toString());
          sb.delete(0, sb.length());
        }
        c = in.read();
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, e.getMessage());
    }
  }
} // log handler

/**
 * accept socket connection from C++ service.
 */
class ConnectionHandler implements Runnable {
  private static final Class CLASS_NAME = ConnectionHandler.class;

  UimacppServiceController controller;

  ConnectionHandler(UimacppServiceController controller) {
    this.controller = controller;
  }

  public void run() {

    Socket aSocket;
    try {
      System.out.println("Waiting for Uima C++ service to connect...");
      aSocket = controller.server.accept();
      controller.handleConnection(aSocket);
    } catch (SocketTimeoutException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    } catch (IOException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }

  }

}

/**
 * Logs messages written to stdout by the C++ service.
 * 
 * 
 */
class StdoutHandler implements Runnable {

  BufferedReader in;

  org.apache.uima.util.Logger logger;

  public StdoutHandler(Process aprocess, org.apache.uima.util.Logger uimaLogger) throws IOException {

    in = new BufferedReader(new InputStreamReader(aprocess.getInputStream()));
    logger = uimaLogger;
  }

  public void run() {
    try {
      StringBuilder sb = new StringBuilder();

      int c = in.read();
      while (c >= 0) {
        sb.append((char) c);
        if (c == '\n') {
          logger.log(Level.INFO, sb.toString());
          sb.delete(0, sb.length());
        }
        c = in.read();
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, e.getMessage());
    }

  }
} // stdoutHandler

/**
 * Logs messages written to stderr by the C++ service.
 * 
 * 
 */
class StderrHandler implements Runnable {

  BufferedReader in;

  org.apache.uima.util.Logger logger;

  public StderrHandler(Process aprocess, org.apache.uima.util.Logger uimaLogger) throws IOException {

    in = new BufferedReader(new InputStreamReader(aprocess.getErrorStream()));
    logger = uimaLogger;
  }

  public void run() {
    try {
      StringBuilder sb = new StringBuilder();

      int c = in.read();
      while (c >= 0) {
        sb.append((char) c);
        if (c == '\n') {
          System.out.println(sb.toString());
          logger.log(Level.INFO, sb.toString());
          sb.delete(0, sb.length());
        }
        c = in.read();
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, e.getMessage());
    }
  }
} // stderrHandler

/**
 * 
 * Waits on C++ process.
 * 
 */
class WaitThread implements Runnable {

  public Process uimacppProcess;

  private org.apache.uima.util.Logger uimaLogger;

  private List<ControllerCallbackListener> listeners;
  
  private UimacppServiceController controller;

  public WaitThread(Process aprocess, org.apache.uima.util.Logger logger,
         // List<ControllerCallbackListener> llist) throws IOException {
	      UimacppServiceController cntlr) throws IOException {
    this.uimacppProcess = aprocess;
    this.uimaLogger = logger;
    this.controller = cntlr;
    this.listeners = this.controller.getCallbackListeners();
  }

  public void run() {
    int rc;
    String message = "WaitThread calling UIMA C++ service shutdown.";
    try {
      rc = uimacppProcess.waitFor();
      message += "rc=" + rc;
      controller.setStopped();
      if (listeners != null) {
        for (int i = 0; i < listeners.size(); i++) {
          ControllerCallbackListener listener = (ControllerCallbackListener) listeners.get(i);
          if (listener != null) {
            listener.notifyOnTermination(message, EventTrigger.ExceededErrorThreshold);
          }
        }
        listeners.clear();
      }
    } catch (InterruptedException e) {
      this.uimaLogger.log(Level.INFO, e.getMessage());
      message += e.getMessage();
      if (listeners != null) {
        for (int i = 0; i < listeners.size(); i++) {
          ControllerCallbackListener listener = (ControllerCallbackListener) listeners.get(i);
          if (listener != null) {
            listener.notifyOnTermination(message, EventTrigger.ExceededErrorThreshold);
          }
        }
        listeners.clear();
      }
    }

  }
} // WaitThread

/**
 * 
 * Runs when JVM termintes.
 * 
 */
class UimacppShutdownHook extends Thread {

  public UimacppServiceController controller;
  
  private org.apache.uima.util.Logger uimaLogger;
  

  public UimacppShutdownHook(UimacppServiceController controller, Socket socket, org.apache.uima.util.Logger logger)
          throws IOException {
    this.controller = controller;
    this.uimaLogger = logger;
  }

  public void run() {
    uimaLogger.log(Level.INFO, "UimacppShutdownHook sending shutdown message.");
    if (controller.mbean != null)
		try {
			System.out.println("UimacppShutdownHook sending quiesce message");
			controller.mbean.quiesceAndShutdown();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
    if (controller.uimacppProcess != null) {
      System.out.println("UimacppShutdownHook destroy C++ process");
      controller.uimacppProcess.destroy();
    }
  }
} // shutdownhook
