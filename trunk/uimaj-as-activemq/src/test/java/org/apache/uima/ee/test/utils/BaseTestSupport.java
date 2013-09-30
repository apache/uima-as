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

package org.apache.uima.ee.test.utils;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import javax.jms.Message;

import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaASProcessStatusImpl;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.error.UimaASPingTimeout;
import org.apache.uima.aae.error.UimaASProcessCasTimeout;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.jms.error.handler.BrokerConnectionException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.ProcessTraceEvent;
import org.apache.uima.util.impl.ProcessTrace_impl;

public abstract class BaseTestSupport extends ActiveMQSupport
// implements UimaASStatusCallbackListener
{
  private static final char FS = System.getProperty("file.separator").charAt(0);

  protected String text = "IBM today elevated five employees to the title of IBM Fellow\n -- its most prestigious technical honor.\n The company also presented more than $2.8 million in cash awards to employees whose technical innovation have yielded exceptional value to the company and its customers.\nIBM conferred the accolades and awards at its 2003 Corporate Technical Recognition Event (CTRE) in Scottsdale, Ariz. CTRE is a 40-year tradition at IBM, established to recognize exceptional technical employees and reward them for extraordinary achievements and contributions to the company's technology leadership.\n Our technical employees are among the best and brightest innovators in the world.\n They share a passion for excellence that defines their work and permeates the products and services IBM delivers to its customers, said Nick Donofrio, senior vice president, technology and manufacturing for IBM.\n CTRE provides the means for us to honor those who have distinguished themselves as exceptional leaders among their peers.\nAmong the special honorees at the 2003 CTRE are five employees who earned the coveted distinction of IBM Fellow:- David Ferrucci aka Dave, Grady Booch, chief scientist of Rational Software, IBM Software Group.\n Recognized internationally for his innovative work on software architecture, modeling, and software engineering process. \nMr. Booch is one of the original authors of the Unified Modeling Language (UML), the industry-standard language of blueprints for software-intensive systems.- Dr. Donald Chamberlin, researcher, IBM Almaden Research Center. An expert in relational database languages, Dr. Chamberlin is co- inventor of SQL, the language that energized the relational database market. He has also";

  protected String doubleByteText = null;

  protected volatile boolean unexpectedException = false;

  private static final boolean SEND_CAS_ASYNCHRONOUSLY = true;

  protected static final int CPC_LATCH = 1;

  protected static final int EXCEPTION_LATCH = 2;

  protected static final int PROCESS_LATCH = 3;

  protected CountDownLatch processCountLatch = null;

  protected CountDownLatch cpcLatch = null;

  protected CountDownLatch exceptionCountLatch = null;

  protected boolean initialized = false;

  protected Object initializeMonitor = new Object();

  protected boolean isStopped = false;

  protected boolean isStopping = false;

  protected long responseCounter = 0;

  protected long expectedProcessTime = 0;

  boolean serviceShutdownException = false;

  List<Class> exceptionsToIgnore = new ArrayList<Class>();

  private int timeoutCounter = 0;

  private Object errorCounterMonitor = new Object();

  private BaseUIMAAsynchronousEngine_impl engine;

  protected UimaAsTestCallbackListener listener = new UimaAsTestCallbackListener();

  protected boolean receivedExpectedParentReferenceId = false;

  protected int maxPingRetryCount = 4;
  
  protected volatile boolean countPingRetries = false;

  protected long failedCasCountDueToBrokerFailure = 0;
  
  protected String deployService(BaseUIMAAsynchronousEngine_impl eeUimaEngine,
          String aDeploymentDescriptorPath) throws Exception {
    String defaultBrokerURL = System.getProperty("BrokerURL");
    if (defaultBrokerURL != null) {
      System.out.println(">>> runTest: Setting defaultBrokerURL to:" + defaultBrokerURL);
      System.setProperty("defaultBrokerURL", defaultBrokerURL);
    } else {
      System.setProperty("defaultBrokerURL", "tcp://localhost:8118");
    }

    Map<String, Object> appCtx = new HashMap();
    appCtx.put(UimaAsynchronousEngine.DD2SpringXsltFilePath,
            "../src/main/scripts/dd2spring.xsl".replace('/', FS));
    appCtx.put(UimaAsynchronousEngine.SaxonClasspath,
            "file:../src/main/saxon/saxon8.jar".replace('/', FS));
    // appCtx.put(UimaAsynchronousEngine.UimaEeDebug, UimaAsynchronousEngine.UimaEeDebug);
    String containerId = null;
    try {
      containerId = eeUimaEngine.deploy(aDeploymentDescriptorPath, appCtx);
    } catch (ResourceInitializationException e) {
      if (!ignoreException(ResourceInitializationException.class)) {
        System.out
                .println(">>>>>>>>>>> runTest: Stopping Client API Due To Initialization Exception");
        isStopping = true;
        eeUimaEngine.stop();
        throw e;
      }
      System.out.println(">>>>>>>>>>> Exception ---:" + e.getClass().getName());
    } catch (Exception e) {
      System.out.println(">>>>>>>>>>> runTest: Exception:" + e.getClass().getName());
      throw e;
    }
    return containerId;
  }

  protected void addExceptionToignore(Class anExceptionToIgnore) {
    exceptionsToIgnore.add(anExceptionToIgnore);
  }

  protected boolean ignoreException(Class<?> anException) {
    if (anException == null) {
      return true;
    }
    for (int i = 0; i < exceptionsToIgnore.size(); i++) {
      String name = anException.getName();
      if (name.equals(exceptionsToIgnore.get(i).getName())) {
        System.out.println("!!! runTest ignoring exception: " + name);
        return true;
      }
    }
    return false;
  }

  public void initialize(BaseUIMAAsynchronousEngine_impl eeUimaEngine, Map<String, Object> appCtx)
          throws Exception {
    eeUimaEngine.addStatusCallbackListener(listener);
    eeUimaEngine.initialize(appCtx);
  }

  protected void setDoubleByteText(String aDoubleByteText) {
    doubleByteText = aDoubleByteText;
  }

  protected void setExpectedProcessTime(long expectedTimeToProcess) {
    expectedProcessTime = expectedTimeToProcess;
  }

  protected String getFilepathFromClassloader(String aFilename) throws Exception {
    URL url = this.getClass().getClassLoader().getResource(aFilename);
    return (url == null ? null : url.getPath());
  }

  protected Map<String, Object> buildContext(String aTopLevelServiceBrokerURI, String aTopLevelServiceQueueName)
          throws Exception {
    return buildContext(aTopLevelServiceBrokerURI, aTopLevelServiceQueueName, 0);
  }

  protected Map<String, Object> buildContext(String aTopLevelServiceBrokerURI, String aTopLevelServiceQueueName,
          int timeout) throws Exception {
    Map<String, Object> appCtx = new HashMap<String, Object>();
    appCtx.put(UimaAsynchronousEngine.ServerUri, aTopLevelServiceBrokerURI);
    appCtx.put(UimaAsynchronousEngine.ENDPOINT, aTopLevelServiceQueueName);
    appCtx.put(UimaAsynchronousEngine.CasPoolSize, Integer.valueOf(4));
    appCtx.put(UimaAsynchronousEngine.ReplyWindow, 15);
    appCtx.put(UimaAsynchronousEngine.Timeout, timeout);
    return appCtx;
  }

  protected boolean isMetaRequest(Message aMessage) throws Exception {
    int messageType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
    int command = aMessage.getIntProperty(AsynchAEMessage.Command);
    return (AsynchAEMessage.Request == messageType && AsynchAEMessage.GetMeta == command);
  }

  protected Thread spinMonitorThread(final Semaphore ctrlSemaphore, int howMany,
          final int aLatchKind) throws Exception {
    final String name;

    switch (aLatchKind) {
      case CPC_LATCH:
        // Initialize latch to open after CPC reply comes in.
        cpcLatch = new CountDownLatch(howMany);
        name = "CpcLatch";
        break;

      case EXCEPTION_LATCH:
        // Initialize latch to open after expected Exceptions comes in.
        exceptionCountLatch = new CountDownLatch(howMany);
        name = "ExceptionLatch";
        break;

      case PROCESS_LATCH:
        // Initialize latch to open after all CASes returned.
        System.out.println("runTest: Initializing Process Latch. Number of CASes Expected:"
                + howMany);
        processCountLatch = new CountDownLatch(howMany);
        name = "ProcessLatch";
        break;

      default:
        // Avoid "may not have been initialized" warning
        name = null;
    }

    // Spin a thread that waits for the latch to open. The latch will open
    // only when
    // a CPC reply comes in
    Thread t = new Thread(name) {
      public void run() {
        try {
          // Signal the parent thread that it is ok to send CASes.
          // This is needed
          // so that the CASes are send out when the count down latch
          // is ready.
          ctrlSemaphore.release();
          // Wait until the count down latch = 0
          switch (aLatchKind) {
            case CPC_LATCH:
              // Initialize latch to open after CPC reply comes in.
              cpcLatch.await();
              break;

            case EXCEPTION_LATCH:
              // Initialize latch to open after Exception reply comes in.
              exceptionCountLatch.await();
              break;

            case PROCESS_LATCH:
              // Initialize latch to open after Process reply comes in.
              processCountLatch.await();
              break;
          }
        } catch (InterruptedException e) {

        }
      }
    };
    t.start();
    return t;
  }

  protected void waitUntilInitialized() throws Exception {
    synchronized (initializeMonitor) {
      while (!initialized) {
        initializeMonitor.wait();
      }
    }
  }

  protected void waitOnMonitor(final Semaphore ctrlSemaphore ) throws Exception {
    // Wait until the count down latch thread is ready
   try {
     ctrlSemaphore.acquire();
   } catch( InterruptedException e) {
   }
  }

  protected void runTestWithMultipleThreads(String serviceDeplyDescriptor, String queueName,
          int howManyCASesPerRunningThread, int howManyRunningThreads, int timeout,
          int aGetMetaTimeout) throws Exception {
    runTestWithMultipleThreads(serviceDeplyDescriptor, queueName, howManyCASesPerRunningThread,
            howManyRunningThreads, timeout, aGetMetaTimeout, false);
  }

  protected void runTestWithMultipleThreads(String serviceDeplyDescriptor, String queueName,
          int howManyCASesPerRunningThread, int howManyRunningThreads, int timeout,
          int aGetMetaTimeout, boolean failOnTimeout ) throws Exception {
    // Instantiate Uima EE Client
    isStopped = false;
    isStopping = false;
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima EE Primitive Service
    final String containerId = deployService(eeUimaEngine, serviceDeplyDescriptor);

    engine = eeUimaEngine;

    Thread t1 = null;
    Thread t2 = null;
    Map appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()), queueName, timeout);
    // Set an explicit getMeta (Ping)timeout
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, aGetMetaTimeout);
    appCtx.put(UimaAsynchronousEngine.Timeout, 1000);

    initialize(eeUimaEngine, appCtx);

    // Wait until the top level service returns its metadata
    waitUntilInitialized();
    final Semaphore ctrlSemaphore = new Semaphore(1);
    t2 = spinMonitorThread(ctrlSemaphore, howManyCASesPerRunningThread * howManyRunningThreads,
            PROCESS_LATCH);
    // Wait until the CPC Thread is ready.
    waitOnMonitor(ctrlSemaphore);

    if (failOnTimeout) {
      // Spin a thread and wait for awhile before killing the remote service.
      // This will cause the client to timeout waiting for a CAS reply and
      // to send a Ping message to test service availability. The Ping times
      // out and causes the client API to stop.
      new Thread("WaitThenUndeploy") {
        public void run() {
          Object mux = new Object();
          synchronized (mux) {
            try {
              mux.wait(5000);
              // Undeploy service container
              eeUimaEngine.undeploy(containerId);
            } catch (Exception e) {
            }
          }
        }
      }.start();

    }

    // Spin runner threads and start sending CASes
    for (int i = 0; i < howManyRunningThreads; i++) {
      SynchRunner runner = new SynchRunner(eeUimaEngine, howManyCASesPerRunningThread, listener);
      Thread runnerThread = new Thread(runner, "Runner" + i);
      runnerThread.start();
      System.out.println("runTest: Started Runner Thread::Id=" + runnerThread.getId());

    }
    // Wait until ALL CASes return from the service
    t2.join();
    t1 = spinMonitorThread(ctrlSemaphore, 1, CPC_LATCH);

    if (!isStopped && !unexpectedException) {
      System.out.println("runTest: Sending CPC");
      // Send CPC
      eeUimaEngine.collectionProcessingComplete();
    }

    // If have skipped CPC trip the latch
    if (unexpectedException && cpcLatch != null) {
      cpcLatch.countDown();
    }
    t1.join();
    isStopping = true;
    eeUimaEngine.stop();
  }

  protected void runCrTest(BaseUIMAAsynchronousEngine_impl aUimaEeEngine, int howMany)
          throws Exception {
    engine = aUimaEeEngine;
    final Semaphore ctrlSemaphore = new Semaphore(1);
    spinMonitorThread(ctrlSemaphore, howMany, PROCESS_LATCH);
    aUimaEeEngine.process();
    waitOnMonitor(ctrlSemaphore);
  }

  protected void runTest(Map appCtx, BaseUIMAAsynchronousEngine_impl aUimaEeEngine,
          String aBrokerURI, String aTopLevelServiceQueueName, int howMany, int aLatchKind)
          throws Exception {
    runTest(appCtx, aUimaEeEngine, aBrokerURI, aTopLevelServiceQueueName, howMany, aLatchKind,
            SEND_CAS_ASYNCHRONOUSLY);
  }

  protected void runTest2(Map appCtx, BaseUIMAAsynchronousEngine_impl aUimaEeEngine,
          String aBrokerURI, String aTopLevelServiceQueueName, int howMany, int aLatchKind)
          throws Exception {
    runTest2(appCtx, aUimaEeEngine, aBrokerURI, aTopLevelServiceQueueName, howMany, aLatchKind,
            SEND_CAS_ASYNCHRONOUSLY);
  }

  /**
   * Initializes a given instance of the Uima EE client and executes a test. It uses synchronization
   * to enforce correct sequence of calls and setups expected result.
   * 
   * @param appCtx
   * @param aUimaEeEngine
   * @param aBrokerURI
   * @param aTopLevelServiceQueueName
   * @param howMany
   * @param aLatchKind
   * @param sendCasAsynchronously
   * @throws Exception
   */
  protected void runTest(Map appCtx, BaseUIMAAsynchronousEngine_impl aUimaEeEngine,
          String aBrokerURI, String aTopLevelServiceQueueName, int howMany, int aLatchKind,
          boolean sendCasAsynchronously) throws Exception {
    Thread t1 = null;
    Thread t2 = null;
    serviceShutdownException = false;
    unexpectedException = false;
    engine = aUimaEeEngine;
    isStopped = false;
    isStopping = false;

    if (appCtx == null) {
      appCtx = buildContext(aBrokerURI, aTopLevelServiceQueueName, 0);
    }
    try {
      initialize(aUimaEeEngine, appCtx);
    } catch (ResourceInitializationException e) {
      if (ignoreException(ResourceInitializationException.class)) {
        return;
      } else {
        throw e;
      }
    } catch (Exception e) {
      throw e;
    }

    // Wait until the top level service returns its metadata
    waitUntilInitialized();
    if (howMany > 0) {
      Semaphore ctrlSemaphore = null;
      // Create a thread that will block until an exception is returned,
      // or 2 threads that wait for 'howMany' CASes and then a CPC reply
      if (aLatchKind == EXCEPTION_LATCH) {
        ctrlSemaphore = new Semaphore(1);
        t1 = spinMonitorThread(ctrlSemaphore, 1, EXCEPTION_LATCH);
      } else {
        ctrlSemaphore = new Semaphore(2);
        t1 = spinMonitorThread(ctrlSemaphore, 1, CPC_LATCH);
        t2 = spinMonitorThread(ctrlSemaphore, howMany, PROCESS_LATCH);
      }

      if (!isStopped) {
        // Wait until the monitor thread(s) start.
        waitOnMonitor(ctrlSemaphore);

        long startTime = System.currentTimeMillis();
        if (!isStopped) {
          // Send an in CAS to the top level service
          try {
            sendCAS(aUimaEeEngine, howMany, sendCasAsynchronously);
          } catch( Exception e) {}
        }
        // Wait until ALL CASes return from the service
        if (t2 != null) {
          t2.join();
          long endTime = System.currentTimeMillis();
          System.out.println(">>>> Total Time in Service:" + (endTime - startTime));
          if (!serviceShutdownException && !isStopped && !unexpectedException) {
            System.out.println("runTest: Sending CPC");
            aUimaEeEngine.collectionProcessingComplete();
          } else {
            System.out
                    .println(">>>>>>>>>>>>>>>> runTest: Not Sending CPC Due To Exception [serviceShutdownException="
                            + serviceShutdownException
                            + "] [isStopped="
                            + isStopped
                            + "] [unexpectedException=" + unexpectedException + "]");
          }
        }

        // If have skipped CPC trip the latch
        if ((serviceShutdownException || unexpectedException) && cpcLatch != null) {
          cpcLatch.countDown();
        }
        t1.join();
      }

    }
    /*
    Object o = new Object();
    
    try {
      synchronized(o) {
        o.wait();
      }
    } catch( Exception e) {
      
    }
*/
    isStopping = true;
    aUimaEeEngine.stop();

    // Finally fail test if unhappy ... must be last call as acts like "throw"
    if (unexpectedException) {
      fail("Unexpected exception returned");
    }
  }

  /**
   * Initializes a given instance of the Uima EE client and executes a test. It uses synchronization
   * to enforce correct sequence of calls and setups expected result.
   * 
   * @param appCtx
   * @param aUimaEeEngine
   * @param aBrokerURI
   * @param aTopLevelServiceQueueName
   * @param howMany
   * @param aLatchKind
   * @param sendCasAsynchronously
   * @throws Exception
   */
  protected void runTest2(Map appCtx, BaseUIMAAsynchronousEngine_impl aUimaEeEngine,
          String aBrokerURI, String aTopLevelServiceQueueName, int howMany, int aLatchKind,
          boolean sendCasAsynchronously) throws Exception {
    Thread t1 = null;
    Thread t2 = null;
    engine = aUimaEeEngine;
    isStopped = false;
    isStopping = false;

    if (appCtx == null) {
      appCtx = buildContext(aBrokerURI, aTopLevelServiceQueueName, 0);
    }
    initialize(aUimaEeEngine, appCtx);

    // Wait until the top level service returns its metadata
    waitUntilInitialized();
    for (int i = 0; i < howMany; i++) {
      Semaphore ctrlSemaphore = null;
      // Create a thread that will block until the CPC reply come back
      // from the top level service
      if (aLatchKind == EXCEPTION_LATCH) {
        ctrlSemaphore = new Semaphore(1);
        t1 = spinMonitorThread(ctrlSemaphore, 1, EXCEPTION_LATCH);
      } else {
        ctrlSemaphore = new Semaphore(2);
        t1 = spinMonitorThread(ctrlSemaphore, 1, CPC_LATCH);
        t2 = spinMonitorThread(ctrlSemaphore, 1, PROCESS_LATCH);
      }

      if (!isStopped) {
        // Wait until the CPC Thread is ready.
        waitOnMonitor(ctrlSemaphore);
        if (!isStopped) {
          // Send an in CAS to the top level service
          sendCAS(aUimaEeEngine, 1, sendCasAsynchronously);
        }
        // Wait until ALL CASes return from the service
        if (t2 != null) {
          t2.join();

          if (!serviceShutdownException && !isStopped && !unexpectedException) {
            System.out.println("runTest: Sending CPC");

            // Send CPC
            aUimaEeEngine.collectionProcessingComplete();
          }
        }

        // If have skipped CPC trip the latch
        if (serviceShutdownException || (unexpectedException && cpcLatch != null)) {
          cpcLatch.countDown();
        }
        t1.join();
      }

    }

    isStopping = true;
    aUimaEeEngine.stop();

    // Finally fail test if unhappy ... must be last call as acts like "throw"
    if (unexpectedException) {
      fail("Unexpected exception returned");
    }
  }

  /**
   * Sends a given number of CASs to Uima EE service. This method sends each CAS using either
   * synchronous or asynchronous API.
   * 
   * @param eeUimaEngine
   *          - fully initialized instance of the Uima EE client
   * @param howMany
   *          - how many CASes to send to the service
   * @param sendCasAsynchronously
   *          - use either synchronous or asynchronous API
   * @throws Exception
   */
  protected void sendCAS(BaseUIMAAsynchronousEngine_impl eeUimaEngine, int howMany,
          boolean sendCasAsynchronously) throws Exception {
    engine = eeUimaEngine;
    for (int i = 0; i < howMany; i++) {
    	if (isStopping) {
    		break;
    	}
      CAS cas = eeUimaEngine.getCAS();
      if (cas == null) {
        if (isStopping) {
          System.out.println(">> runTest: stopping after sending " + i + " of " + howMany
                  + " CASes");
        } else {
          System.out.println(">>>> ERROR! sendCas: " + (i + 1) + "-th CAS is null?");
        }
        return;
      }
      if (doubleByteText != null) {
        cas.setDocumentText(doubleByteText);
      } else {
        cas.setDocumentText(text);
      }
      if (sendCasAsynchronously) {
        eeUimaEngine.sendCAS(cas);
      } else {
        eeUimaEngine.sendAndReceiveCAS(cas);
      }
    }
  }

  /**
   * Increments total number of CASes processed
   */
  protected void incrementCASesProcessed() {
    responseCounter++;
    System.out.println("runTest: Client:::::::::::::: Received:" + responseCounter + " Reply");

  }

  public long getNumberOfCASesProcessed() {
    return responseCounter;
  }
  public void resetCASesProcessed() {
	  responseCounter = 0;
  }
  protected class UimaAsTestCallbackListener extends UimaAsBaseCallbackListener {

    private String casSent = null;
    private int pingTimeoutCount=0;
    
    public void onBeforeProcessCAS(UimaASProcessStatus status, String nodeIP, String pid) {
      System.out.println("runTest: onBeforeProcessCAS() Notification - CAS:"
              + status.getCasReferenceId()+" is being processed on machine:"+nodeIP+" by process (PID):"+pid);
    }
    public synchronized void onBeforeMessageSend(UimaASProcessStatus status) {
      casSent = status.getCasReferenceId();
      System.out.println("runTest: Received onBeforeMessageSend() Notification With CAS:"
              + status.getCasReferenceId());
    }
    public void onUimaAsServiceExit(EventTrigger cause) {
        System.out.println("runTest: Received onUimaAsServiceExit() Notification With Cause:"
                + cause.name());
    }
    public synchronized void entityProcessComplete(CAS aCAS, EntityProcessStatus aProcessStatus, List<AnalysisEnginePerformanceMetrics> componentMetricsList) {
      entityProcessComplete(aCAS, aProcessStatus);
      StringBuilder sb = new StringBuilder("--- CAS");
      sb.append(((UimaASProcessStatus) aProcessStatus).getCasReferenceId()).append(" Per Component Performance Metrics:\n");
      for(AnalysisEnginePerformanceMetrics metrics : componentMetricsList) {
        sb.append("\n\t-- Component:").
          append(metrics.getUniqueName()).
          append(" AnalysisTime:").
          append(metrics.getAnalysisTime()).
          append(" Cases Processed:").append(metrics.getNumProcessed());
      }
      System.out.println(sb.toString());
    }
    /**
     * Callback method which is called by Uima EE client when a reply to process CAS is received.
     * The reply contains either the CAS or an exception that occurred while processing the CAS.
     */
    public synchronized void entityProcessComplete(CAS aCAS, EntityProcessStatus aProcessStatus) {
      String casReferenceId = null;
      String parentCasReferenceId = null;
      boolean expectedException = false;

      if (isStopping) {
        System.out
                .println(">>>>> runTest: Ignoring entityProcessComplete callback as engine is shutting down");
        return;
      }
      if (aProcessStatus instanceof UimaASProcessStatus) {
        casReferenceId = ((UimaASProcessStatus) aProcessStatus).getCasReferenceId();
        parentCasReferenceId = ((UimaASProcessStatus) aProcessStatus).getParentCasReferenceId();
      }
      if (aProcessStatus.isException()) {
        List list = aProcessStatus.getExceptions();
        if (casReferenceId == null) {
          System.out.println("runTest: Received Reply Containing " + list.size() + " Exception(s)");
        } else if (parentCasReferenceId == null) {
          if (casSent.equals(casReferenceId)) {
            receivedExpectedParentReferenceId = true;
          }
          System.out.println("runTest: Received Reply from CAS " + casReferenceId + " Containing "
                  + list.size() + " Exception(s)");
        } else {
          System.out.println("runTest: ERROR - Received " + list.size()
                  + " Exception(s) from child CAS " + casReferenceId
                  + " --- should have been on Parent " + parentCasReferenceId);
          unexpectedException = true;
        }

        for (int i = 0; i < list.size(); i++) {
          Exception e = (Exception) list.get(i);
          if ( e instanceof BrokerConnectionException ) {
            System.out.println("Client Reported Broker Connection Failure");
            failedCasCountDueToBrokerFailure++;
          } else if (e instanceof ServiceShutdownException
                  || (e.getCause() != null && e.getCause() instanceof ServiceShutdownException)) {
            serviceShutdownException = true;
            isStopping = true;
            engine.stop();
          } else if (ignoreException(e.getClass())) {
            expectedException = true;
          } else if (e instanceof ResourceProcessException && isProcessTimeout(e)) {
            synchronized (errorCounterMonitor) {
              System.out.println("runTest: Incrementing ProcessTimeout Counter");
              timeoutCounter++;
            }
          } else if (engine != null && (e instanceof UimaASPingTimeout || (e.getCause() != null && e.getCause() instanceof UimaASPingTimeout) )) {
            System.out.println("runTest: Ping Timeout - service Not Responding To Ping");
            if (cpcLatch != null) {
              cpcLatch.countDown();
            }
            isStopping = true;
            engine.stop();
          } else if ( engine != null && e instanceof UimaASProcessCasTimeout) {
            if ( e.getCause() != null && e.getCause() instanceof UimaASPingTimeout) {
              if ( countPingRetries ) {
                if ( pingTimeoutCount > maxPingRetryCount ) {
                  if (cpcLatch != null) {
                    cpcLatch.countDown();
                  }
                  isStopping = true;
                  engine.stop();
                  
                } else {
                  pingTimeoutCount++;
                }
                
              }
            }
          }
          if (!expectedException) {
            e.printStackTrace();
          }
        }
        if (exceptionCountLatch != null) {
          exceptionCountLatch.countDown();
          if (processCountLatch != null) {
            processCountLatch.countDown();
          }
        } else if (processCountLatch != null) {
        	processCountLatch.countDown();
          if (!expectedException) {
            unexpectedException = true;
            System.out.println("runTest:  ... when expecting normal completion!");
            while (processCountLatch.getCount() > 0) {
                processCountLatch.countDown();
              }
          }
        }
      }
      // Not an exception
      else if (processCountLatch != null && aCAS != null) {
        if (parentCasReferenceId != null) {
          System.out.println("runTest: Received Reply Containing CAS:" + casReferenceId
                  + " The Cas Was Generated From Parent Cas Id:" + parentCasReferenceId);
        } else {
          System.out.println("runTest: Received Reply Containing CAS:" + casReferenceId);
        }

        if (doubleByteText != null) {
          String returnedText = aCAS.getDocumentText();
          if (!doubleByteText.equals(returnedText)) {
            System.out.println("!!! DocumentText in CAS reply different from that in CAS sent !!!");
            System.out
                    .println("This is expected using http connector with vanilla AMQ 5.0 release,");
            System.out.println("and the test file DoubleByteText.txt contains double byte chars.");
            System.out
                    .println("To fix, use uima-as/src/main/lib/optional/activemq-optional-5.0.0.jar");
            unexpectedException = true;
            processCountLatch.countDown();

            return;
          }
        }
        // test worked, reset use of this text
        doubleByteText = null;
        if (parentCasReferenceId == null) {
          processCountLatch.countDown();
        }
        List eList = aProcessStatus.getProcessTrace().getEventsByComponentName("UimaEE", false);
        for (int i = 0; i < eList.size(); i++) {
          ProcessTraceEvent eEvent = (ProcessTraceEvent) eList.get(i);
          System.out.println("runTest: Received Process Event - " + eEvent.getDescription()
                  + " Duration::" + eEvent.getDuration() + " ms"); // / (float) 1000000);
          // Check if the running test wants to check how long the processing of CAS took
          if (expectedProcessTime > 0
                  && "Total Time In Process CAS".equals(eEvent.getDescription())) {
            // Check if the expected duration exceeded actual duration for processing
            // a CAS. Allow 50ms difference.
            if (eEvent.getDuration() > expectedProcessTime
                    && (eEvent.getDuration() % expectedProcessTime) > 50) {
              System.out.println("!!!!!!!!!!!!! runTest: Expected Process CAS Duration of:"
                      + expectedProcessTime + " ms. Instead Process CAS Took:"
                      + eEvent.getDuration());
              unexpectedException = true;
            }
          }

        }
        incrementCASesProcessed();
      } else if (aCAS != null) {
        if (parentCasReferenceId != null) {
          System.out.println("runTest: Received Reply Containing CAS:" + casReferenceId
                  + " The Cas Was Generated From Parent Cas Id:" + parentCasReferenceId);
        } else {
          System.out.println("runTest: Received Reply Containing CAS:" + casReferenceId);
        }
        incrementCASesProcessed();
      }
    }

    private boolean isProcessTimeout(Exception e) {
      return (e.getCause() != null && (e.getCause() instanceof UimaASProcessCasTimeout));
    }

    /**
     * Callback method which is called by Uima EE client when the initialization of the client is
     * completed successfully.
     */
    public void initializationComplete(EntityProcessStatus aStatus) {
      boolean isPingException;

      if (aStatus != null && aStatus.isException()) {
        System.out.println("runTest: Initialization Received Reply Containing Exception:");
        List exceptions = aStatus.getExceptions();
        for (int i = 0; i < exceptions.size(); i++) {
          if (exceptions.get(i) instanceof UimaASPingTimeout) {
            System.out.println("runTest: Client Received PING Timeout. Service Not Available");
            if (cpcLatch != null) {
              cpcLatch.countDown();
            }

          }
          ((Throwable) exceptions.get(i)).printStackTrace();
        }
        if (exceptionCountLatch != null)
          exceptionCountLatch.countDown();
      }
      synchronized (initializeMonitor) {
        initialized = true;
        initializeMonitor.notifyAll();
      }
    }

    /**
     * Callback method which is called by Uima EE client when a CPC reply is received OR exception
     * occured while processing CPC request.
     */
    public void collectionProcessComplete(EntityProcessStatus aStatus) {
      if (isStopping) {
        System.out
                .println(">>>>> runTest: Ignoring collectionProcessComplete callback as engine is shutting down");
        return;
      }

      if (aStatus != null && aStatus.isException()) {

        List list = aStatus.getExceptions();
        boolean expectedException = false;
        for (int i = 0; i < list.size(); i++) {
          Exception e = (Exception) list.get(i);
          if (e instanceof ServiceShutdownException
                  || (e.getCause() != null && e.getCause() instanceof ServiceShutdownException)) {
            serviceShutdownException = true;
          } else if (ignoreException(e.getClass())) {
            expectedException = true;
          }
          if (!expectedException) {
            e.printStackTrace();
          }
        }
        if (!expectedException) {
          System.out.println("runTest: Received CPC Reply Containing Exception\n"
                  + " ... when expecting normal CPC reply!");
          unexpectedException = true;
        }
        if (exceptionCountLatch != null) {
          exceptionCountLatch.countDown();
        }
        if (cpcLatch != null) {
          cpcLatch.countDown();
        }
      } else {
        System.out.println("runTest: Received CPC Reply");
        if (cpcLatch != null) {
          cpcLatch.countDown();
        }
      }
    }
  }


  public class SimpleCallbackListener extends UimaAsTestCallbackListener {

    /**
     * Callback method which is called by Uima EE client when a reply to process CAS is received.
     * The reply contains either the CAS or an exception that occurred while processing the CAS.
     */
    public synchronized void entityProcessComplete(CAS aCAS, EntityProcessStatus aProcessStatus) {
      String casReferenceId = null;

      if (isStopping) {
        System.out
                .println(">>>>> runTest: Ignoring entityProcessComplete callback as engine is shutting down");
        return;
      }
      
      if (aProcessStatus instanceof UimaASProcessStatus) {
        if (  aProcessStatus.isException() ) {
          System.out.println("--------- Got Exception While Processing CAS:"+casReferenceId);
        } else {
          casReferenceId = ((UimaASProcessStatus) aProcessStatus).getCasReferenceId();
          System.out.println("Client Received Reply - CAS:"+casReferenceId);
        }
      }
      processCountLatch.countDown();

    }

  }
  
  
  
  
  /**
   * A Runnable class used to test concurrency support in Uima EE client. Each instance of this
   * class will start and send specified number of CASes to a service using synchronous
   * sendAndReceive API. Each thread sends a CAS and waits for a reply.
   * 
   */
  public class SynchRunner implements Runnable {
    private BaseUIMAAsynchronousEngine_impl uimaClient = null;

    private long howManyCASes = 1;
    private UimaAsTestCallbackListener callbackListener;
    public SynchRunner(BaseUIMAAsynchronousEngine_impl aUimaClient, int howMany) {
      this(aUimaClient, howMany, null);
    }
    public SynchRunner(BaseUIMAAsynchronousEngine_impl aUimaClient, int howMany, UimaAsTestCallbackListener aListener) {
      uimaClient = aUimaClient;
      howManyCASes = howMany;
      callbackListener = aListener;
    }

    // Run until All CASes are sent
    public void run() {
      UimaASProcessStatusImpl status = null;
      try {
        while (howManyCASes-- > 0) {
          CAS cas = uimaClient.getCAS();
          cas.setDocumentText(text);
          ProcessTrace pt = new ProcessTrace_impl();

          try {
            // Send CAS and wait for a response
            String casReferenceId = uimaClient.sendAndReceiveCAS(cas, pt);
            status = new UimaASProcessStatusImpl(pt, cas, casReferenceId);
          } catch (ResourceProcessException rpe) {
            //rpe.printStackTrace();
            status = new UimaASProcessStatusImpl(pt);
            status.addEventStatus("Process", "Failed", rpe);
          } finally {
            if ( callbackListener != null ) {
              callbackListener.entityProcessComplete(cas, status);
            }
            cas.release();
          }
        }
        System.out.println(">>>>>>>> runTest: Thread::" + Thread.currentThread().getId()
                + " Is Exiting - Completed Full Run");
      } catch (Exception e) {
        // e.printStackTrace();
      }
    }
  }

  protected void spinShutdownThread(final BaseUIMAAsynchronousEngine_impl uimaEEEngine, long when)
          throws Exception {
    spinShutdownThread(uimaEEEngine, when, null, 0);
  }

  protected void spinShutdownThread(final BaseUIMAAsynchronousEngine_impl uimaEEEngine, long when,
          final String[] aSpringContainerIds, final int stop_level) throws Exception {
    Date timeToRun = new Date(System.currentTimeMillis() + when);
    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      public void run() {
        timer.cancel();
        timer.purge();
        if (aSpringContainerIds == null) {
          isStopping = true;
          System.out.println(">>>> runTest: Stopping UIMA EE Engine");
          uimaEEEngine.stop();
          isStopping = false;
          isStopped = true;
          System.out.println(">>>> runTest: UIMA EE Engine Stopped");
          if (cpcLatch != null)
            cpcLatch.countDown();
          if (processCountLatch != null) {
            while (processCountLatch.getCount() > 0) {
              processCountLatch.countDown();
            }
          }
        } else {
          try {
            System.out.println(">>>> runTest: Quiescing Service And Stopping it");
            for( int i = aSpringContainerIds.length; i > 0; i--) {
              uimaEEEngine.undeploy(aSpringContainerIds[i-1], stop_level);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }

      }
    }, timeToRun);

  }
}
