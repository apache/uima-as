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

package org.apache.uima.ee.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.uima.UIMAFramework;
import org.apache.uima.UIMA_IllegalStateException;
import org.apache.uima.aae.UimaClassFactory;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.activemq.JmsOutputChannel;
import org.apache.uima.adapter.jms.activemq.SpringContainerDeployer;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.adapter.jms.message.JmsMessageContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.ee.test.utils.BaseTestSupport;
import org.apache.uima.internal.util.XMLUtils;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;
import org.apache.uima.resourceSpecifier.factory.DeploymentDescriptorFactory;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASPrimitiveDeploymentDescriptor;
import org.apache.uima.resourceSpecifier.factory.impl.ServiceContextImpl;
import org.apache.uima.util.XMLInputSource;
import org.xml.sax.SAXException;

public class TestUimaASExtended extends BaseTestSupport {

  private CountDownLatch getMetaCountLatch = null;

  private static final int MaxGetMetaRetryCount = 2;

  private static final String primitiveServiceQueue1 = "NoOpAnnotatorQueue";

  private static final String PrimitiveDescriptor1 = "resources/descriptors/analysis_engine/NoOpAnnotator.xml";

  private int getMetaRequestCount = 0;

  public BaseTestSupport superRef = null;

  /**
   * bring up testing for compressed binary serialization
   */
  
  /**
   * Test binary compressed serialization between client and svc, and between 
   * service and remote delegate
   * 
   * @throws Exception
   */
  public void testCompressedTypeFiltering() throws Exception {
    System.out.println("-------------- testCompressedTypeFiltering -------------");
    // Instantiate Uima-AS Client
    final BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(uimaAsEngine, relativePath + "/Deploy_RoomNumberAnnotator.xml");
    deployService(uimaAsEngine, relativePath + "/Deploy_MeetingDetectorTAE_RemoteRoomNumberBinary.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()), "MeetingDetectorTaeQueue");
    // Set an explicit getMeta (Ping)timeout
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 2000);
    // Set an explicit process timeout so to test the ping on timeout
    appCtx.put(UimaAsynchronousEngine.Timeout, 1000);
    appCtx.put(UimaAsynchronousEngine.SERIALIZATION_STRATEGY, "binary");

    runTest(null, uimaAsEngine, String.valueOf(broker.getMasterConnectorURI()),
            "MeetingDetectorTaeQueue", 3, PROCESS_LATCH);
  }

  
  /**
   * Tests Broker startup and shutdown
   */
  public void testBrokerLifecycle() {
    System.out.println("-------------- testBrokerLifecycle -------------");
    System.out.println("UIMA_HOME=" + System.getenv("UIMA_HOME")
            + System.getProperty("file.separator") + "bin" + System.getProperty("file.separator")
            + "dd2spring.xsl");
  }
  public void testForHang() throws Exception {
		System.out
		            .println("-------------- testForHang -------------");
		System.setProperty("BrokerURL", broker.getMasterConnectorURI());

		BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
		deployService(eeUimaEngine, relativePath + "/Deploy_ComplexAggregateMultiplier.xml");
		runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
		            100, PROCESS_LATCH);
  }

  /**
   * Tests programmatic generation of DD for deployment
   * 
   * @throws Exception
   */
  public void testGenerateAndDeployPrimitiveDD() throws Exception {
	    System.out.println("-------------- testGenerateAndDeployPrimitiveDD -------------");
	  File directory = new File (".");
	  // Set up a context object containing basic service deployment
	  // information
	  ServiceContext context = new ServiceContextImpl("PersonTitle",
			  "PersonTitle Annotator Description",
			  directory.getCanonicalPath() + 
			  System.getProperty("file.separator")+
			  resourceDirPath+
			  System.getProperty("file.separator")+
			  "descriptors" +
			  System.getProperty("file.separator")+
			  "analysis_engine" +
			  System.getProperty("file.separator")+
			  "PersonTitleAnnotator.xml", 
			  "PersonTitleAnnotatorQueue",
			  broker.getMasterConnectorURI());
	  context.setCasPoolSize(2);
	  // create DD with default settings
	  UimaASPrimitiveDeploymentDescriptor dd = DeploymentDescriptorFactory
			  .createPrimitiveDeploymentDescriptor(context);

	  // Get default Error Handler for process and change error threshold
	  dd.getProcessErrorHandlingSettings().setThresholdCount(4);

	  // Two instances of AE in a jvm
	  dd.setScaleup(2);

	  // Generate deployment descriptor in xml format
	  String ddXML = dd.toXML();
	  System.out.println(ddXML);
	  
	  File tempFile = File.createTempFile("Deploy_PersonTitle", ".xml");
	  BufferedWriter out = new BufferedWriter(new FileWriter(tempFile));
	  out.write(ddXML);
	  out.close();
	  char FS = System.getProperty("file.separator").charAt(0);
	  
	// create Map to hold required parameters
	  Map<String,Object> appCtx = new HashMap<String,Object>();
	  appCtx.put(UimaAsynchronousEngine.DD2SpringXsltFilePath,
	            "../src/main/scripts/dd2spring.xsl".replace('/', FS));
	  appCtx.put(UimaAsynchronousEngine.SaxonClasspath,
	            "file:../src/main/saxon/saxon8.jar".replace('/', FS));	  
	  
	  BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
	  String aSpringContainerId =
	      eeUimaEngine.deploy(tempFile.getAbsolutePath(), appCtx);
	  
	  eeUimaEngine.undeploy(aSpringContainerId);
	  eeUimaEngine.stop();
	  
	  
  }
  public void testDeployAggregateServiceWithFailingCollocatedCM() throws Exception {
    System.out.println("-------------- testDeployAggregateServiceWithFailingCollocatedCM -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithFailingCollocatedCM.xml");
    
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    appCtx.put(UimaAsynchronousEngine.Timeout, 0);
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 0);
    
    //addExceptionToignore(org.apache.uima.aae.error.UimaEEServiceException.class); 
    
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }
  
  
  public void getLargeCAS(CAS aCAS, File xmiFile) throws IOException, CollectionException {
	    FileInputStream inputStream = new FileInputStream(xmiFile);
	    try {
	    	XmiCasDeserializer.deserialize(inputStream, aCAS, false);
	    } catch (SAXException e) {
	      throw new CollectionException(e);
	    } finally {
	      inputStream.close();
	    }
	    
	  }
  /*
  public void testLargeCAS() {
	    System.out.println("-------------- testLargeCAS -------------");
	    try {
		    // Instantiate Uima AS Client
		    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
		    // Deploy Uima AS Primitive Service
//		    deployService(uimaAsEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
		    deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
//		    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
//		            "PersonTitleAnnotatorQueue");
		    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
		            "NoOpAnnotatorQueue");
		    
		    
		    appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
		    appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
		    initialize(uimaAsEngine, appCtx);
		    waitUntilInitialized();
	        CAS cas = uimaAsEngine.getCAS();
	        getLargeCAS(cas, new File("C:/uima/largeCASTest/NYT_ENG_20070514.0065.out.xmi"));
	       
	        System.out.println("UIMA AS Client Sending CAS Request to a Service");
		    uimaAsEngine.sendCAS(cas);
		    uimaAsEngine.collectionProcessingComplete();
		    System.clearProperty("DefaultBrokerURL");
		    uimaAsEngine.stop();	  
	    	
	    } catch( Exception e) {
	    	e.printStackTrace();
	    }
  }
  */
  /**
   * Tests service quiesce and stop support. This test sets a CasPool to 1 to send just one CAS at a
   * time. After the first CAS is sent, a thread is started with a timer to expire before the reply
   * is received. When the timer expires, the client initiates quiesceAndStop on the top level
   * controller. As part of this, the top level controller stops its listeners on the input queue
   * (GetMeta and Process Listeners), and registers a callback with the InProcess cache. When the
   * cache is empty, meaning all CASes are processed, the cache notifies the controller which then
   * begins the service shutdown. Meanwhile, the client receives a reply for the first CAS, and
   * sends a second CAS. This CAS, will remain in the queue as the service has previously stopped
   * its listeners. The client times out after 10 seconds and shuts down.
   * 
   * @throws Exception
   */
  public void testQuiesceAndStop() throws Exception {
    System.out.println("-------------- testQuiesceAndStop -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    // Set an explicit process timeout so to test the ping on timeout
    appCtx.put(UimaAsynchronousEngine.Timeout, 10000);
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 300);
    appCtx.put(UimaAsynchronousEngine.CasPoolSize, 10);
    String containers[] = new String[1];
    containers[0] = deployService(eeUimaEngine, relativePath + "/Deploy_ScaledPrimitiveAggregateAnnotator.xml");

    
//    String containers[] = new String[2];
 //   containers[0] = deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
   // containers[1] =  deployService(eeUimaEngine, relativePath
     //       + "/Deploy_AggregateAnnotatorWithInternalCM1000Docs.xml");
//    spinShutdownThread(eeUimaEngine, 5000, containers, SpringContainerDeployer.QUIESCE_AND_STOP);
    spinShutdownThread(eeUimaEngine, 5000, containers, SpringContainerDeployer.QUIESCE_AND_STOP);
    
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue", 5000, EXCEPTION_LATCH);
    //eeUimaEngine.stop();
  }
/*
  public void testQuiesceAndStop2() throws Exception {
	    System.out.println("-------------- testQuiesceAndStop -------------");
	    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
	    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
	            "TopLevelTaeQueue");
	    // Set an explicit process timeout so to test the ping on timeout
	    appCtx.put(UimaAsynchronousEngine.Timeout, 10000);
	    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 300);
	    appCtx.put(UimaAsynchronousEngine.CasPoolSize, 1);
	    String containers[] = new String[1];
	    containers[0] = deployService(eeUimaEngine, relativePath + "/Deploy_ScaledPrimitiveAggregateAnnotator.xml");
	    
	    
	    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
	            "TopLevelTaeQueue", 100, PROCESS_LATCH);
	    System.out.println("------------ Undeploying ----------------");
	    eeUimaEngine.undeploy(containers[0] , SpringContainerDeployer.QUIESCE_AND_STOP);
//	    eeUimaEngine.stop();
	  }

  */
  
  
  
  public void testStopNow() throws Exception {
    System.out.println("-------------- testAggregateWithFailedRemoteDelegate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    String containers[] = new String[2];

    containers[0] = deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    containers[1] = deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateAnnotatorWithInternalCM1000Docs.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    // Set an explicit process timeout so to test the ping on timeout
    appCtx.put(UimaAsynchronousEngine.Timeout, 4000);
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 300);
    spinShutdownThread(eeUimaEngine, 3000, containers, SpringContainerDeployer.STOP_NOW);
    //  send may fail since we forcefully stop the service. Tolerate
    //  ResourceProcessException
    addExceptionToignore(ResourceProcessException.class); 
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue", 10, EXCEPTION_LATCH);
  }
  public void testSendAndReceive() throws Exception  {
      BaseUIMAAsynchronousEngine_impl uimaAsEngine 
      	= new BaseUIMAAsynchronousEngine_impl();
      
      deployService(uimaAsEngine, relativePath + "/Deploy_MeetingDetectorAggregate.xml");
      // Deploy Uima AS Primitive Service
 //     deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
      Map<String, Object> appCtx = buildContext(broker.getMasterConnectorURI().toString(),"MeetingDetectorQueue");
      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();
      int errorCount = 0;
      List<AnalysisEnginePerformanceMetrics> componentMetricsList = 
    		  new ArrayList<AnalysisEnginePerformanceMetrics>();
      for (int i = 0; i < 15; i++) {
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaAsEngine.sendAndReceiveCAS(cas,componentMetricsList);
          System.out.println("-------> Client Received Performance Metrics of Size:"+componentMetricsList.size());
        } catch( Exception e) {
          errorCount++;
        } finally {
          cas.release();
          componentMetricsList.clear();
        }
      }
      uimaAsEngine.stop();
  }
  public void testMultipleSyncClientsWithMultipleBrokers() throws Exception  {
	    System.out.println("-------------- testMultipleSyncClientsWithMultipleBrokers -------------");
	    
	    class RunnableClient implements Runnable {
	    	String brokerURL;
	    	BaseTestSupport testSupport;
            BaseUIMAAsynchronousEngine_impl uimaAsEngine;
	    	
	    	RunnableClient(String brokerURL,BaseTestSupport testSupport) {
	    		this.brokerURL = brokerURL;
	    		this.testSupport = testSupport;
	    	}
	    	public void initialize(String dd, String serviceEndpoint) throws Exception {
	    		uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
	            // Deploy Uima AS Primitive Service
	            deployService(uimaAsEngine, dd);

	    		@SuppressWarnings("unchecked")
			  Map<String, Object> appCtx = buildContext(brokerURL, serviceEndpoint);
		  	  appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
		  	  appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
		  	  testSupport.initialize(uimaAsEngine, appCtx);
		  	  waitUntilInitialized();
	    	}
			public void run() {
				try {
		            for (int i = 0; i < 1000; i++) {
			              CAS cas = uimaAsEngine.getCAS();
			              cas.setDocumentText("Some Text");
			              System.out.println("UIMA AS Client#"+ Thread.currentThread().getId()+" Sending CAS#"+(i + 1) + " Request to a Service Managed by Broker:"+brokerURL);
			              try {
				                uimaAsEngine.sendAndReceiveCAS(cas);
			              } catch( Exception e) {
			            	  e.printStackTrace();
			              } finally {
			                cas.release();
			              }
			            }
			            System.out.println("Thread:"+Thread.currentThread().getId()+" Completed run()");
			            uimaAsEngine.stop();
				} catch( Exception e) {
					e.printStackTrace();
				}

			}
	    	
	    }
	    
	    ExecutorService executor = Executors.newCachedThreadPool();

	    //	change broker URl in system properties
	    System.setProperty("BrokerURL", broker.getMasterConnectorURI().toString());
	    
	    RunnableClient client1 = 
	    		new RunnableClient(broker.getMasterConnectorURI(), this);
	    client1.initialize(relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml", "NoOpAnnotatorQueue");

	    final BrokerService broker2 = setupSecondaryBroker(true);

	    //	change broker URl in system properties
	    System.setProperty("BrokerURL", broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());

	    RunnableClient client2 = 
	    		new RunnableClient(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), this);
	    client2.initialize(relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml", "NoOpAnnotatorQueue");

	    Future<?> f1 = executor.submit(client1);
	    Future<?> f2 = executor.submit(client2);
	    f1.get();
	    f2.get();
	    executor.shutdownNow();
	    broker2.stop();
	    broker.stop();
	    broker.waitUntilStopped();
	}
  
  public void testAggregateHttpTunnelling() throws Exception {
    System.out.println("-------------- testAggregateHttpTunnelling -------------");
    // Create Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy remote service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    // Deploy top level aggregate that communicates with the remote via Http Tunnelling
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithHttpDelegate.xml");

    // Initialize and run the Test. Wait for a completion and cleanup resources.
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            10, CPC_LATCH);
  }
  public void testClientHttpTunnellingToAggregate() throws Exception {
    System.out.println("-------------- testClientHttpTunnellingToAggregate -------------");
    // Add HTTP Connector to the broker. 
    String httpURI = getHttpURI();
    // Create Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy remote service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotator.xml");
    // Initialize and run the Test. Wait for a completion and cleanup resources.
    System.out.println("-------- Connecting Client To Service: "+httpURI);
    runTest(null, eeUimaEngine, httpURI, "TopLevelTaeQueue", 1, CPC_LATCH);
  }
  public void testClientHttpTunnelling() throws Exception {
    System.out.println("-------------- testClientHttpTunnelling -------------");
    String httpURI = getHttpURI();
    // Create Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy remote service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    // Initialize and run the Test. Wait for a completion and cleanup resources.
    System.out.println("-------- Connecting Client To Service: "+httpURI);
    runTest(null, eeUimaEngine, httpURI, "NoOpAnnotatorQueue", 1, PROCESS_LATCH);
  }


  public void testClientHttpTunnellingWithDoubleByteText() throws Exception {
    System.out.println("-------------- testClientHttpTunnellingWithDoubleByteText -------------");

    BufferedReader in = null;
    try {
      File file = new File(relativeDataPath + "/DoubleByteText.txt");
      System.out.println("Checking for existence of File:" + file.getAbsolutePath());
      // Process only if the file exists
      if (file.exists()) {
        System.out
                .println(" *** DoubleByteText.txt exists and will be sent through http connector.");
        System.out.println(" ***   If the vanilla activemq release is being used,");
        System.out
                .println(" ***   and DoubleByteText.txt is bigger than 64KB or so, this test case will hang.");
        System.out
                .println(" *** To fix, override the classpath with the jar files in and under the");
        System.out
                .println(" ***   apache-uima-as/uima-as/src/main/apache-activemq-X.y.z directory");
        System.out.println(" ***   in the apache-uima-as source distribution.");

        String httpURI = getHttpURI();
      // Create Uima-AS Client
        BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
        // Deploy remote service
        deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");

        InputStream fis = new FileInputStream(file);
        Reader rd = new InputStreamReader(fis, "UTF-8");
        in = new BufferedReader(rd);
        // Set the double-byte text. This is what will be sent to the service
        String line = in.readLine();
        super.setDoubleByteText(line);
        int err = XMLUtils.checkForNonXmlCharacters(line);
        if (err >= 0) {
          fail("Illegal XML char at offset " + err);
        }
        System.out.println("-------- Connecting Client To Service: "+httpURI);
        // Initialize and run the Test. Wait for a completion and cleanup resources.
        runTest(null, eeUimaEngine, httpURI, "NoOpAnnotatorQueue", 1, CPC_LATCH);
      }
    } catch (Exception e) {
      // Double-Byte Text file not present. Continue on with the next test
      e.printStackTrace();
      fail("Could not complete test");
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
  
  /**
   * Tests support for ActiveMQ failover protocol expressed in broker
   * URL as follows "failover:(tcp:IP:Port1,tcp:IP:Port2)". The test launches a secondary
   * broker, launches a Primitive service that uses that broker,
   * and finally configures the UIMA AS Client to connect to the secondary broker
   * and specifies an alternative broker on a different port. This test 
   * only tests ability of UIMA AS to handle a complex URL, and it does *not*
   * test the actual failover from one broker to the next. 
   * 
   * @throws Exception
   */
  public void testBrokerFailoverSupportUsingHTTP() throws Exception  {
    System.out.println("-------------- testBrokerFailoverSupportUsingTCP -------------");
     // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
      BrokerService broker2 = setupSecondaryBroker(false);

      String bhttpURL = addHttpConnector(broker2, DEFAULT_HTTP_PORT2);
      String burl = "failover:("+bhttpURL+","+broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString().replaceAll("tcp","http")+")?randomize=false";
      System.setProperty("BrokerURL", burl); 
      // Deploy Uima AS Primitive Service
      deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      Map<String, Object> appCtx = buildContext(burl,"NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();
      int errorCount = 0;
      for (int i = 0; i < 15; i++) {
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaAsEngine.sendAndReceiveCAS(cas);
        } catch( Exception e) {
          errorCount++;
        } finally {
          cas.release();
        }
      }
      uimaAsEngine.stop();
      super.cleanBroker(broker2);
      broker2.stop();
      broker2.waitUntilStopped();

  }
  
  /**
   * Tests support for ActiveMQ failover protocol expressed in broker
   * URL as follows "failover:(tcp:IP:Port1,tcp:IP:Port2)". The test launches a secondary
   * broker, launches a Primitive service that uses that broker,
   * and finally configures the UIMA AS Client to connect to the secondary broker
   * and specifies an alternate broker on a different port. This test 
   * only tests ability of UIMA AS to handle a complex URL, and it does *not*
   * test the actual failover from one broker to the next. 
   * 
   * @throws Exception
   */
  public void testBrokerFailoverSupportUsingTCP() throws Exception  {
    System.out.println("-------------- testBrokerFailoverSupportUsingTCP -------------");
     // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
      
      BrokerService broker2 = setupSecondaryBroker(true);
      // Deploy Uima AS Primitive Service
      deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      Map<String, Object> appCtx = buildContext("failover:("+System.getProperty("BrokerURL")+","+getBrokerUri()+")?randomize=false","NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();
      int errorCount = 0;
      for (int i = 0; i < 15; i++) {
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaAsEngine.sendAndReceiveCAS(cas);
        } catch( Exception e) {
          errorCount++;
        } finally {
          cas.release();
        }
      }
      uimaAsEngine.stop();
      super.cleanBroker(broker2);

      broker2.stop();
      broker2.waitUntilStopped();

  }
  
  /**
   * This test starts a secondary broker, starts NoOp Annotator, and
   * using synchronous sendAndReceive() sends 10 CASes for analysis. Before sending 11th, the test
   * stops the secondary broker and sends 5 more CASes. All CASes sent after
   * the broker shutdown result in GetMeta ping and a subsequent timeout.
   * @throws Exception
   */
  
  public void testSyncClientRecoveryFromBrokerStop() throws Exception  {
    System.out.println("-------------- testSyncClientRecoveryFromBrokerStop -------------");
     // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
      BrokerService broker2 = setupSecondaryBroker(true);
      // Deploy Uima AS Primitive Service
      deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      Map<String, Object> appCtx = 
      buildContext(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 1100);
      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 300);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();
      int errorCount = 0;
      for (int i = 0; i < 15; i++) {
        
        if ( i == 10 ) {
          //  Stop the broker
          broker2.stop();
          broker2.waitUntilStopped();
        }
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaAsEngine.sendAndReceiveCAS(cas);
        } catch( Exception e) {
          errorCount++;
          System.out.println("Client Received Expected Error on CAS:"+(i+1)+" ErrorCount:"+errorCount);
        } finally {
          cas.release();
        }
      }
      
      uimaAsEngine.stop();
      //  expecting 5 failures due to broker missing
      if ( errorCount != 5 ) {
        fail("Expected 5 failures due to broker down, instead received:"+errorCount+" failures");
      }
  }
  /**
   * This test starts a secondary broker, starts NoOp Annotator, and
   * using synchronous sendAndReceive() sends 5 CASes for analysis. Before sending 6th, the test
   * stops the secondary broker and sends 5 more CASes. All CASes sent after
   * the broker shutdown result in GetMeta ping and a subsequent timeout. Before
   * sending 11th CAS the test starts the broker again and sends 10 more CASes 
   * @throws Exception
   */
  public void testSyncClientRecoveryFromBrokerStopAndRestart() throws Exception  {
    System.out.println("-------------- testSyncClientRecoveryFromBrokerStopAndRestart -------------");
     // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
      BrokerService broker2 = setupSecondaryBroker(true);
      // Deploy Uima AS Primitive Service
      deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      
      Map<String, Object> appCtx = 
      buildContext(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 20000);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();
      int errorCount=0;
      for (int i = 0; i < 20; i++) {
        
        if ( i == 5 ) {
          broker2.stop();
          broker2.waitUntilStopped();
        } else if ( i == 10 ) {
          //  restart the broker 
          System.setProperty("activemq.broker.jmx.domain","org.apache.activemq.test");
          broker2 = setupSecondaryBroker(true);
          
          broker2.start();
          broker2.waitUntilStarted();

        }
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaAsEngine.sendAndReceiveCAS(cas);
        } catch( Exception e) {
          errorCount++;
          System.out.println("Client Received Expected Error on CAS:"+(i+1));
        } finally {
          cas.release();
        }
      }
      
      uimaAsEngine.stop();
      super.cleanBroker(broker2);

      broker2.stop();

      //  expecting 5 failures due to broker missing
      if ( errorCount != 5 ) {
        fail("Expected 5 failures due to broker down, instead received:"+errorCount+" failures");
      }
      broker2.waitUntilStopped();

  }
  /**
   * This test creates 4 UIMA AS clients and runs each in a separate thread. There is a single 
   * shared jms connection to a broker that each client uses. After initialization a client
   * sends 1000 CASes to a remote service. While clients are processing the test kills 
   * the broker, waits for 4 seconds and restarts it. While the broker is down, clients
   * keep trying sending CASes, receiving Ping timeouts. Once the broker is available again
   * all clients should recover and begin processing CASes again. This tests recovery of a 
   * shared connection.
   * 
   * @throws Exception
   */
  public void testMultipleSyncClientsRecoveryFromBrokerStopAndRestart() throws Exception  {
    System.out.println("-------------- testMultipleSyncClientsRecoveryFromBrokerStopAndRestart -------------");
    final BrokerService broker2 = setupSecondaryBroker(true);
    final CountDownLatch latch = new CountDownLatch(4);
    Thread[] clientThreads = new Thread[4];
    
    //  Create 4 Uima AS clients each running in a separate thread
    for(int i=0; i < 4; i++) {
      clientThreads[i] = new Thread() {
        public void run() {
          BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
          try {
            // Deploy Uima AS Primitive Service
            deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
            Map<String, Object> appCtx = 
              buildContext(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "NoOpAnnotatorQueue");

            appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
            appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
            initialize(uimaAsEngine, appCtx);
            waitUntilInitialized();
            for (int i = 0; i < 1000; i++) {
              if ( i == 5 ) {
                latch.countDown();   // indicate that some CASes were processed
              }
              CAS cas = uimaAsEngine.getCAS();
              cas.setDocumentText("Some Text");
              System.out.println("UIMA AS Client#"+ Thread.currentThread().getId()+" Sending CAS#"+(i + 1) + " Request to a Service");
              try {
                uimaAsEngine.sendAndReceiveCAS(cas);
              } catch( Exception e) {
                System.out.println("Client Received Expected Error on CAS:"+(i+1));
              } finally {
                cas.release();
              }
              synchronized(uimaAsEngine) {
            	  uimaAsEngine.wait(100);
              }
            }
            System.out.println("Thread:"+Thread.currentThread().getId()+" Completed run()");
            uimaAsEngine.stop();
          } catch( Exception e) {
            e.printStackTrace();
            return;
          }
        }
      };
      clientThreads[i].start();
    }
    BrokerService broker3 =  null;
    try {
      latch.await();  // wait for all threads to process a few CASes

      broker2.stop();
      System.out.println("Stopping Broker - wait ...");
      broker2.waitUntilStopped();

      System.out.println("Restarting Broker - wait ...");
      //  restart the broker 
      broker3 = setupSecondaryBroker(true);
      broker3.waitUntilStarted();

    } catch ( Exception e ) {
      
    } finally {
      for(int i=0; i < 4; i++ ) {
        clientThreads[i].join();
      }
      System.out.println("Stopping Broker - wait ...");
      if ( broker3 != null ) {
        super.cleanBroker(broker3);

        broker3.stop();
        broker3.waitUntilStopped();

      }
    }
}
  /**
   * This test starts a secondary broker, starts NoOp Annotator, and
   * using asynchronous send() sends a total of 15 CASes for analysis. After processing 11th
   * the test stops the secondary broker and sends 4 more CASes which fails due to broker not running.
   * 
   * @throws Exception
   */
  public void testAsyncClientRecoveryFromBrokerStop() throws Exception  {
    System.out.println("-------------- testAsyncClientRecoveryFromBrokerStop -------------");
    
    BrokerService broker2 = setupSecondaryBroker(true);
     // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
      deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      Map<String, Object> appCtx = 
      buildContext(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "NoOpAnnotatorQueue");
      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();
      
      for (int i = 0; i < 15; i++) {
        
        if ( i == 10 ) {
          broker2.stop();
          broker2.waitUntilStopped();

        }
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        uimaAsEngine.sendCAS(cas);
      }
      
      uimaAsEngine.stop();
      super.cleanBroker(broker2);
      broker2.stop();
      broker2.waitUntilStopped();

  }
  
  public void testAsyncClientRecoveryFromBrokerStopAndRestart() throws Exception  {
    System.out.println("-------------- testAsyncClientRecoveryFromBrokerStopAndRestart -------------");

    BrokerService broker2 = setupSecondaryBroker(true);
     // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
      // Deploy Uima AS Primitive Service
      deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      Map<String, Object> appCtx = 
        buildContext(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "NoOpAnnotatorQueue");

      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();

      for (int i = 0; i < 150; i++) {
        if ( i == 10 ) {
          broker2.stop();
          broker2.waitUntilStopped();

        } else if ( i == 20 ) {
          broker2 = setupSecondaryBroker(true);
          broker2.waitUntilStarted();

        }
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        uimaAsEngine.sendCAS(cas);
      }
      
      uimaAsEngine.stop();
      super.cleanBroker(broker2);

      broker2.stop();
      broker2.waitUntilStopped();

  }

  /**
   * Tests recovery from a broker restart when running multiple instances of 
   * UIMA AS client in the same JVM. The scenario is:
   * 1) start broker
   * 2) start service
   * 3) create 1st client and initialize it
   * 4) create 2nd client and initialize it
   * 5) send 1 CAS from client#1
   * 6) when reply is received kill broker and restart it
   * 7) send 2 CAS from client#1
   * 8) CAS #2 fails, but forces SharedConnection to reconnect to broker
   * 9) CAS#3 and #4 are sent from client#1
   * 10) CASes 1-4 are sent from client#2 without error
   * 
   * @throws Exception
   */

  public void testMultipleClientsRecoveryFromBrokerStopAndRestart() throws Exception  {
    System.out.println("-------------- testMultipleClientsRecoveryFromBrokerStopAndRestart -------------");
      
    BrokerService broker2 = setupSecondaryBroker(true);
      // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaClient1 = new BaseUIMAAsynchronousEngine_impl();
      // Deploy Uima AS Primitive Service
      deployService(uimaClient1, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      Map<String, Object> appCtx = 
        buildContext(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "NoOpAnnotatorQueue");
    
      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaClient1, appCtx);
      waitUntilInitialized();
      
      // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaClient2 = new BaseUIMAAsynchronousEngine_impl();
      Map<String, Object> appCtx2 = 
      buildContext(broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "NoOpAnnotatorQueue");

      
      appCtx2.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx2.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaClient2, appCtx2);
      waitUntilInitialized();
      
      
      int errorCount=0;
      for (int i = 0; i < 4; i++) {
        //  Stop broker before second CAS is sent to the service
        if ( i == 1 ) {
          broker2.stop();
          broker2.waitUntilStopped();

          //  restart broker before 3rd CAS is sent
          //  restart the broker 
          broker2 = setupSecondaryBroker(true);
          broker2.waitUntilStarted();

        } 
        CAS cas = uimaClient1.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client#1 Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaClient1.sendAndReceiveCAS(cas);
        } catch( Exception e) {
          errorCount++;
          System.out.println("UIMA AS Client#1 Received Expected Error on CAS:"+(i+1));
        } finally {
          cas.release();
        }
      }
      for (int i = 0; i < 4; i++) {
        CAS cas = uimaClient2.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client#2 Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaClient2.sendAndReceiveCAS(cas);
        } catch( Exception e) {
          errorCount++;
          System.out.println("UIMA AS Client#2 Received Expected Error on CAS:"+(i+1));
        } finally {
          cas.release();
        }
      }
      
      uimaClient1.stop();
      uimaClient2.stop();
      super.cleanBroker(broker2);

      broker2.stop();
      broker2.waitUntilStopped();

  }
  /**
   * Tests ability of an aggregate to recover from a Broker restart. The broker managing
   * delegate's input queue is stopped after 1st CAS is fully processed. As part of error
   * handling the listener on delegate temp reply queue is stopped and a delegate marked 
   * as FAILED. The aggregate error handling is configured to retry the command and as
   * part of retry a new temp queue and a listener are created for the delegate when
   * the broker is restarted. When a 2nd CAS is sent from a client to the aggregate the 
   * aggregate will force retry of the previous CAS. Once this is done, 2nd CAS is sent 
   * to the delegate and processing continues.
   * @throws Exception
   */
  public void testAggregateRecoveryFromBrokerStopAndRestart() throws Exception  {
    System.out.println("-------------- testAggregateRecoveryFromBrokerStopAndRestart -------------");
    runAggregateRecoveryFromBrokerStopAndRestart("Deploy_AggregateWithRemoteNoOpOnBroker8200.xml");

  }
  /**
   * Tests ability of an aggregate to recover from a Broker restart. The broker managing
   * delegate's input queue is stopped after 1st CAS is fully processed. As part of error
   * handling the listener on delegate temp reply queue is stopped and a delegate marked 
   * as FAILED. The aggregate error handling is configured with no retries. After the broker
   * is stopped, the listener on a temp reply queue is shutdown. When the broker is restarted
   * the client sends a new CAS which forces creation of a new temp queue and new listener.
   * @throws Exception
   */
  public void testAggregateRecoveryFromBrokerStopAndRestartNoDelegateRetries() throws Exception  {
    System.out.println("-------------- testAggregateRecoveryFromBrokerStopAndRestartNoDelegateRetries -------------");
    runAggregateRecoveryFromBrokerStopAndRestart("Deploy_AggregateWithRemoteNoOpOnBroker8200NoRetry.xml");
  }
  private void runAggregateRecoveryFromBrokerStopAndRestart(String aggregateDescriptor ) throws Exception {
    BrokerService broker2 = setupSecondaryBroker(false);
    System.setProperty("BrokerURL", broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());

      // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaClient1 = new BaseUIMAAsynchronousEngine_impl();
      // Deploy Uima AS Primitive Service
      deployService(uimaClient1, relativePath + "/Deploy_NoOpAnnotatorWithPlaceholder.xml");
      deployService(uimaClient1, relativePath + "/"+aggregateDescriptor);
      Map<String, Object> appCtx = 
      buildContext(broker.getMasterConnectorURI(), "TopLevelTaeQueue");

      appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaClient1, appCtx);
      waitUntilInitialized();
      
      
      int errorCount=0;
      for (int i = 0; i < 10; i++) {
        //  Stop broker before second CAS is sent to the service
        if ( i == 1 ) {
          System.out.println("Stopping Secondary Broker Running on Port:"+broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());
          broker2.stop();
          broker2.waitUntilStopped();

          //  restart broker before 3rd CAS is sent
          //  restart the broker 
          broker2 = setupSecondaryBroker(true);
          broker2.waitUntilStarted();

        } 
        CAS cas = uimaClient1.getCAS();
        cas.setDocumentText("Some Text");
        System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
        try {
          uimaClient1.sendAndReceiveCAS(cas);
          System.out.println("UIMA AS Client Received Reply For CAS#" + (i + 1) );
        } catch( Exception e) {
          errorCount++;
          System.out.println("UIMA AS Client Received Expected Error on CAS:"+(i+1));
        } finally {
          cas.release();
        }
      }
      uimaClient1.stop();
      System.out.println("Stopping Broker - wait ...");
      super.cleanBroker(broker2);

      broker2.stop();
      broker2.waitUntilStopped();

  }
  /**
   * Tests sending CPC after CAS timeout. The service is a Primitive taking 
   * 6 seconds to process a CAS. The client waits for 5 secs to force
   * a timeout. This test forces the client to send GetMeta Ping and to
   * 'hold' the subsequent CPC request.
   *   
   * @throws Exception
   */
  public void testCpcAfterCasTimeout() throws Exception  {
    System.out.println("-------------- testCpcAfterCasTimeout -------------");
     // Instantiate Uima AS Client
      BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
      deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorAWithLongDelay.xml");
      Map<String, Object> appCtx = buildContext(broker.getMasterConnectorURI(),
      "NoOpAnnotatorAQueue");
      appCtx.put(UimaAsynchronousEngine.Timeout, 5000);
      appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
      initialize(uimaAsEngine, appCtx);
      waitUntilInitialized();
      
      for( int i=0; i < 3; i++ ) {
        CAS cas = uimaAsEngine.getCAS();
        cas.setDocumentText("Some Text");
        uimaAsEngine.sendCAS(cas);  // will timeout after 5 secs
        uimaAsEngine.collectionProcessingComplete();  // the CPC should not
        // be sent to a service until the timeout occurs.
      }
      uimaAsEngine.stop();
  }
  public void testClientCRProcess() throws Exception {
	    System.out.println("-------------- testClientCRProcess -------------");
	    super.resetCASesProcessed();
	    
	    // Instantiate Uima AS Client
	    final BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
//	    UIMAFramework.getLogger(BaseUIMAAsynchronousEngineCommon_impl.class).setLevel(Level.FINEST);
//	    UIMAFramework.getLogger(BaseUIMAAsynchronousEngine_impl.class).setLevel(Level.FINEST);
//	    UIMAFramework.getLogger().setLevel(Level.FINEST);
//	    UIMAFramework.getLogger().setOutputStream(System.out);

	    // Deploy Uima AS Primitive Service
	    deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
	    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
	            "NoOpAnnotatorQueueLongDelay");
	    appCtx.put(UimaAsynchronousEngine.Timeout, 0);
	    appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
	    appCtx.put(UimaAsynchronousEngine.CasPoolSize,10);
	    
	    String collectionReaderDescriptor =
	    		resourceDirPath + System.getProperty("file.separator") +
	    		"descriptors"+ System.getProperty("file.separator") +
	    		"collection_reader"+ System.getProperty("file.separator") +
	    		"FileSystemCollectionReader.xml";
	   		  
         // add Collection Reader if specified
         try {
             CollectionReaderDescription collectionReaderDescription = 
                     UIMAFramework.getXMLParser()
                             .parseCollectionReaderDescription(new XMLInputSource(collectionReaderDescriptor));
             collectionReaderDescription.getCollectionReaderMetaData().
             	getConfigurationParameterSettings().
             		setParameterValue("InputDirectory", relativeDataPath);
             CollectionReader collectionReader = UIMAFramework
                     .produceCollectionReader(collectionReaderDescription);
             uimaAsEngine.setCollectionReader(collectionReader);	    
         } catch( Throwable e) {
        	 e.printStackTrace();
         }
	    
	    initialize(uimaAsEngine, appCtx);
	    waitUntilInitialized();
	    
	    uimaAsEngine.process();

	    Assert.assertEquals(8, getNumberOfCASesProcessed());
	    System.clearProperty("DefaultBrokerURL");
	    uimaAsEngine.stop();
	  }

  public void testClientProcess() throws Exception {
    System.out.println("-------------- testClientProcess -------------");
    
    // Instantiate Uima AS Client
    final BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima AS Primitive Service
    deployService(uimaAsEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
			"PersonTitleAnnotatorQueue");
    appCtx.put(UimaAsynchronousEngine.Timeout, 0);
    appCtx.put(UimaAsynchronousEngine.CpcTimeout, 1100);
    appCtx.put(UimaAsynchronousEngine.CasPoolSize,2);
    initialize(uimaAsEngine, appCtx);
    waitUntilInitialized();

    for (int i = 0; i < 50; i++) {
      CAS cas = uimaAsEngine.getCAS();
      cas.setDocumentText("Some Text");
      System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
      uimaAsEngine.sendCAS(cas);
    }
    
    uimaAsEngine.collectionProcessingComplete();
    System.clearProperty("DefaultBrokerURL");
    uimaAsEngine.stop();
  }
  
  
  public void testClientProcessTimeout() throws Exception {
    System.out
            .println("-------------- testClientProcessTimeout -------------");
    // Instantiate Uima AS Client
    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima AS Primitive Service
    deployService(uimaAsEngine, relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueueLongDelay");
    appCtx.put(UimaAsynchronousEngine.Timeout, 1100);
    initialize(uimaAsEngine, appCtx);
    waitUntilInitialized();

    for (int i = 0; i < 1; i++) {
      CAS cas = uimaAsEngine.getCAS();
      cas.setDocumentText("Some Text");
      System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
      uimaAsEngine.sendCAS(cas);
    }
    
    uimaAsEngine.collectionProcessingComplete();
    uimaAsEngine.stop();
  
  
  }
  
  
  public void testClientBrokerPlaceholderSubstitution() throws Exception {
    System.out.println("-------------- testClientBrokerPlaceholderSubstitution -------------");
    // Instantiate Uima AS Client
    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima AS Primitive Service
    deployService(uimaAsEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");

    System.setProperty( "defaultBrokerURL", broker.getMasterConnectorURI());
    Map<String, Object> appCtx = buildContext("${defaultBrokerURL}","PersonTitleAnnotatorQueue");

    initialize(uimaAsEngine, appCtx);
    waitUntilInitialized();
    for (int i = 0; i < 10; i++) {
      CAS cas = uimaAsEngine.getCAS();
      cas.setDocumentText("Some Text");
      System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
      uimaAsEngine.sendCAS(cas);
    }
    uimaAsEngine.collectionProcessingComplete();
    uimaAsEngine.stop();
    
  }

  public void testClientEndpointPlaceholderSubstitution() throws Exception {
	    System.out.println("-------------- testClientEndpointPlaceholderSubstitution -------------");
	    // Instantiate Uima AS Client
	    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
	    // Deploy Uima AS Primitive Service
	    deployService(uimaAsEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");

	    // Nest the placeholders in the broker & endpoint strings
	    String url = broker.getMasterConnectorURI();
	    System.setProperty( "defaultBrokerURL", url.substring(2,url.length()-2));
	    String brokerUrl = url.substring(0,2) + "${defaultBrokerURL}" + url.substring(url.length()-2);	    
	    System.setProperty( "PersonTitleEndpoint", "TitleAnnotator");
	    String endpoint = "Person${PersonTitleEndpoint}Queue";  // "PersonTitleAnnotatorQueue"
	    Map<String, Object> appCtx = buildContext(brokerUrl, endpoint);

	    initialize(uimaAsEngine, appCtx);
	    waitUntilInitialized();
	    for (int i = 0; i < 10; i++) {
	      CAS cas = uimaAsEngine.getCAS();
	      cas.setDocumentText("Some Text");
	      System.out.println("UIMA AS Client Sending CAS#" + (i + 1) + " Request to a Service");
	      uimaAsEngine.sendCAS(cas);
	    }
	    uimaAsEngine.collectionProcessingComplete();
	    uimaAsEngine.stop();
	    
	  }
  
  public void testClientCpcTimeout() throws Exception {
    System.out.println("-------------- testClientCpcTimeout -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    addExceptionToignore(org.apache.uima.aae.error.UimaASCollectionProcessCompleteTimeout.class);

    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithCpCDelay.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueue");
    // Set an explicit CPC timeout as exceptions thrown in the 2nd annotator's CPC don't reach the
    // client.
    appCtx.put(UimaAsynchronousEngine.CpcTimeout, 2000);
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueue", 1, CPC_LATCH); // PC_LATCH);
  }

  /**
   * Tests handling of multiple calls to initialize(). A subsequent call to initialize should result
   * in ResourceInitializationException.
   * 
   * @throws Exception
   */
  public void testInvalidInitializeCall() throws Exception {
    System.out.println("-------------- testInvalidInitializeCall -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();

    deployService(eeUimaEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue");

    try {
      initialize(eeUimaEngine, appCtx);
      waitUntilInitialized();
      System.out.println("First Initialize Call Completed");
      eeUimaEngine.initialize(appCtx);
      fail("Subsequent call to initialize() did not return expected exception:"
              + UIMA_IllegalStateException.class
              + " Subsequent call to initialize succeeded with no error");
    } catch (ResourceInitializationException e) {
      if (e.getCause() != null && !(e.getCause() instanceof UIMA_IllegalStateException)) {
        fail("Invalid Exception Thrown. Expected:" + UIMA_IllegalStateException.class
                + " Received:" + e.getClass());
      } else {
        System.out.println("Received Expected Exception:" + UIMA_IllegalStateException.class);
      }
    } catch (ServiceShutdownException e) {
      // expected
    } finally {
      eeUimaEngine.stop();
    }
  }

  /**
   * Tests deployment of a primitive Uima-AS Service (PersontTitleAnnotator). Deploys the primitive
   * in the same jvm using Uima-AS Client API and blocks on a monitor until the Uima Client calls
   * initializationComplete() method. Once the primitive service starts it is expected to send its
   * metadata to the Uima client which in turn notifies this object with a call to
   * initializationComplete() where the monitor is signaled to unblock the thread. This code will
   * block if the Uima Client does not call initializationComplete()
   * 
   * @throws Exception
   */
  public void testDeployPrimitiveService() throws Exception {
    System.out.println("-------------- testDeployPrimitiveService -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue", 0, EXCEPTION_LATCH);
  }
  public void testDeployPrimitiveServiceWithInitFailure() throws Exception {
	    System.out.println("-------------- testDeployPrimitiveServiceWithInitFailure -------------");
	    // Instantiate Uima-AS Client
	    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
	    // Deploy Uima-AS Primitive Service
	    try {
		    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithInitException.xml");
	    } catch( ResourceInitializationException e) {
	    	System.out.println("Received Expected ResourceInitializationException On Service Deploy");
		    return;
	    }
	    fail("Expected ResourceInitializationException Not Thrown from deployed Service.");
  }
  
  public void testTypeSystemMerge() throws Exception {
    System.out.println("-------------- testTypeSystemMerge -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath+ "/Deploy_GovernmentOfficialRecognizer.xml");
    deployService(eeUimaEngine, relativePath+ "/Deploy_NamesAndPersonTitlesRecognizer.xml");
    deployService(eeUimaEngine, relativePath+ "/Deploy_TokenSentenceRecognizer.xml");
    deployService(eeUimaEngine, relativePath+ "/Deploy_AggregateToTestTSMerge.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
    "TopLevelTaeQueue");
    
    try {
      initialize(eeUimaEngine, appCtx);
      waitUntilInitialized();
      //  Check if the type system returned from the service contains
      //  expected types
      CAS cas = eeUimaEngine.getCAS();
      TypeSystem ts = cas.getTypeSystem();
      //  "example.EmailsAddress" type was 'contributed' by the Flow Controller
      if ( ts.getType("example.EmailAddress") == null ) {
        fail("Incomplete Type system. Expected Type 'example.EmailAddress' missing from the CAS type system");
      } else if ( ts.getType("example.GovernmentOfficial") == null) {
        fail("Incomplete Type system. Expected Type 'example.GovernmentOfficial' missing from the CAS type system");
      } else if ( ts.getType("example.Name") == null) {
        fail("Incomplete Type system. Expected Type 'example.Name' missing from the CAS type system");
      } else if ( ts.getType("example.PersonTitle") == null) {
        fail("Incomplete Type system. Expected Type 'example.PersonTitle' missing from the CAS type system");
      } else if ( ts.getType("example.PersonTitleKind") == null) {
        fail("Incomplete Type system. Expected Type 'example.PersonTitleKind' missing from the CAS type system");
      } else if ( ts.getType("org.apache.uima.examples.tokenizer.Sentence") == null) {
        fail("Incomplete Type system. Expected Type 'org.apache.uima.examples.tokenizer.Sentence' missing from the CAS type system");
      } else if ( ts.getType("org.apache.uima.examples.tokenizer.Token") == null) {
        fail("Incomplete Type system. Expected Type 'org.apache.uima.examples.tokenizer.Token' missing from the CAS type system");
      } 
      
    } catch (ResourceInitializationException e) {
        fail("Initialization Exception");
    } catch (Exception e) {
    } finally {
      eeUimaEngine.stop();
    }
  }

  /**
   * Tests detection of misconfiguration between deployement descriptor and AE descriptor.
   * The AE descriptor is configured to allow one instance of the AE while the deployment
   * descriptor attempts to scale out the service containing the AE. ResourceInitializationException
   * is expected.
   * 
   * @throws Exception
   */
  public void testMultiInstanceDeployFailureInPrimitiveService() throws Exception {
    System.out.println("-------------- testMultiInstanceDeployFailureInPrimitiveService -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    try {
      // Deploy Uima-AS Primitive Service
      deployService(eeUimaEngine, relativePath + "/Deploy_ScaledPersonTitleAnnotator.xml");
      fail("Expected ResourceInitializationException Due to Misconfiguration but instead the service initialized successfully");
    } catch ( ResourceInitializationException e) {
      // expected
    }
  }
  /**
   * Tests processing of an Exception that a service reports on CPC
   * 
   * @throws Exception
   */
  public void testDeployAggregateWithDelegateCpCException() throws Exception {
    System.out.println("-------------- testDeployAggregateWithDelegateCpCException -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithCpCException.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotator.xml");
    addExceptionToignore(org.apache.uima.aae.error.UimaEEServiceException.class);

    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    initialize(eeUimaEngine, appCtx);
    waitUntilInitialized();
    System.out.println("Client Initialized");
    CAS cas = eeUimaEngine.getCAS();
    for (int i = 0; i < 3; i++) {
      eeUimaEngine.sendAndReceiveCAS(cas);
      cas.reset();
    }
    System.out.println("Client Sending CPC");

    // Send CPC. The service should recreate a session and send CPC reply
    eeUimaEngine.collectionProcessingComplete();

    // Now send some CASes and sleep to let the inactivity timer pop again
    for (int i = 0; i < 3; i++) {
      eeUimaEngine.sendAndReceiveCAS(cas); // This will start a timer on reply queue
      cas.reset();
    }
    // Send another CPC
    eeUimaEngine.collectionProcessingComplete();

    eeUimaEngine.stop();

  }

  public void testDeployPrimitiveServiceWithCpCException() throws Exception {
    System.out.println("-------------- testDeployPrimitiveServiceWithCpCException -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithCpCException.xml");
    // Add expected exception so that we release CPC Latch
    addExceptionToignore(org.apache.uima.aae.error.UimaEEServiceException.class);

    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueue", 1, PROCESS_LATCH);
  }

  /**
   * Tests sending CPC request from a client that does not send CASes to a service
   * 
   * @throws Exception
   */
  public void testCpCWithNoCASesSent() throws Exception {
    System.out.println("-------------- testCpCWithNoCASesSent -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(uimaAsEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue");
    initialize(uimaAsEngine, appCtx);
    waitUntilInitialized();

    for (int i = 0; i < 10; i++) {
      System.out.println("UIMA AS Client Sending CPC Request to a Service");
      uimaAsEngine.collectionProcessingComplete();
    }
    uimaAsEngine.stop();
  }

  /**
   * Tests inactivity timeout on a reply queue. The service stops a session after 5 seconds of
   * sending GetMeta reply. The client than waits for 10 seconds and sends a CPC.
   * 
   * @throws Exception
   */
  public void testServiceInactivityTimeoutOnReplyQueue() throws Exception {
    System.out.println("-------------- testServiceInactivityTimeoutOnReplyQueue -------------");
    String sessionTimeoutOverride = System.getProperty("SessionTimeoutOverride");
    System.setProperty("SessionTimeoutOverride", "5000");

    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueue");
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 1000);
    initialize(eeUimaEngine, appCtx);
    waitUntilInitialized();
    System.out.println("Client Initialized");

    CAS cas = eeUimaEngine.getCAS();
    eeUimaEngine.sendAndReceiveCAS(cas); // This will start a timer on reply queue
    cas.reset();
    // Now sleep for 8 seconds to let the service timeout on its reply queue due
    // to a 5 second inactivity timeout
    Thread.currentThread().sleep(8000);
    System.out.println("Client Sending CPC");

    // Send CPC. The service should recreate a session and send CPC reply
    eeUimaEngine.collectionProcessingComplete();
    // Now send some CASes and sleep to let the inactivity timer pop again
    for (int i = 0; i < 5; i++) {
      eeUimaEngine.sendAndReceiveCAS(cas); // This will start a timer on reply queue
      cas.reset();
      if (i == 3) {
        Thread.currentThread().sleep(8000);
      }
    }
    // Send another CPC
    eeUimaEngine.collectionProcessingComplete();

    eeUimaEngine.stop();

    // Reset inactivity to original value or remove if it was not set
    if (sessionTimeoutOverride != null) {
      System.setProperty("SessionTimeoutOverride", sessionTimeoutOverride);
    } else {
      System.clearProperty("SessionTimeoutOverride");
    }
  }

  /**
   * Tests handling of ResourceInitializationException that happens in a collocated primitive
   * 
   * @throws Exception
   */
  public void testDeployAggregateServiceWithFailingCollocatedComponent() throws Exception {
    System.out
            .println("-------------- testDeployAggregateServiceWithFailingCollocatedComponent -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    try {
      deployService(eeUimaEngine, relativePath
              + "/Deploy_AggregateWithFailingCollocatedDelegate.xml");
    } catch (ResourceInitializationException e) {
      // This is expected
    } catch (Exception e) {
      e.printStackTrace();
      fail("Expected ResourceInitializationException Instead Caught:" + e.getClass().getName());
    }
  }
  public void testClientProcessTimeoutWithAggregateMultiplier() throws Exception {
    System.out.println("-------------- testClientProcessTimeoutWithAggregateMultiplier -------------");
    addExceptionToignore(org.apache.uima.aae.error.UimaASProcessCasTimeout.class);

    BaseUIMAAsynchronousEngine_impl uimaAsEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(uimaAsEngine, relativePath + "/Deploy_AggregateMultiplierWithDelay.xml");

    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    appCtx.put(UimaAsynchronousEngine.Timeout, 3000);
    appCtx.put(UimaAsynchronousEngine.CasPoolSize, 1);

    // reduce the cas pool size and reply window
    appCtx.remove(UimaAsynchronousEngine.ShadowCasPoolSize);
    appCtx.put(UimaAsynchronousEngine.ShadowCasPoolSize, Integer.valueOf(1));
    
    
    initialize(uimaAsEngine, appCtx);
    waitUntilInitialized();
    
    for( int i=0; i < 2; i++ ) {
      CAS cas = uimaAsEngine.getCAS();
      cas.setDocumentText("Some Text");
      uimaAsEngine.sendCAS(cas);  // will timeout after 5 secs
      uimaAsEngine.collectionProcessingComplete();  // the CPC should not
      // be sent to a service until the timeout occurs.
    }
    uimaAsEngine.stop();
  }

  public void testDeployAggregateService() throws Exception {
    System.out.println("-------------- testDeployAggregateService -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotator.xml");
    
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    appCtx.put(UimaAsynchronousEngine.Timeout, 0);
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 0);
    
    addExceptionToignore(org.apache.uima.aae.error.UimaEEServiceException.class); 
    
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            10, PROCESS_LATCH);
  }
  /**
   * Sends total of 10 CASes to async aggregate configured to process 2 CASes at a time.
   * The inner NoOp annotator is configured to sleep for 5 seconds. The client should
   * be receiving 2 ACKs simultaneously confirming that the aggregate is processing 2 
   * input CASes at the same time.
   * 
   * @throws Exception
   */
  public void testDeployAggregateServiceWithScaledInnerNoOp() throws Exception {
	    System.out.println("-------------- testDeployAggregateServiceWithScaledInnerNoOp -------------");
	    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
	    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");
	    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithScaledInnerNoOp.xml");
	    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
	            "TopLevelTaeQueue");
	    appCtx.put(UimaAsynchronousEngine.Timeout, 0);
	    appCtx.put(UimaAsynchronousEngine.CasPoolSize, 5);
	    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 0);
	    
	    addExceptionToignore(org.apache.uima.aae.error.UimaEEServiceException.class); 
	    
	    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
	            10, PROCESS_LATCH);
	  }
  public void testDeployAggregateServiceWithDelegateTimeoutAndContinueOnError() throws Exception {
    System.out.println("-------------- testDeployAggregateServiceWithDelegateTimeoutAndContinueOnError -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithDelegateTimeoutAndContinueOnError.xml");

    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),"TopLevelTaeQueue");
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }
  
  public void testScaledSyncAggregateProcess() throws Exception {
    System.out.println("-------------- testScaledSyncAggregateProcess -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_ScaledPrimitiveAggregateAnnotator.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            5, PROCESS_LATCH);
  }

  public void testAggregateWithFailedRemoteDelegate() throws Exception {
    System.out.println("-------------- testAggregateWithFailedRemoteDelegate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithExceptionOn5thCAS.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithFailedRemoteDelegate.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }


  public void testCMAggregateClientStopRequest() throws Exception {
    System.out.println("-------------- testCMAggregateClientStopRequest -------------");
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_CMAggregateWithCollocated1MillionDocsCM.xml");
    superRef = this;

    Thread t = new Thread() {
      public void run() {
        try {
          // Wait for some CASes to return from the service
          while (superRef.getNumberOfCASesProcessed() == 0) {
            // No reply received yet so wait for 1 second and
            // check again
            synchronized (this) {
              this.wait(1000); // wait for 1 sec
            }
          }
          // The client received at least one reply, wait
          // at this point the top level service should show a connection error
          synchronized (this) {
            // wait for 3 seconds before stopping
            this.wait(5000);
          }
          eeUimaEngine.stopProducingCases();
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    };
    t.start();
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testCMAggregateClientStopRequest2() throws Exception {
    System.out.println("-------------- testCMAggregateClientStopRequest2 -------------");
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith1MillionDocs.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith1MillionDocs.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_CMAggregateWithRemote1MillionDocsCM.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_CMAggregateWithRemote1MillionDocsCM.xml");
    superRef = this;
    Thread t = new Thread() {
      public void run() {
        try {
          // Wait for some CASes to return from the service
          while (superRef.getNumberOfCASesProcessed() == 0) {
            // No reply received yet so wait for 1 second and
            // check again
            synchronized (this) {
              this.wait(1000); // wait for 1 sec
            }
          }
          // The client received at least one reply, wait
          // at this point the top level service should show a connection error
          synchronized (this) {
            // wait for 3 seconds before stopping
            this.wait(3000);
          }
          eeUimaEngine.stopProducingCases();
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }
    };
    t.start();
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testAggregateCMWithFailedRemoteDelegate() throws Exception {
    System.out.println("-------------- testAggregateCMWithFailedRemoteDelegate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithExceptionOn5thCAS.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateCMWithFailedRemoteDelegate.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }

  public void testAggregateCMWithFailedCollocatedDelegate() throws Exception {
    System.out.println("-------------- testAggregateCMWithFailedCollocatedDelegate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateCMWithFailedCollocatedDelegate.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }

  public void testComplexAggregateCMWithFailedCollocatedDelegate() throws Exception {
    System.out
            .println("-------------- testComplexAggregateCMWithFailedCollocatedDelegate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath
            + "/Deploy_ComplexAggregateWithFailingInnerAggregateCM.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }

  public void testAggregateCMWithRemoteCMAndFailedRemoteDelegate() throws Exception {
    System.out
            .println("-------------- testAggregateCMWithRemoteCMAndFailedRemoteDelegate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithExceptionOn5thCAS.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith1MillionDocs.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateCMWithRemoteCMAndFailedRemoteDelegate.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }

  /**
   * Tests a simple Aggregate with one remote Delegate and collocated Cas Multiplier
   * 
   * @throws Exception
   */
  public void testDeployAggregateServiceWithBrokerPlaceholder() throws Exception {
    System.out
            .println("-------------- testDeployAggregateServiceWithBrokerPlaceholder -------------");
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");

    try {
      Thread t = new Thread() {
        public void run() {
          BrokerService bs = null;
          try {
            // at this point the top level service should show a connection error
            synchronized (this) {
              this.wait(5000); // wait for 5 secs
            }
            // Create a new broker that runs a different port that the rest of testcases
            bs = setupSecondaryBroker(false);
            System.setProperty("AggregateBroker", bs.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());
            System.setProperty("NoOpBroker", bs.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());
            deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorUsingPlaceholder.xml");
            deployService(eeUimaEngine, relativePath
                    + "/Deploy_AggregateAnnotatorUsingPlaceholder.xml");
            // Start the uima AS client. It connects to the top level service and sends
            // 10 messages
            runTest(null, eeUimaEngine, System.getProperty("AggregateBroker"), "TopLevelTaeQueue",
                    1, PROCESS_LATCH);
          } catch (InterruptedException e) {
          } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
          } finally {
            if (bs != null) {
              try {
                bs.stop();
                bs.waitUntilStopped();
               
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }
        }
      };
      t.start();
      t.join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Tests missing broker on service startup. The service listener on input queue recovers from this
   * by silently attempting reconnect at 5 second intervals. The test first launches the service,
   * than spins a thread where a new broker is created after 5 seconds, and finally the uima as
   * client is started. The test shows initial connection failure and when the broker becomes
   * available the connection is established and messages begin to flow from the client to the
   * service and back.
   * 
   * @throws Exception
   */
  public void testDelayedBrokerWithAggregateService() throws Exception {
    System.out.println("-------------- testDelayedBrokerWithAggregateService -------------");
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    BrokerService bs = null;
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    // set up and start secondary broker. It is started only to detect an open port so that we
    // define SecondaryBrokerURL property. This property is used to resolve a placeholder
    // in the aggregate descriptor. Once the property is set we shutdown the secondary broker to
    // test aggregate recovery from missing broker. Hopefully the same port is still open when
    // the test starts the secondary broker for the second time. 
    bs = setupSecondaryBroker(false);
    System.setProperty("SecondaryBrokerURL",bs.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());
    bs.stop();
    
    // wait for the broker to stop
    bs.waitUntilStopped();
    // Deploy aggregate on a secondary broker which was shutdown above. The aggregate should 
    // detect missing broker and silently wait for the broker to come up
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorOnSecondaryBroker.xml");
    try {
      // spin a thread to restart a broker after 5 seconds
      Thread t = new Thread() {
        public void run() {
          BrokerService bs = null;
          try {
            // at this point the top level service should show a connection error
            synchronized (this) {
              this.wait(5000); // wait for 5 secs
            }
            // Create a new broker on port 8119
            bs = setupSecondaryBroker(false);
            bs.waitUntilStarted();
            System.setProperty("SecondaryBrokerURL",bs.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());
            // Start the uima AS client. It connects to the top level service and sends
            // 10 messages
            runTest(null, eeUimaEngine, bs.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(), "TopLevelTaeQueue", 10,
                    PROCESS_LATCH);
          } catch (InterruptedException e) {
          } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
          } finally {
            if (bs != null) {
              try {
                bs.stop();
                bs.waitUntilStopped();

              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }
        }
      };
      t.start();
      t.join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Tests a simple Aggregate with one remote Delegate and collocated Cas Multiplier
   * 
   * @throws Exception
   */
  public void testDeployAggregateServiceWithTempReplyQueue() throws Exception {
    System.out.println("-------------- testDeployAggregateServiceWithTempReplyQueue -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateUsingRemoteTempQueue.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);

  }

  /**
   * Tests a simple Aggregate with one remote Delegate and collocated Cas Multiplier
   * 
   * @throws Exception
   */
  public void testProcessAggregateServiceWith1000Docs() throws Exception {
    System.out.println("-------------- testProcessAggregateServiceWith1000Docs -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateAnnotatorWithInternalCM1000Docs.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);

  }

  public void testProcessAggregateWithInnerAggregateCM() throws Exception {
    System.out.println("-------------- testProcessAggregateWithInnerAggregateCM() -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");
    deployService(eeUimaEngine, relativePath + "/Deploy_ComplexAggregateWithInnerAggregateCM.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }
  public void testAggregateWithInnerSynchAggregateCM() throws Exception {
	    System.out.println("-------------- testAggregateWithInnerSynchAggregateCM() -------------");
	    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
	    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");
	    deployService(eeUimaEngine, relativePath + "/Deploy_ComplexAggregateWithInnerUimaAggregateCM.xml");
	    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
	            10, PROCESS_LATCH);
	  }

  /**
   * Tests exception thrown in the Uima-AS Client when the Collection Reader is added after the uima
   * ee client is initialized
   * 
   * @throws Exception
   */
  public void testExceptionOnPostInitializeCollectionReaderInjection() throws Exception {
    System.out
            .println("-------------- testExceptionOnPostInitializeCollectionReaderInjection -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue");
    initialize(eeUimaEngine, appCtx);
    waitUntilInitialized();
    try {
      // Simulate plugging in a Collection Reader. This should throw
      // ResourceInitializationException since the client code has
      // been already initialized.
      eeUimaEngine.setCollectionReader(null);
    } catch (ResourceInitializationException e) {
      System.out.println("Received Expected Exception:" + ResourceInitializationException.class);
      // Expected
      return;
    } catch (Exception e) {
      fail("Invalid Exception Thrown. Expected:" + ResourceInitializationException.class
              + " Received:" + e.getClass());
    } finally {
      eeUimaEngine.stop();
    }
    fail("Expected" + ResourceInitializationException.class);
  }

  /**
   * Tests the shutdown due to a failure in the Flow Controller while diabling a delegate
   * 
   * @throws Exception
   */
  public void testTerminateOnFlowControllerExceptionOnDisable() throws Exception {
    System.out
            .println("-------------- testTerminateOnFlowControllerExceptionOnDisable -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateWithFlowControllerExceptionOnDisable.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH); 
  }

  /**
   * Tests the shutdown due to a failure in the Flow Controller when initializing
   * 
   * @throws Exception
   */
  public void testTerminateOnFlowControllerExceptionOnInitialization() throws Exception {
    System.out
            .println("-------------- testTerminateOnFlowControllerExceptionOnInitialization -------------");

    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    String[] containerIds = new String[2];
    try {
      containerIds[0] = deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
      containerIds[1] = deployService(eeUimaEngine, relativePath
              + "/Deploy_AggregateWithFlowControllerExceptionOnInitialization.xml");
      fail("Expected ResourceInitializationException. Instead, the Aggregate Deployed Successfully");
    } catch (ResourceInitializationException e) {
      Exception cause = getCause(e);
      System.out.println("\nExpected Initialization Exception was received:" + cause);
    } catch (Exception e) {
      fail("Expected ResourceInitializationException. Instead Got:" + e.getClass());
    } finally {
      eeUimaEngine.undeploy(containerIds[0]);
      eeUimaEngine.undeploy(containerIds[1]);
    }

  }

  /**
   * Tests the shutdown due to a failure in the Flow Controller when initializing AND have delegates
   * to disable (Jira issue UIMA-1171)
   * 
   * @throws Exception
   */
  public void testTerminateOnFlowControllerExceptionOnInitializationWithDisabledDelegates()
          throws Exception {
    System.out
            .println("-------------- testTerminateOnFlowControllerExceptionOnInitializationWithDisabledDelegates -----");

    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    String containerId = null;
    try {
      containerId = deployService(eeUimaEngine, relativePath
              + "/Deploy_AggregateWithFlowControllerExceptionOnInitialization.xml");
      fail("Expected ResourceInitializationException. Instead, the Aggregate Deployed Successfully");
    } catch (ResourceInitializationException e) {
      Exception cause = getCause(e);
      System.out.println("\nExpected Initialization Exception was received - cause: " + cause);
    } catch (Exception e) {
      fail("Expected ResourceInitializationException. Instead Got:" + e.getClass());
    } finally {
      eeUimaEngine.undeploy(containerId);
    }
  }

  /**
   * Deploys a Primitive Uima-AS service and sends 5 CASes to it.
   * 
   * @throws Exception
   */

  public void testPrimitiveServiceProcess() throws Exception {
    System.out.println("-------------- testPrimitiveServiceProcess -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue", 5, PROCESS_LATCH);
  }

  /**
   * Deploys a Primitive Uima-AS service and sends 5 CASes to it.
   * 
   * @throws Exception
   */

  public void testSyncAggregateProcess() throws Exception {
    System.out.println("-------------- testSyncAggregateProcess -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_MeetingDetectorAggregate.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "MeetingDetectorQueue", 5, PROCESS_LATCH);
  }

  /**
   * Deploys a Primitive Uima-AS service and sends 5 CASes to it.
   * 
   * @throws Exception
   */

  public void testPrimitiveServiceProcessPingFailure() throws Exception {
    System.out.println("-------------- testPrimitiveServiceProcessPingFailure -------------");
    // Instantiate Uima-AS Client
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    final String containerID = deployService(eeUimaEngine, relativePath
            + "/Deploy_PersonTitleAnnotator.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue");
    // Set an explicit getMeta (Ping)timeout
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 2000);
    // Set an explicit process timeout so to test the ping on timeout
    appCtx.put(UimaAsynchronousEngine.Timeout, 1000);
    // Spin a thread and wait for awhile before killing the remote service.
    // This will cause the client to timeout waiting for a CAS reply and
    // to send a Ping message to test service availability. The Ping times
    // out and causes the client API to stop.
    new Thread() {
      public void run() {
        Object mux = new Object();
        synchronized (mux) {
          try {
            mux.wait(1000);
            // Undeploy service container
            System.out.println("** About to undeploy PersonTitle service");
            eeUimaEngine.undeploy(containerID);
          } catch (Exception e) {
          }
        }
      }
    }.start();

    super.countPingRetries=true;
    
    try {
      runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
              "PersonTitleAnnotatorQueue", 500, EXCEPTION_LATCH);
    } catch (RuntimeException e) {
      System.out.println(">>> runtest generated exception: " + e);
      e.printStackTrace(System.out);
    }
    super.countPingRetries=false;

  }

  /**
   * Tests error handling on delegate timeout. The Delegate is started as remote, the aggregate
   * initializes and the client starts sending CASes. After a short while the client kills the
   * remote delegate. The aggregate receives a CAS timeout and disables the delegate. A timed out
   * CAS is sent to the next delegate in the pipeline. ALL 1000 CASes are returned to the client.
   * 
   * @throws Exception
   */
  public void testDelegateTimeoutAndDisable() throws Exception {
    System.out.println("-------------- testDelegateTimeoutAndDisable -------------");
    // Instantiate Uima-AS Client
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    final String containerID = deployService(eeUimaEngine, relativePath
            + "/Deploy_RoomNumberAnnotator.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_MeetingDetectorTAE_RemoteRoomNumberDisable.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "MeetingDetectorTaeQueue");
    // Set an explicit getMeta (Ping)timeout
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 2000);
    // Set an explicit process timeout so to test the ping on timeout
    appCtx.put(UimaAsynchronousEngine.Timeout, 1000);
    // Spin a thread and wait for awhile before killing the remote service.
    // This will cause the client to timeout waiting for a CAS reply and
    // to send a Ping message to test service availability. The Ping times
    // out and causes the client API to stop.

    new Thread() {
      public void run() {
        Object mux = new Object();
        synchronized (mux) {
          try {
            mux.wait(500);
            // Undeploy service container
            eeUimaEngine.undeploy(containerID);
          } catch (Exception e) {
          }
        }
      }
    }.start();

    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "MeetingDetectorTaeQueue", 1000, PROCESS_LATCH);

  }

  /**
   * This test kills a remote Delegate while in the middle of processing 1000 CASes. The CAS timeout
   * error handling disables the delegate and forces ALL CASes from the Pending Reply List to go
   * through Error Handler. The Flow Controller is configured to continueOnError and CASes that
   * timed out are allowed to continue to the next delegate. ALL 1000 CASes are accounted for in the
   * NoOp Annotator that is last in the flow.
   * 
   * @throws Exception
   */
  public void testDisableDelegateOnTimeoutWithCM() throws Exception {
    System.out.println("-------------- testDisableDelegateOnTimeoutWithCM -------------");
    // Instantiate Uima-AS Client
    final BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    final String containerID = deployService(eeUimaEngine, relativePath
            + "/Deploy_RoomNumberAnnotator.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_MeetingDetectorTAEWithCM_RemoteRoomNumberDisable.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "MeetingDetectorTaeQueue");
    // Set an explicit getMeta (Ping)timeout
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 2000);
    // Set an explicit process timeout so to test the ping on timeout
    appCtx.put(UimaAsynchronousEngine.Timeout, 1000);
    // Spin a thread and wait for awhile before killing the remote service.
    // This will cause the client to timeout waiting for a CAS reply and
    // to send a Ping message to test service availability. The Ping times
    // out and causes the client API to stop.

    new Thread() {
      public void run() {
        Object mux = new Object();
        synchronized (mux) {
          try {
            mux.wait(300);
            // Undeploy service container
            eeUimaEngine.undeploy(containerID);
          } catch (Exception e) {
          }
        }
      }
    }.start();

    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "MeetingDetectorTaeQueue", 1, PROCESS_LATCH);

  }

  /**
   * Tests Uima-AS client ability to test sendAndReceive in multiple/concurrent threads It spawns 4
   * thread each sending 100 CASes to a Primitive Uima-AS service
   * 
   * @throws Exception
   */
  public void testSynchCallProcessWithMultipleThreads() throws Exception {
    System.out.println("-------------- testSynchCallProcessWithMultipleThreads -------------");
    int howManyCASesPerRunningThread = 100;
    int howManyRunningThreads = 4;
    runTestWithMultipleThreads(relativePath + "/Deploy_PersonTitleAnnotator.xml",
            "PersonTitleAnnotatorQueue", howManyCASesPerRunningThread, howManyRunningThreads, 0, 0);
  }

  /**
   * 
   * @throws Exception
   */
  public void testPrimitiveProcessCallWithLongDelay() throws Exception {
    System.out.println("-------------- testPrimitiveProcessCallWithLongDelay -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
    // We expect 18000ms to be spent in process method
    super.setExpectedProcessTime(6000);

    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueueLongDelay");
    appCtx.remove(UimaAsynchronousEngine.ReplyWindow);
    appCtx.put(UimaAsynchronousEngine.ReplyWindow, 1);
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueueLongDelay", 4, PROCESS_LATCH, true);
  }

  /**
   * Tests time spent in process CAS. The CAS is sent to three remote delegates each with a delay of
   * 6000ms in the process method. The aggregate is expected to sum up the time spent in each
   * annotator process method. The final sum is returned to the client (the test) and compared
   * against expected 18000ms. The test actually allows for 20ms margin to account for any overhead
   * (garbage collecting, slow cpu, etc)
   * 
   * @throws Exception
   */
  public void testAggregateProcessCallWithLongDelay() throws Exception {

    System.out.println("-------------- testAggregateProcessCallWithLongDelay -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Services each with 6000ms delay in process()
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorAWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorBWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorCWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithLongDelay.xml");
    // We expect 18000ms to be spent in process method
    super.setExpectedProcessTime(18000);
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    appCtx.remove(UimaAsynchronousEngine.ReplyWindow);
    // make sure we only send 1 CAS at a time
    appCtx.put(UimaAsynchronousEngine.ReplyWindow, 1);
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue", 1, PROCESS_LATCH, true);
  }

  /**
   * Tests Aggregate configuration where the Cas Multiplier delegate is the last delegate in the
   * Aggregate's pipeline
   * 
   * @throws Exception
   */
  public void testAggregateProcessCallWithLastCM() throws Exception {
    System.out.println("-------------- testAggregateProcessCallWithLastCM -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima-AS Primitive Services each with 6000ms delay in process()
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithLastCM.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH, true);
  }

  /**
   * Tests shutdown while running with multiple/concurrent threads The Annotator throws an exception
   * and the Aggregate error handling is setup to terminate on the first error.
   * 
   * @throws Exception
   */

  public void testTimeoutInSynchCallProcessWithMultipleThreads() throws Exception {
    System.out
            .println("-------------- testTimeoutInSynchCallProcessWithMultipleThreads -------------");
    int howManyCASesPerRunningThread = 2;
    int howManyRunningThreads = 4;
    int processTimeout = 2000;
    int getMetaTimeout = 500;
    runTestWithMultipleThreads(relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml",
            "NoOpAnnotatorQueueLongDelay", howManyCASesPerRunningThread, howManyRunningThreads,
            processTimeout, getMetaTimeout);
  }

  /**
   * Tests shutdown while running with multiple/concurrent threads The Annotator throws an exception
   * and the Aggregate error handling is setup to terminate on the first error.
   * 
   * @throws Exception
   */

  public void testTimeoutFailureInSynchCallProcessWithMultipleThreads() throws Exception {
    System.out
            .println("-------------- testTimeoutFailureInSynchCallProcessWithMultipleThreads -------------");
    int howManyCASesPerRunningThread = 1000;
    int howManyRunningThreads = 4;
    int processTimeout = 2000;
    int getMetaTimeout = 500;
    runTestWithMultipleThreads(relativePath + "/Deploy_NoOpAnnotator.xml", "NoOpAnnotatorQueue",
            howManyCASesPerRunningThread, howManyRunningThreads, 2000, 1000, true);

  }

  /**
   * Tests a parallel flow in the Uima-AS aggregate.
   * 
   * @throws Exception
   */
  public void testProcessWithParallelFlow() throws Exception {
    System.out.println("-------------- testProcessWithParallelFlow -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator2.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithParallelFlow.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  /**
   * Tests ability to disable one delegate in parallel flow and continue
   * 
   * @throws Exception
   */
  public void testDisableDelegateInParallelFlow() throws Exception {
    System.out.println("-------------- testDisableDelegateInParallelFlow -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator2.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithParallelFlow.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }
  /**
   * This test launches four remote delegates and an aggregate. Three of the delegates:
   * SlowNoOp1, SlowNoOp2, and NoOp are processing CASes in parallel. SlowNoOp1 and SlowNoOp2
   * are configured with a very long delay that exceeds timeout values specified in the aggregate 
   * deployment descriptor. The SlowNoOp1 times out 3 times and is disabled,
   * SlowNoOp2 times out 4 times and is disabled. The test tests ability
   * of an aggregate to recover from the timeouts and subsequent disable and carry
   * on processing with remaining delegates.  
   * 
   * @throws Exception
   */
  public void testMutlipleDelegateTimeoutsInParallelFlows() throws Exception {
    System.out.println("-------------- testTimeoutDelegateInParallelFlows -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator2.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SlowNoOpAnnotator1.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SlowNoOpAnnotator2.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithSlowParallelDelegates.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    appCtx.put(UimaAsynchronousEngine.Timeout, 30000);

    runTest(appCtx, eeUimaEngine, null, null, 1, PROCESS_LATCH);
  }

  /**
   * 
   * @throws Exception
   */
  public void testTimeoutDelegateInParallelFlows() throws Exception {
    System.out.println("-------------- testTimeoutDelegateInParallelFlows -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithDelay.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator2.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator3.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithParallelFlows.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    // Set an explicit process timeout so one of the 1st parallels is disabled but 2nd parallel flow
    // continues.
    appCtx.put(UimaAsynchronousEngine.Timeout, 200000);

    runTest(appCtx, eeUimaEngine, null, null, 1, PROCESS_LATCH);
  }

  /**
   * Tests Timeout logic
   * 
   * @throws Exception
   */
  public void testRemoteDelegateTimeout() throws Exception {
    System.out.println("-------------- testRemoteDelegateTimeout -------------");
    System.out.println("The Aggregate sends 2 CASes to the NoOp Annotator which");
    System.out.println("delays each CAS for 6000ms. The timeout is set to 4000ms");
    System.out.println("Two CAS retries are expected");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateAnnotatorWithLongDelayDelegate.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    // The Remote NoOp delays each CAS for 6000ms. The Aggregate sends two CASes so adjust
    // client timeout to be just over 12000ms.
    appCtx.put(UimaAsynchronousEngine.Timeout, 19000);

    runTest(appCtx, eeUimaEngine, null, null, 1, PROCESS_LATCH);
  }

  /**
   * Tests Timeout logic
   * 
   * @throws Exception
   */
  public void testDisableOnRemoteDelegatePingTimeout() throws Exception {
    System.out.println("-------------- testDisableOnRemoteDelegatePingTimeout -------------");
    System.out.println("The Aggregate sends 2 CASes to the NoOp Annotator which");
    System.out.println("delays each CAS for 6000ms. The timeout is set to 4000ms");
    System.out.println("Two CAS retries are expected");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    String delegateContainerId = deployService(eeUimaEngine, relativePath
            + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateAnnotatorWithLongDelayDelegate.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    // The Remote NoOp delays each CAS for 6000ms. The Aggregate sends two CASes so adjust
    // client timeout to be just over 12000ms.
    appCtx.put(UimaAsynchronousEngine.Timeout, 13000);
    // Remove container with the remote NoOp delegate so that we can test
    // the CAS Process and Ping timeout.
    eeUimaEngine.undeploy(delegateContainerId);
    // Send the CAS and handle exception
    runTest(appCtx, eeUimaEngine, null, null, 1, EXCEPTION_LATCH);
  }

  public void testDeployAggregateWithCollocatedAggregateService() throws Exception {
    System.out
            .println("-------------- testDeployAggregateWithCollocatedAggregateService -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_ComplexAggregate.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            10, PROCESS_LATCH);
  }

  public void testProcessWithAggregateUsingCollocatedMultiplier() throws Exception {
    System.out
            .println("-------------- testProcessWithAggregateUsingCollocatedMultiplier -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotator.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testExistanceOfParentCasReferenceIdOnChildFailure() throws Exception {
	  System.out
            .println("-------------- testExistanceOfParentCasReferenceIdOnChildFailure -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithDelegateFailure.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
    // When a callback is received to handle the exception on the child CAS, the message should
    // contain an CAS id of the parent. If it does the callback handler will set
    // receivedExpectedParentReferenceId = true
    Assert.assertTrue(super.receivedExpectedParentReferenceId);
  }

  public void testProcessWithAggregateUsingRemoteMultiplier() throws Exception {
    System.out
            .println("-------------- testProcessWithAggregateUsingRemoteMultiplier -------------");
    System.setProperty("BrokerURL", broker.getMasterConnectorURI());

    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplier.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithRemoteMultiplier.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testParentProcessLast() throws Exception {
    System.out
            .println("-------------- testParentProcessLast -------------");
    System.setProperty("BrokerURL", broker.getMasterConnectorURI());

    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith10Docs_1.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithProcessParentLastCMs.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  /**
   * Starts two remote delegates on one broker and a top level client aggregate on 
   * another. Tests sending Free Cas requests to the appropriate broker. 
   * 
   * @throws Exception
   */
  public void testProcessWithAggregateUsingRemoteMultiplierOnSeparateBroker() throws Exception {
    System.out
            .println("-------------- testProcessWithAggregateUsingRemoteMultiplierOnSeparateBroker -------------");
    System.setProperty("activemq.broker.jmx.domain","org.apache.activemq.test");
    BrokerService broker2 = setupSecondaryBroker(true);
    System.setProperty("BrokerURL", broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());

    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplier.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithRemoteMultiplier.xml");
    
    Map<String, Object> appCtx = new HashMap();
    appCtx.put(UimaAsynchronousEngine.ServerUri, broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());
    appCtx.put(UimaAsynchronousEngine.ENDPOINT, "TopLevelTaeQueue");
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 0);
    runTest(appCtx, eeUimaEngine, broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString(),
            "TopLevelTaeQueue", 1, PROCESS_LATCH);    
    super.cleanBroker(broker2);

    broker2.stop();
    broker2.waitUntilStopped();

  }
  
  
  /**
   * First CM feeds 100 CASes to a "merger" CM that generates one output CAS for every 5 input.
   * Second CM creates unique document text that is checked by the last component. The default FC
   * should let 4 childless CASes through, replacing every 5th by its child.
   * 
   * @throws Exception
   */
  public void testProcessWithAggregateUsingCollocatedMerger() throws Exception {
    System.out.println("-------------- testProcessWithAggregateUsingRemoteMerger -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithCollocatedMerger.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testProcessWithAggregateUsingRemoteMerger() throws Exception {
    System.out.println("-------------- testProcessWithAggregateUsingRemoteMerger -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMerger.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithRemoteMerger.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  
  public void testClientWithAggregateMultiplier() throws Exception {
    System.out.println("-------------- testClientWithAggregateMultiplier -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplier.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateMultiplier.xml");

    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");

    // reduce the cas pool size and reply window
    appCtx.remove(UimaAsynchronousEngine.ShadowCasPoolSize);
    appCtx.put(UimaAsynchronousEngine.ShadowCasPoolSize, Integer.valueOf(2));
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue", 1, PROCESS_LATCH);
  }
  public void testClientProcessWithRemoteMultiplier() throws Exception {
    System.out.println("-------------- testClientProcessWithRemoteMultiplier -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplier.xml");

    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TestMultiplierQueue");
    
    appCtx.remove(UimaAsynchronousEngine.ShadowCasPoolSize);
    appCtx.put(UimaAsynchronousEngine.ShadowCasPoolSize, Integer.valueOf(1));
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "TestMultiplierQueue", 1, PROCESS_LATCH);
  }

  public void testClientProcessWithComplexAggregateRemoteMultiplier() throws Exception {

    System.out
            .println("-------------- testClientProcessWithComplexAggregateRemoteMultiplier -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith10Docs_1.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_CasMultiplierAggregateWithRemoteCasMultiplier.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testProcessWithAggregateUsing2RemoteMultipliers() throws Exception {
    System.out
            .println("-------------- testProcessWithAggregateUsing2RemoteMultipliers -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith10Docs_1.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith10Docs_2.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWith2RemoteMultipliers.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testProcessWithAggregateUsing2CollocatedMultipliers() throws Exception {
    System.out
            .println("-------------- testProcessWithAggregateUsing2CollocatedMultipliers -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWith2Multipliers.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testProcessAggregateWithInnerCMAggregate() throws Exception {
    System.out.println("-------------- testProcessAggregateWithInnerCMAggregate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_TopAggregateWithInnerAggregateCM.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }
  public void testProcessAggregateWithInnerAggregateDelegateInitFailure() throws Exception {
    System.out.println("-------------- testProcessAggregateWithInnerAggregateDelegateInitFailure -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    try {
      deployService(eeUimaEngine, relativePath + "/Deploy_TopAggregateWithInnerAggregateDelegateInitFailure.xml");
      fail("Expected ResourceInitializationException But The Deployment Succeeded Instead");
    } catch( ResourceInitializationException e) {
      eeUimaEngine.stop();
    }
  }

  public void testComplexDeployment() throws Exception {
    System.out.println("-------------- testComplexDeployment -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy replicated services for the inner remote aggregate CM
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    // Deploy an instance of a remote aggregate CM containing a collocated Cas Multiplier
    // CM --> Replicated Remote Primitive --> NoOp CC
    deployService(eeUimaEngine, relativePath + "/Deploy_CMAggregateWithCollocatedCM.xml");
    // Deploy top level Aggregate Cas Multiplier with 2 collocated Cas Multipliers
    // CM1 --> CM2 --> Remote AggregateCM --> Candidate Answer --> CC
    deployService(eeUimaEngine, relativePath + "/Deploy_TopLevelComplexAggregateCM.xml");

    runTest2(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue", 10, PROCESS_LATCH);
  }

  public void testTypesystemMergeWithMultiplier() throws Exception {
    System.out.println("-------------- testTypesystemMergeWithMultiplier -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithMergedTypes.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testStopAggregateWithRemoteMultiplier() throws Exception {
    System.out.println("-------------- testStopAggregateWithRemoteMultiplier -------------");
    
    System.setProperty("BrokerURL", broker.getMasterConnectorURI());
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplier.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithExceptionOn5thCAS.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithRemoteMultiplier.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }

  public void testCancelProcessAggregateWithCollocatedMultiplier() throws Exception {
    System.out
            .println("-------------- testCancelProcessAggregateWithCollocatedMultiplier -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_ComplexAggregateWith1MillionDocs.xml");
    // Spin a thread to cancel Process after 20 seconds
    spinShutdownThread(eeUimaEngine, 20000);
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);
  }

  public void testCancelProcessAggregateWithRemoteMultiplier() throws Exception {
    System.out.println("-------------- testStopAggregateWithRemoteMultiplier -------------");
    System.setProperty("BrokerURL", broker.getMasterConnectorURI());
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_RemoteCasMultiplierWith1MillionDocs.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithRemoteMultiplier.xml");
    // Spin a thread to cancel Process after 20 seconds
    spinShutdownThread(eeUimaEngine, 20000);
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH);// EXCEPTION_LATCH);
  }

  /**
   * Test correct reply from the service when its process method fails. Deploys the Primitive
   * Service ( NoOp Annotator) that is configured to throw an exception on every CAS. The expected
   * behavior is for the Primitive Service to return a reply with an Exception. This code blocks on
   * a Count Down Latch, until the exception is returned from the service. When the exception is
   * received the latch is opened indicating success.
   * 
   * @throws Exception
   */
  public void testPrimitiveServiceResponseOnException() throws Exception {
    System.out.println("-------------- testPrimitiveServiceResponseOnException -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy remote service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");
    // Deploy Uima-AS Primitive Service
    // Initialize and run the Test. Wait for a completion and cleanup resources.
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueue", 1, EXCEPTION_LATCH);
  }

  public void testProcessParallelFlowWithDelegateFailure() throws Exception {
    System.out.println("-------------- testProcessParallelFlowWithDelegateFailure -------------");
    // Create Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy remote service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");
    // Deploy remote service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator2.xml");
    // Deploy top level aggregate service
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateWithParallelFlowTerminateOnDelegateFailure.xml");

    // Initialize and run the Test. Wait for a completion and cleanup resources.
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH); // PC_LATCH);
  }

  /**
   * Tests that the thresholdAction is taken when thresholdCount errors occur in the last
   * thresholdWindow CASes. Aggregate has two annotators, first fails with increasing frequency (on
   * CASes 10 19 27 34 40 45 49 52 54) and is disabled after 3 errors in a window of 7 (49,52,54)
   * Second annotator counts the CASes that reach it and verifies that it sees all but the 9
   * failures. It throws an exception if the first is disabled after too many or too few errors.
   * 
   * @throws Exception
   */
  public void testErrorThresholdWindow() throws Exception {
    System.out.println("-------------- testErrorThresholdWindow -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithThresholdWindow.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    // Set an explicit CPC timeout as exceptions thrown in the 2nd annotator's CPC don't reach the
    // client.
    appCtx.put(UimaAsynchronousEngine.CpcTimeout, 20000);
    runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue", 1, PROCESS_LATCH); // PC_LATCH);
  }

  public void testProcessParallelFlowWithDelegateDisable() throws Exception {
    System.out.println("-------------- testProcessParallelFlowWithDelegateDisable -------------");
    // Create Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SimpleAnnotator.xml");
    deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateWithParallelFlowDisableOnDelegateFailure.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, PROCESS_LATCH); // PC_LATCH);
  }

  public void testPrimitiveShutdownOnTooManyErrors() throws Exception {
    System.out.println("-------------- testPrimitiveShutdownOnTooManyErrors -------------");
    // Create Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy remote service
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");
    // Deploy top level aggregate service
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotator.xml");
    // Initialize and run the Test. Wait for a completion and cleanup resources.
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
  }


  /**
   * Tests exception thrown in the Uima-AS Client when the Collection Reader is added after the uima
   * ee client is initialized
   * 
   * @throws Exception
   */

  public void testCollectionReader() throws Exception {
    System.out.println("-------------- testCollectionReader -------------");
    // Instantiate Uima-AS Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue");
    // reduce the cas pool size and reply window
    appCtx.remove(UimaAsynchronousEngine.CasPoolSize);
    appCtx.put(UimaAsynchronousEngine.CasPoolSize, Integer.valueOf(2));
    appCtx.remove(UimaAsynchronousEngine.ReplyWindow);
    appCtx.put(UimaAsynchronousEngine.ReplyWindow, 1);

    // set the collection reader
    String filename = super
            .getFilepathFromClassloader("descriptors/collection_reader/ExtendedTestFileSystemCollectionReader.xml");
    if (filename == null) {
      fail("Unable to find file:" + "descriptors/collection_reader/ExtendedTestFileSystemCollectionReader.xml"
              + "in classloader");
    }
    File collectionReaderDescriptor = new File(filename);
    CollectionReaderDescription collectionReaderDescription = UIMAFramework.getXMLParser()
            .parseCollectionReaderDescription(new XMLInputSource(collectionReaderDescriptor));
    CollectionReader collectionReader = UIMAFramework
            .produceCollectionReader(collectionReaderDescription);
    eeUimaEngine.setCollectionReader(collectionReader);
    initialize(eeUimaEngine, appCtx);
    waitUntilInitialized();
    runCrTest(eeUimaEngine, 7);
    synchronized (this) {
      wait(50);
    }
    eeUimaEngine.stop();
  }

  public void testAsynchronousTerminate() throws Exception {
    System.out.println("-------------- testAsynchronousTerminate -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
    "TopLevelTaeQueue");
    try {
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithDelay.xml");
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator2.xml");
      deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithParallelFlow.xml");
      initialize(eeUimaEngine, appCtx);
      // Wait until the top level service returns its metadata
      waitUntilInitialized();
    } catch( Exception e) {
			throw e;
    }


    CAS cas = eeUimaEngine.getCAS();
    System.out.println(" Sending CAS to kick off aggregate w/colocated CasMultiplier");
    eeUimaEngine.sendCAS(cas);

    System.out.println(" Waiting 1 seconds");
    Thread.sleep(1000);

    System.out.println(" Trying to stop service");
    eeUimaEngine.stop();
    System.out.println(" stop() returned!");
    
  }

  public void testCallbackListenerOnFailure() throws Exception {
    System.out.println("-------------- testCallbackListenerOnFailure -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");

    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "NoOpAnnotatorQueue");
    // Register special callback listener. This listener will receive
    // an exception with the Cas Reference id.
    TestListener listener = new TestListener(this);
    eeUimaEngine.addStatusCallbackListener(listener);
    initialize(eeUimaEngine, appCtx);
    // Wait until the top level service returns its metadata
    waitUntilInitialized();
    CAS cas = eeUimaEngine.getCAS();

    // Send request out and save Cas Reference id
    String casReferenceId = eeUimaEngine.sendCAS(cas);
    // Spin a callback listener thread
    Thread t = new Thread(listener);
    t.start();
    // Wait for reply CAS. This method blocks
    String cRefId = listener.getCasReferenceId();

    try {
      // Test if received Cas Reference Id matches the id of the CAS sent out
      if (!cRefId.equals(casReferenceId)) {
        fail("Received Invalid Cas Reference Id. Expected:" + casReferenceId + " Received: "
                + cRefId);
      } else {
        System.out.println("Received Expected Cas Identifier:" + casReferenceId);
      }
    } finally {
      // Stop callback listener thread
      listener.doStop();
      eeUimaEngine.stop();
    }
  }
  public void testCauseOfInitializationFailure() throws Exception {
	    System.out.println("-------------- testCauseOfInitializationFailure -------------");
	    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
	    try {
	      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithInitException.xml");
	      Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
	              "NoOpAnnotatorQueue");
	      exceptionCountLatch = new CountDownLatch(1);
	      initialize(eeUimaEngine, appCtx);
	      fail("Expected ResourceInitializationException. Instead, the Aggregate Reports Successfull Initialization");
	    } catch (ResourceInitializationException e) {
	      Exception cause = getCause(e);
	      if ( cause != null && (cause instanceof FileNotFoundException) ) {
		      System.out.println("Expected FileNotFoundException was received");
	      } else {
		      fail("Expected FileNotFoundException NOT received as a cause of failure. Instead Got:" + cause);
	      }
	    } catch (Exception e) {
	      fail("Expected ResourceInitializationException. Instead Got:" + e.getClass());
	    } finally {
	      eeUimaEngine.stop();
	    }
	  }

  public void testTerminateOnInitializationFailure() throws Exception {
    System.out.println("-------------- testTerminateOnInitializationFailure -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    try {
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
      deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithParallelFlow.xml");
      Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
              "TopLevelTaeQueue");
      exceptionCountLatch = new CountDownLatch(1);
      initialize(eeUimaEngine, appCtx);
      fail("Expected ResourceInitializationException. Instead, the Aggregate Reports Successfull Initialization");
    } catch (ResourceInitializationException e) {
      Exception cause = getCause(e);
      System.out.println("Expected Initialization Exception was received:" + cause);
    } catch (Exception e) {
      fail("Expected ResourceInitializationException. Instead Got:" + e.getClass());
    } finally {
      eeUimaEngine.stop();
    }
  }

  /**
   * Tests shutdown due to delegate broker missing. The Aggregate is configured to retry getMeta 3
   * times and continue. The client times out after 20 seconds and forces the shutdown. NOTE: The
   * Spring listener tries to recover JMS connection on failure. In this test a Listener to remote
   * delegate cannot be established due to a missing broker. The Listener is setup to retry every 60
   * seconds. After failure, the listener goes to sleep for 60 seconds and tries again. This results
   * in a 60 second delay at the end of this test.
   * 
   * @throws Exception
   */
  public void testTerminateOnInitializationFailureWithDelegateBrokerMissing() throws Exception {
    System.out
            .println("-------------- testTerminateOnInitializationFailureWithDelegateBrokerMissing -------------");
    System.out
            .println("---------------------- The Uima Client Times Out After 20 seconds --------------------------");
    System.out
            .println("-- The test requires 1 minute to complete due to 60 second delay in Spring Listener ----");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    try {
      // Deploy remote service
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
      // Deploy top level aggregate that communicates with the remote via Http Tunnelling
      deployService(eeUimaEngine, relativePath
              + "/Deploy_AggregateAnnotatorTerminateOnDelegateBadBrokerURL.xml");
      // Initialize and run the Test. Wait for a completion and cleanup resources.
      Map<String, Object> appCtx = new HashMap();
      appCtx.put(UimaAsynchronousEngine.ServerUri, String.valueOf(broker.getMasterConnectorURI()));
      appCtx.put(UimaAsynchronousEngine.ENDPOINT, "TopLevelTaeQueue");
      appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 20000);
      runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
              "TopLevelTaeQueue", 1, EXCEPTION_LATCH);
      fail("Expected ResourceInitializationException. Instead, the Aggregate Reports Successfull Initialization");
    } catch (ResourceInitializationException e) {
      Exception cause = getCause(e);
      System.out.println("Expected Initialization Exception was received:" + cause);
    } catch (Exception e) {
      fail("Expected ResourceInitializationException. Instead Got:" + e.getClass());
    } finally {
      eeUimaEngine.stop();
    }
  }

  /**
   * Tests shutdown due to delegate broker missing. The Aggregate is configured to terminate on
   * getMeta timeout.
   * 
   * @throws Exception
   */
  public void testTerminateOnInitializationFailureWithAggregateForcedShutdown() throws Exception {
    System.out
            .println("-------------- testTerminateOnInitializationFailureWithAggregateForcedShutdown -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Initialize and run the Test. Wait for a completion and cleanup resources.
    try {
      // Deploy remote service
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
      // Deploy top level aggregate that communicates with the remote via Http Tunnelling
      deployService(eeUimaEngine, relativePath
              + "/Deploy_AggregateAnnotatorWithHttpDelegateNoRetries.xml");
      runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
              "TopLevelTaeQueue", 10, EXCEPTION_LATCH);
      fail("Expected ResourceInitializationException. Instead, the Aggregate Reports Successfull Initialization");
    } catch (ResourceInitializationException e) {
      Exception cause = getCause(e);
      System.out.println("Expected Initialization Exception was received:" + cause);
    } catch (Exception e) {
      fail("Expected ResourceInitializationException. Instead Got:" + e.getClass());
    } finally {
      eeUimaEngine.stop();
    }

  }


  /**
   * Tests shutdown due to delegate broker missing. The Aggregate is configured to retry getMeta 3
   * times and continue. The client times out after 20 seconds and forces the shutdown. NOTE: The
   * Spring listener tries to recover JMS connection on failure. In this test a Listener to remote
   * delegate cannot be established due to a missing broker. The Listener is setup to retry every 60
   * seconds. After failure, the listener goes to sleep for 60 seconds and tries again. This results
   * in a 60 second delay at the end of this test.
   * 
   * @throws Exception
   */
  public void testDisableOnInitializationFailureWithDelegateBrokerMissing() throws Exception {
    System.out
            .println("-------------- testDisableOnInitializationFailureWithDelegateBrokerMissing() -------------");
    System.out
            .println("---------------------- The Uima Client Times Out After 20 seconds --------------------------");
    System.out
            .println("-- The test requires 1 minute to complete due to 60 second delay in Spring Listener ----");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    try {
      // Deploy remote service
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
      // Deploy top level aggregate that communicates with the remote via Http Tunnelling
      deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotatorWithHttpDelegate.xml");
      // Initialize and run the Test. Wait for a completion and cleanup resources.
      Map<String, Object> appCtx = new HashMap();
      appCtx.put(UimaAsynchronousEngine.ServerUri, String.valueOf(broker.getMasterConnectorURI()));
      appCtx.put(UimaAsynchronousEngine.ENDPOINT, "TopLevelTaeQueue");
      appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 20000);
      runTest(appCtx, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
              "TopLevelTaeQueue", 1, PROCESS_LATCH);
    } catch (Exception e) {
      fail("Expected Success. Instead Received Exception:" + e.getClass());
    } finally {
      eeUimaEngine.stop();
    }
  }

 
  /**
   * This tests some of the error handling. Each annotator writes a file and throws an exception.
   * After the CAS is processed the presence/absence of certain files indicates success or failure.
   * The first annotator fails and lets the CAS proceed, so should write only one file. The second
   * annotator fails and is retried 2 times, and doesn't let the CAS proceed, so should write 3
   * files. The third annotator should not see the CAS, so should not write any files
   * 
   * @throws Exception
   */
  public void testContinueOnRetryFailure() throws Exception {
    System.out.println("-------------- testContinueOnRetryFailure -------------");
    File tempDir = new File("target/temp");
    deleteAllFiles(tempDir);
    try {
        tempDir.mkdir();
    } catch( Exception e) {
    	e.printStackTrace();
    	throw e;
    }
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_WriterAnnotatorA.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_WriterAnnotatorB.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithContinueOnRetryFailures.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);
    if (!(new File(tempDir, "WriterAnnotatorB.3")).exists()
            || (new File(tempDir, "WriterAnnotatorB.4")).exists()) {
      fail("Second annotator should have run 3 times");
    }
    if ((new File(tempDir, "WriterAnnotatorC.1")).exists()) {
      fail("Third annotator should not have seen CAS");
    }
  }

  /**
   * Test use of a JMS Service Adapter. Invoke from a synchronous aggregate to emulate usage from
   * RunAE or RunCPE.
   * 
   * @throws Exception
   */
  public void testJmsServiceAdapter() throws Exception {
    System.out.println("-------------- testJmsServiceAdapter -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SyncAggregateWithJmsService.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            10, PROCESS_LATCH);
  }
  /*
   * Tests Uima AS client placeholder handling and substitution. The Uima Aggregate instantiates
   * UIMA AS client proxy using Jms Client Descriptor that contains a placeholder
   * ${defaultBrokerURL} instead of hard coded Broker URL. The client parses the 
   * placeholder string, retrieves the name (defaultBrokerURL) and uses it to look
   * up tha actual broker URL in System properties.
   */
  public void testJmsServiceAdapterWithPlaceholder() throws Exception {
    System.out.println("-------------- testJmsServiceAdapterWithPlaceholder -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SyncAggregateWithJmsServiceUsingPlaceholder.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            10, PROCESS_LATCH);
  }

  /**
   * Tests use of a JMS Service Adapter and an override of the MultipleDeploymentAllowed. 
   * In this test, the AE descriptor of the remote service is configured with MultipleDeploymentAllowed=false
   * Without the override this causes an exception when instantiating Uima aggregate with
   * MultipleDeploymentAllowed=true. 
   * 
   * @throws Exception
   */
  public void testJmsServiceAdapterWithOverride() throws Exception {
    System.out.println("-------------- testJmsServiceAdapterWithOverride -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_SingleInstancePersonTitleAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SyncAggregateWithJmsServiceAndScaleoutOverride.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            10, PROCESS_LATCH);
  }

  public void testJmsServiceAdapterWithException() throws Exception {
    System.out.println("-------------- testJmsServiceAdapterWithException -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithException.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SyncAggregateWithJmsService.xml");
    Map<String, Object> appCtx = buildContext(String.valueOf(broker.getMasterConnectorURI()),
            "TopLevelTaeQueue");
    appCtx.put(UimaAsynchronousEngine.GetMetaTimeout, 0);
    initialize(eeUimaEngine, appCtx);
    waitUntilInitialized();
    CAS cas = eeUimaEngine.getCAS();
    try {
      eeUimaEngine.sendAndReceiveCAS(cas);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      eeUimaEngine.collectionProcessingComplete();
      cas.reset();
    }
    eeUimaEngine.stop();
  }

  public void testJmsServiceAdapterWithProcessTimeout() throws Exception {
    System.out.println("-------------- testJmsServiceAdapterWithProcessTimeout -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotatorWithLongDelay.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_SyncAggregateWithJmsServiceLongDelay.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            1, EXCEPTION_LATCH);

  }

  public void testJmsServiceAdapterWithGetmetaTimeout() throws Exception {
    System.out.println("-------------- testJmsServiceAdapterWithGetmetaTimeout -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    try {
      deployService(eeUimaEngine, relativePath + "/Deploy_SyncAggregateWithJmsService.xml");
    } catch (ResourceInitializationException e) {
      System.out.println("Received Expected ResourceInitializationException");
      return;
    }
    Assert.fail("Expected ResourceInitializationException Not Thrown. Instead Got Clean Run");
  }


//  public void testCatchExtraThreads() throws Exception {
//    Thread.sleep(24 * 60 * 60 * 1000);  // sleep for one day
//  }
  
  public void testDeployAgainAndAgain() throws Exception {
    System.out.println("-------------- testDeployAgainAndAgain -------------");
                                                                                          // in the
                                                                                          // loop,
                                                                                          // no
                                                                                          // change.

    for (int num = 1; num <= 50; num++) {
      BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl(); // here or
      System.out.println("\nRunning iteration " + num);
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
      deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator2.xml");
      deployService(eeUimaEngine, relativePath + "/Deploy_AggregateWithParallelFlow.xml");
      runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
              "TopLevelTaeQueue", 1, PROCESS_LATCH);
    }
  }

  private Exception getCause(Throwable e) {
    Exception cause = (Exception) e;
    while (cause.getCause() != null) {
      cause = (Exception) cause.getCause();
    }
    return cause;
  }

  /**
   * This tests GetMeta retries. It deploys a simple Aggregate service that contains a collocated
   * Primitive service and a Primitive remote. The Primitive remote is simulated in this code. The
   * code starts a listener where the Aggregate sends GetMeta requests. The listener responds to the
   * Aggregate with its metadata only when an expected number of GetMeta retries is met. If the
   * Aggregate fails to send expected number of GetMeta requests, the listener will not adjust its
   * CountDownLatch and will cause this test to hang.
   * 
   * @throws Exception
   */

  public void getMetaRetry() throws Exception {
    getMetaCountLatch = new CountDownLatch(MaxGetMetaRetryCount);
    Connection connection = getConnection();
    connection.start();

    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    ActiveMQDestination destination = (ActiveMQDestination) session
            .createQueue(primitiveServiceQueue1);
    ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session
            .createConsumer(destination);
    consumer.setMessageListener(new MessageListener() {
      public void onMessage(Message aMessage) {
        try {
          if (isMetaRequest(aMessage)) {
            // Reply with metadata when retry count reaches defined threshold
            if (getMetaRequestCount > 0 && getMetaRequestCount % MaxGetMetaRetryCount == 0) {
              JmsMessageContext msgContext = new JmsMessageContext(aMessage, primitiveServiceQueue1);
              JmsOutputChannel outputChannel = new JmsOutputChannel();
              outputChannel.setServiceInputEndpoint(primitiveServiceQueue1);
              outputChannel.setServerURI(getBrokerUri());
              Endpoint endpoint = msgContext.getEndpoint();
              outputChannel.sendReply(getPrimitiveMetadata1(PrimitiveDescriptor1), endpoint, true);
            }
            getMetaRequestCount++;
            getMetaCountLatch.countDown(); // Count down to unblock the thread
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    consumer.start();
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    String containerId = deployService(eeUimaEngine, relativePath
            + "/Deploy_AggregateAnnotator.xml");

    Map<String, Object> appCtx = new HashMap();
    appCtx.put(UimaAsynchronousEngine.ServerUri, String.valueOf(broker.getMasterConnectorURI()));
    appCtx.put(UimaAsynchronousEngine.ENDPOINT, "TopLevelTaeQueue");
    appCtx.put(UimaAsynchronousEngine.CasPoolSize, Integer.valueOf(4));
    appCtx.put(UimaAsynchronousEngine.ReplyWindow, 15);
    appCtx.put(UimaAsynchronousEngine.Timeout, 0);
    initialize(eeUimaEngine, appCtx);

    System.out
            .println("TestBroker.testGetMetaRetry()-Blocking On GetMeta Latch. Awaiting GetMeta Requests");

    /*********************************************************************************/
    /**** This Code Will Block Until Expected Number Of GetMeta Requests Arrive ******/
    getMetaCountLatch.await();
    /*********************************************************************************/

    consumer.stop();
    connection.stop();
    eeUimaEngine.undeploy(containerId);
    eeUimaEngine.stop();
  }

  public ProcessingResourceMetaData getPrimitiveMetadata1(String aDescriptor) throws Exception {
    ResourceSpecifier resourceSpecifier = UimaClassFactory.produceResourceSpecifier(aDescriptor);
    return ((AnalysisEngineDescription) resourceSpecifier).getAnalysisEngineMetaData();
  }

  private static boolean deleteAllFiles(File directory) {
    if (directory.isDirectory()) {
      String[] files = directory.list();
      for (int i = 0; i < files.length; i++) {
        deleteAllFiles(new File(directory, files[i]));
      }
    }
    // Now have an empty directory or simple file
    return directory.delete();
  }

  private static class TestListener extends UimaAsBaseCallbackListener implements Runnable {
    private String casReferenceId = null;

    private Object monitor = new Object();

    public TestListener(TestUimaASExtended aTester) {
    }

    public void collectionProcessComplete(EntityProcessStatus arg0) {
      // TODO Auto-generated method stub

    }

    public void onBeforeMessageSend(UimaASProcessStatus status) {
      System.out.println("TestListener Received onBeforeMessageSend Notification with Cas:"
              + status.getCasReferenceId());
    }

    public void entityProcessComplete(CAS aCAS, EntityProcessStatus aProcessStatus) {
      if (aProcessStatus.isException()) {
        if (aProcessStatus instanceof UimaASProcessStatus) {
          casReferenceId = ((UimaASProcessStatus) aProcessStatus).getCasReferenceId();
          if (casReferenceId != null) {
            synchronized (monitor) {
              monitor.notifyAll();
            }
          }
        }
      }
    }

    public void initializationComplete(EntityProcessStatus arg0) {
      // TODO Auto-generated method stub

    }

    public String getCasReferenceId() {
      synchronized (monitor) {
        while (casReferenceId == null) {
          try {
            monitor.wait();
          } catch (InterruptedException e) {
          }
        }
      }
      return casReferenceId;
    }

    public void doStop() {
    }

    public void run() {
      System.out.println("Stopping TestListener Callback Listener Thread");
    }
  }
}
