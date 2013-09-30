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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.uima.UIMA_IllegalStateException;
import org.apache.uima.aae.UimaClassFactory;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.activemq.JmsOutputChannel;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.adapter.jms.message.JmsMessageContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.ee.test.utils.BaseTestSupport;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;

public class TestUimaASBasic extends BaseTestSupport {
  private static final int DEFAULT_HTTP_PORT = 8888;

  private CountDownLatch getMetaCountLatch = null;

  private static final int MaxGetMetaRetryCount = 2;

  private static final String primitiveServiceQueue1 = "NoOpAnnotatorQueue";

  private static final String PrimitiveDescriptor1 = "resources/descriptors/analysis_engine/NoOpAnnotator.xml";

  private int getMetaRequestCount = 0;

  /**
   * Tests Broker startup and shutdown
   */
  public void testBrokerLifecycle() {
    System.out.println("-------------- testBrokerLifecycle -------------");
  }

  /**
   * Tests handling of multiple calls to initialize(). A subsequent call to initialize should result
   * in ResourceInitializationException.
   * 
   * @throws Exception
   */
  public void testInvalidInitializeCall() throws Exception {
    System.out.println("-------------- testInvalidInitializeCall -------------");
    // Instantiate Uima EE Client
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
      e.printStackTrace();
      // expected
    } catch (Throwable e) {
      e.printStackTrace();

    } finally {
      eeUimaEngine.stop();
    }
  }

  /**
   * Tests deployment of a primitive Uima EE Service (PersontTitleAnnotator). Deploys the primitive
   * in the same jvm using Uima EE Client API and blocks on a monitor until the Uima Client calls
   * initializationComplete() method. Once the primitive service starts it is expected to send its
   * metadata to the Uima client which in turn notifies this object with a call to
   * initializationComplete() where the monitor is signaled to unblock the thread. This code will
   * block if the Uima Client does not call initializationComplete()
   * 
   * @throws Exception
   */
  public void testDeployPrimitiveService() throws Exception {
    System.out.println("-------------- testDeployPrimitiveService -------------");
    // Instantiate Uima EE Client
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    // Deploy Uima EE Primitive Service
    deployService(eeUimaEngine, relativePath + "/Deploy_PersonTitleAnnotator.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()),
            "PersonTitleAnnotatorQueue", 0, EXCEPTION_LATCH);
  }

  /**
   * Tests a simple Aggregate with one remote Delegate and collocated Cas Multiplier
   * 
   * @throws Exception
   */
  public void testDeployAggregateService() throws Exception {
    System.out.println("-------------- testDeployAggregateService -------------");
    BaseUIMAAsynchronousEngine_impl eeUimaEngine = new BaseUIMAAsynchronousEngine_impl();
    System.setProperty(JmsConstants.SessionTimeoutOverride, "2500000");
    deployService(eeUimaEngine, relativePath + "/Deploy_NoOpAnnotator.xml");
    deployService(eeUimaEngine, relativePath + "/Deploy_AggregateAnnotator.xml");
    runTest(null, eeUimaEngine, String.valueOf(broker.getMasterConnectorURI()), "TopLevelTaeQueue",
            0, EXCEPTION_LATCH);
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
}
