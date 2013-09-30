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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.URI;
import java.util.concurrent.Semaphore;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.uima.UIMAFramework;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.jms.error.handler.BrokerConnectionException;
import org.apache.uima.util.Level;

public class ActiveMQSupport extends TestCase {
  private static final Class CLASS_NAME = ActiveMQSupport.class;
  protected static final String DEFAULT_BROKER_URL_KEY="DefaultBrokerURL";
  protected static final String DEFAULT_BROKER_URL_KEY_2="DefaultBrokerURL2";
  protected static final String DEFAULT_HTTP_BROKER_URL_KEY="DefaultHttpBrokerURL";
  protected static final String DEFAULT_HTTP_BROKER_URL_KEY_2="DefaultHttpBrokerURL2";
  protected static final int DEFAULT_BROKER_PORT=61617;
  protected static final int DEFAULT_BROKER_PORT_2=61620;
  protected static final int DEFAULT_HTTP_PORT = 18888;
  protected static final int DEFAULT_HTTP_PORT2 = 18890;

  
  protected static BrokerService broker;

  protected String uri = null;

  protected static ThreadGroup brokerThreadGroup = null;

  protected static TransportConnector tcpConnector = null;

  protected static final String relativePath = "src" + System.getProperty("file.separator")
          + "test" + System.getProperty("file.separator") + "resources"
          + System.getProperty("file.separator") + "deployment";

  protected static final String resourceDirPath = "src" + System.getProperty("file.separator")
          + "test" + System.getProperty("file.separator") + "resources";
 
  protected static final String relativeDataPath = "src" + System.getProperty("file.separator")
          + "test" + System.getProperty("file.separator") + "resources"
          + System.getProperty("file.separator") + "data";

  protected static TransportConnector httpConnector = null;

  public static Semaphore brokerSemaphore = new Semaphore(1);

  protected synchronized void setUp() throws Exception {
    super.setUp();
    broker = createBroker();
    broker.start();
    broker.setMasterConnectorURI(uri);
    addHttpConnector(DEFAULT_HTTP_PORT);
    if ( System.getProperty(DEFAULT_BROKER_URL_KEY) != null) {
      System.clearProperty(DEFAULT_BROKER_URL_KEY);
    }
    System.setProperty(DEFAULT_BROKER_URL_KEY, broker.getMasterConnectorURI());
    if ( System.getProperty(DEFAULT_HTTP_BROKER_URL_KEY) != null) {
      System.clearProperty(DEFAULT_HTTP_BROKER_URL_KEY);
    }
    System.setProperty(DEFAULT_HTTP_BROKER_URL_KEY, httpConnector.getConnectUri().toString());
    // define property so that UIMA AS error handler doesnt call System.exit() if the
    // error handler action=terminate.
    System.setProperty("dontKill","");  
  }
  protected void cleanBroker( BrokerService targetBroker) throws Exception {
    // Remove messages from all queues
    targetBroker.deleteAllMessages();
    org.apache.activemq.broker.Connection[] connections = targetBroker.getRegionBroker().getClients();
    for( org.apache.activemq.broker.Connection connection : connections) {
      try {
        connection.stop();
      } catch( Exception e) {
        e.printStackTrace();
      }
    }

    ActiveMQDestination[] destinations = targetBroker.getRegionBroker().getDestinations();

    if ( destinations != null ) {
      for( ActiveMQDestination destination: destinations ) {
        if ( !destination.isTopic() ) {
          targetBroker.removeDestination(destination);
        }
      }
    }
  }
  
  protected String addHttpConnector(int aDefaultPort) throws Exception {
    return addHttpConnector(broker, aDefaultPort);
  }
  protected String addHttpConnector(BrokerService aBroker, int aDefaultPort) throws Exception {
    boolean found = false;
    while( !found ) {
      try {
        httpConnector = addConnector(aBroker, "http",aDefaultPort);
        //  Use reflection to determine if the AMQ version is at least 5.2. If it is, we must
        //  plug in a broker to the httpConnector otherwise we get NPE when starting the connector.
        //  AMQ version 4.1.1 doesn't exhibit this problem.
        try {
          Method m = httpConnector.getClass().getDeclaredMethod("setBrokerService", new Class[] {BrokerService.class});
          m.invoke(httpConnector, aBroker);
        } catch ( NoSuchMethodException e) {
          //  Ignore, this is not AMQ 5.2
        }
        System.out.println("Adding HTTP Connector:" + httpConnector.getConnectUri());
        httpConnector.start();
        return httpConnector.getUri().toString();
      } catch ( BindException e) { 
        aDefaultPort++;
      } catch ( IOException e) {
        if ( e.getCause() != null && e.getCause() instanceof BindException ) {
          aDefaultPort++;
        } else {
          throw new BrokerConnectionException("Unexpected Exception While Connecting to Broker with URL:"+uri+"\n"+e);
        }
      } 
    }
    throw new BrokerConnectionException("Unable to acquire Open Port for HTTPConnector");
  }
  protected String getHttpURI() throws Exception {
    while ( httpConnector == null  ) {
     synchronized(this) {
       this.wait(100); 
     }
    }
    return httpConnector.getConnectUri().toString();
  }
  protected void removeQueue(String aQueueName) throws Exception {
    httpConnector.stop();
  }

  protected void removeHttpConnector() throws Exception {
    httpConnector.stop();
    broker.removeConnector(httpConnector);
  }
  
  protected TransportConnector addConnector(BrokerService aBroker, String type, int basePort) 
  throws BrokerConnectionException{
    boolean found = false;
    TransportConnector transportConnector = null;
    while( !found ) {
      try {
        String uri = type+"://localhost:" + basePort;
        transportConnector = aBroker.addConnector(uri);
        found = true;
      } catch ( BindException e) { 
        basePort++;
      } catch ( IOException e) {
        if ( e.getCause() != null && e.getCause() instanceof BindException ) {
          basePort++;
        } else {
          throw new BrokerConnectionException("Unexpected Exception While Connecting to Broker with URL:"+uri+"\n"+e);
        }
      } catch( Exception e) {
        throw new BrokerConnectionException("Unexpected Exception While Connecting to Broker with URL:"+uri+"\n"+e);
      }
    }
    return transportConnector;
  }

  protected String getBrokerUri() {
    return uri;
  }

  protected ConnectionFactory createConnectionFactory() throws Exception {
    return new ActiveMQConnectionFactory(uri);
  }

  protected Connection getConnection() throws Exception {
    return createConnectionFactory().createConnection();
  }

  public BrokerService createBroker() throws Exception {
    return createBroker(DEFAULT_BROKER_PORT, true, false);
  }

  protected BrokerService createBroker(int port, boolean useJmx, boolean secondaryBroker) throws Exception {
      String hostName = "localhost"; 
      BrokerService broker = 
        BrokerFactory.createBroker(new URI("broker:()/" + hostName + "?persistent=false"));
      tcpConnector = addConnector(broker, "tcp",port);
      uri = tcpConnector.getUri().toString();
      System.out.println(">>>> Starting Broker With URL:" + uri);

      if ( secondaryBroker ) {
        broker.getManagementContext().setJmxDomainName(broker.getManagementContext().getJmxDomainName()+".test");      
        tcpConnector.setName(DEFAULT_BROKER_URL_KEY_2);
      } else {
        tcpConnector.setName(DEFAULT_BROKER_URL_KEY);
      }
      broker.setUseJmx(useJmx);
      PolicyEntry policy = new PolicyEntry();
      policy.setDeadLetterStrategy(new SharedDeadLetterStrategy());

      PolicyMap pMap = new PolicyMap();
      pMap.setDefaultEntry(policy);

      broker.setDestinationPolicy(pMap);
      broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
      broker.setPersistent(false);
      broker.setUseShutdownHook(true);
      broker.setUseLoggingForShutdownErrors(false);
      try {
          Method method = broker.getClass().getDeclaredMethod("setSchedulerSupport", new Class[] {Boolean.TYPE});
          method.invoke(broker, new Object[] {Boolean.FALSE});
      } catch( NoSuchMethodException e) {
    	  //	ignore
      }

      return broker;
  }
  protected BrokerService setupSecondaryBroker(boolean addProperty) throws Exception {
    System.setProperty("activemq.broker.jmx.domain","org.apache.activemq.test");
    BrokerService broker2 = createBroker(DEFAULT_BROKER_PORT_2, true, true);
    broker2.start();
    if ( addProperty ) {
      System.setProperty("BrokerURL", broker2.getConnectorByName(DEFAULT_BROKER_URL_KEY_2).getUri().toString());
    }
    return broker2;
  }
  protected void stopBroker() throws Exception {
    if (broker != null) {
      System.out.println(">>> Stopping Broker");
      if (tcpConnector != null) {
        tcpConnector.stop();
        broker.removeConnector(tcpConnector);
        System.out.println("Broker Connector:" + tcpConnector.getUri().toString() + " is stopped");
      }
      removeHttpConnector();
      broker.deleteAllMessages();
      broker.stop();
      broker.waitUntilStopped();
      System.out.println(">>> Broker Stopped");
    }
  }

  protected synchronized void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("activemq.broker.jmx.domain");
    System.clearProperty("BrokerURL");
    stopBroker();

  }

}
