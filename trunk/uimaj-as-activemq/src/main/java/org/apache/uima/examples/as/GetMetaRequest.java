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

package org.apache.uima.examples.as;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.UIMAMessage;
import org.springframework.jmx.JmxException;

/**
 * This is a stand-alone utility class with a "main".
 * It is used to retrieve the meta information for a UIMA service, which is attached to a JMS Broker
 * 
 * It is passed the broker URI and the service name, and optionally, a -verbose flag.
 */
public class GetMetaRequest {
  private static MBeanServerConnection brokerMBeanServer = null;
  private static String brokerName;
  private static JMXConnector jmxc = null;
  private static boolean initialized = false;
  private static enum QueueState {exists, existsnot, jmxnot};
  private static String jmxPort;

  /**
   * retrieve meta information for a UIMA-AS Service attached to a broker
   * It uses the port 1099 as the JMX port on the broker, unless overridden
   *   by defining the system property activemq.broker.jmx.port with a value of another port number
   * It uses the default JMX ActiveMQ Domain "org.apache.activemq", unless overridden
   *   by defining the system property activemq.broker.jmx.domain with a value of the domain to use
   *   This normally never needs to be done unless multiple brokers are run on the same node 
   *   as is sometimes done for unit tests.
   * @param args - brokerUri serviceName [-verbose]
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Need arguments: brokerURI serviceName [-verbose]");
      System.exit(1);
    }
    String brokerURI = args[0];
    String queueName = args[1];
    boolean printReply = false;
    if(args.length > 2) {
      if (args[2].equalsIgnoreCase("-verbose")) {
        printReply = true;
      }
      else {
        System.err.println("Unknown argument: " + args[2]);
        System.exit(1);
      }
    }
    final Connection connection;
    Session producerSession = null;
    Queue producerQueue = null;
    MessageProducer producer;
    MessageConsumer consumer;
    Session consumerSession = null;
    TemporaryQueue consumerDestination = null;
    long startTime = 0;

    //  Check if JMX server port number was specified
    jmxPort = System.getProperty("activemq.broker.jmx.port");
    if ( jmxPort == null || jmxPort.trim().length() == 0 ) {
      jmxPort = "1099";  // default
    }

    try {
      //  First create connection to a broker
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURI);
      connection = factory.createConnection();
      connection.start();
      Runtime.getRuntime().addShutdownHook( 
         new Thread(
              new Runnable() {
                  public void run() {
                    try {
                        if ( connection != null ) {
                          connection.close();
                        } 
                        if ( jmxc != null ) {
                          jmxc.close();
                        }
                    } catch( Exception ex) {
                    }
                  }       
              }
         )
      );
      
      URI target = new URI(brokerURI);
      String brokerHost = target.getHost();

      attachToRemoteBrokerJMXServer(brokerURI);
      if (isQueueAvailable(queueName) == QueueState.exists) {
        System.out.println("Queue "+queueName+" found on "+ brokerURI);
        System.out.println("Sending getMeta...");
      }
      else if (isQueueAvailable(queueName) == QueueState.existsnot) {
        System.err.println("Queue "+queueName+" does not exist on "+ brokerURI);
        System.exit(1);
      }
      else {
        System.out.println("Cannot see queues on JMX port "+brokerHost+":"+jmxPort);
        System.out.println("Sending getMeta anyway...");
      }

      producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      producerQueue = producerSession.createQueue(queueName);
      producer = producerSession.createProducer(producerQueue);
      consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerDestination = consumerSession.createTemporaryQueue();
      //  -----------------------------------------------------------------------------
      //  Create message consumer. The consumer uses a selector to filter out messages other
      //  then GetMeta replies. Currently UIMA AS service returns two messages for each request:
      //  ServiceInfo message and GetMeta message. The ServiceInfo message is returned by the 
      //  service immediately upon receiving a message from a client. This serves dual purpose, 
      //  1) to make sure the client reply destination exists
      //  2) informs the client which service is processing its request
      //  -----------------------------------------------------------------------------
      consumer = consumerSession.createConsumer(consumerDestination, "Command=2001");
      TextMessage msg = producerSession.createTextMessage();
      msg.setStringProperty(AsynchAEMessage.MessageFrom, consumerDestination.getQueueName());
      msg.setStringProperty(UIMAMessage.ServerURI, brokerURI);
      msg.setIntProperty(AsynchAEMessage.MessageType, AsynchAEMessage.Request);
      msg.setIntProperty(AsynchAEMessage.Command, AsynchAEMessage.GetMeta);
      msg.setJMSReplyTo(consumerDestination);
      msg.setText("");
      producer.send(msg);
      startTime = System.nanoTime();

      System.out.println("Sent getMeta request to " + queueName + " at " + brokerURI);
      System.out.println("Waiting for getMeta reply...");
      ActiveMQTextMessage reply = (ActiveMQTextMessage) consumer.receive();
      long waitTime = (System.nanoTime() - startTime)/1000000;
      
      System.out.println("Reply from "+reply.getStringProperty("ServerIP")+" received in " + waitTime + " ms");
      if (printReply) {
        System.out.println("Reply MessageText: " + reply.getText());
      }
    } catch (Exception e) {
      System.err.println(e.toString());
    }
    System.exit(0);
}
  /**
   * Connects to this service Broker's JMX Server. If unable to connect, this method
   * fails silently. The method uses default JMX Port number 1099 to create a connection
   * to the Broker's JMX MBean server. The default can be overridden via System 
   * property 'activemq.broker.jmx.port'. If connection cannot be established the 
   * method silently fails.
   * 
   */
  private static void attachToRemoteBrokerJMXServer(String brokerUri) throws Exception {
    //  Fetch AMQ jmx domain from system properties. This property is not required
    //  and the default AMQ jmx is used. The only exception is when the service is
    //  deployed in a jvm with multiple brokers deployed as it is the case with jUnit 
    //  tests. In such a case, each broker will register self with JMX using a different
    //  domain.
    String jmxAMQDomain = System.getProperty("activemq.broker.jmx.domain");
    if ( jmxAMQDomain == null ) {
      jmxAMQDomain = "org.apache.activemq";    // default
    }
    String brokerHostname="";
    URI target = new URI(brokerUri);
    brokerHostname = target.getHost();
    initialize(jmxAMQDomain, brokerHostname,jmxPort);
  }
  /**
   * Creates a connection to an MBean Server identified by
   * <code>remoteJMXServerHostName and remoteJMXServerPort</code>
   * 
   * @param remoteJMXServerHostName
   *          - MBeanServer host name
   * @param remoteJMXServerPort
   *          - MBeanServer port
   * @return - none
   * 
   * @throws Exception
   */
  private static void initialize(String jmxDomain, String remoteJMXServerHostname,
          String remoteJMXServerPort) throws Exception {
    // Construct connect string to the JMX MBeanServer
    String remoteJmxUrl = "service:jmx:rmi:///jndi/rmi://" + remoteJMXServerHostname + ":"
            + remoteJMXServerPort + "/jmxrmi";

    try {
      JMXServiceURL url = new JMXServiceURL(remoteJmxUrl);
      jmxc = JMXConnectorFactory.connect(url, null);
      brokerMBeanServer = jmxc.getMBeanServerConnection();
      // Its possible that the above code succeeds even though the broker runs
      // with no JMX Connector. At least on windows the above does not throw an
      // exception as expected. It appears that the broker registers self JVMs MBeanServer
      // but it does *not* register any Connections, nor Queues. The code below 
      // checks if the MBean server we are connected to has any QueueMBeans registered.
      // A broker with jmx connector should have queue mbeans registered and thus
      //  the code below should always succeed. Conversely, a broker with no jmx connector
      // reports no queue mbeans.
      
      //  Query broker MBeanServer for QueueMBeans
      Set queueSet = brokerMBeanServer.queryNames(new ObjectName(jmxDomain
              + ":*,Type=Queue"), (QueryExp) null);
      if ( queueSet.isEmpty() ) {  //  No QueueMBeans, meaning no JMX support
        throw new JmxException("ActiveMQ Broker Not Configured With JMX Support");
      }
    } catch (Exception e) {
      return;
    }
    // Query JMX Server for Broker MBean. We need the broker's name from an MBean to construct
    // queue query string.

    for (Object nameObject : brokerMBeanServer.queryNames(new ObjectName(jmxDomain
            + ":*,Type=Broker"), (QueryExp) null)) {
      ObjectName brokerObjectName = (ObjectName) nameObject;
      if (brokerObjectName.getCanonicalName().endsWith("Type=Broker")) {
        // Extract just the name from the canonical name
        brokerName = brokerObjectName.getCanonicalName().substring(0,
                brokerObjectName.getCanonicalName().indexOf(","));
        initialized = true;
        break; // got the broker name
      }
    }
  }

  private static boolean isServerAvailable() {
    try {
      brokerMBeanServer.getMBeanCount();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static QueueState isQueueAvailable(String queueName) throws Exception {
    if (!initialized) {
      return QueueState.jmxnot;
    }
    ObjectName uimaServiceQueuePattern = new ObjectName(brokerName + ",Type=Queue,Destination=" + queueName);
    // Tests if queue exists. If a client terminates, the reply queue will be removed and we
    // expect null from getQueueMBean()
    if (isServerAvailable() && getQueueMBean(queueName, uimaServiceQueuePattern) == null) {
      return QueueState.existsnot;
    }
    return QueueState.exists;
  }

  private static QueueViewMBean getQueueMBean(String key, ObjectName matchPattern) throws Exception {
    // Fetch queue names matching a given pattern.
    Set<ObjectName> queues = new HashSet<ObjectName>(brokerMBeanServer.queryNames(matchPattern,
            null));
    for (ObjectName name : queues) {
      // Create and return a proxy to the queue's MBean
      return (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(brokerMBeanServer,
              name, QueueViewMBean.class, true);
    }
    return null;
  }

}
