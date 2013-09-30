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

package org.apache.uima.aae.jmx;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.uima.UIMAFramework;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.util.Level;
import org.springframework.jmx.JmxException;

public class RemoteJMXServer {
  private static final Class CLASS_NAME = RemoteJMXServer.class;

  private MBeanServerConnection brokerMBeanServer = null;

  private ConcurrentHashMap<String, QueueViewMBean> queueMBeanMap = new ConcurrentHashMap<String, QueueViewMBean>();

  private String brokerName;

  private JMXConnector jmxc = null;

  private volatile boolean initialized = false;

  public boolean isInitialized() {
    return initialized;
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
  public void initialize(String jmxDomain, String remoteJMXServerHostname,
          String remoteJMXServerPort) throws Exception {
    // Construct connect string to the JMX MBeanServer
    String remoteJmxUrl = "service:jmx:rmi:///jndi/rmi://" + remoteJMXServerHostname + ":"
            + remoteJMXServerPort + "/jmxrmi";

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
  /**
   * Disconnects from MBeanServer
   */
  public void disconnect() {
    if (jmxc != null) {
      try {
        jmxc.close();
        brokerMBeanServer = null;
        queueMBeanMap.clear();
      } catch (IOException e) {
      }
    }
  }

  /**
   * Tries to fetch total number of MBeans in the MBeanServer. The real goal here is to check if the
   * server responds. Failure here indicates failed server connection.
   * 
   * @return
   */
  public boolean isServerAvailable() {
    try {
      brokerMBeanServer.getMBeanCount();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public void attachToTempQueue(String tempQueueName) throws Exception {
    if (!queueMBeanMap.containsKey(tempQueueName)) {
      ObjectName uimaServiceTempReplyQueuePattern = composeObjectName("TempQueue", tempQueueName);
      QueueViewMBean replyQueueMBean = getQueueMBean(tempQueueName,
              uimaServiceTempReplyQueuePattern);
      if (replyQueueMBean != null) {
        queueMBeanMap.put(tempQueueName, replyQueueMBean);
      }
    }
  }

  private ObjectName composeObjectName(String queueType, String queueName) throws Exception {

    ObjectName n = new ObjectName(brokerName + ",Type=" + queueType + ",Destination=" + queueName);
    return n;
  }

  /**
   * Checks if a given queue name exists in remote MBeanServer's registry.
   * NOTE: The code returns true in case the MBeanServer is not available.
   * 
   * @param queueName
   *          - queue to lookup in the MBeanServer
   * @return - true if queue exists, false otherwise
   */
  public boolean isClientReplyQueueAvailable(String queueName) {
    try {
      ObjectName uimaServiceTempReplyQueuePattern = composeObjectName("TempQueue", queueName);
      // Tests if queue exists. If a client terminates, the reply queue will be removed and we
      // expect null from getQueueMBean()
      if (isServerAvailable() && getQueueMBean(queueName, uimaServiceTempReplyQueuePattern) == null) {
        return false;
      }
    } catch (Exception e) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
              "isClientReplyQueueAvailable", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_exception__WARNING", e);    
    }
    return true;  // returns true, in case the MBeanServer is not available or the queue exists
  }

  /**
   * Creates proxy to a queue MBean using given match pattern
   * 
   * @param key
   * @param matchPattern
   * @return
   * @throws Exception
   */
  private QueueViewMBean getQueueMBean(String key, ObjectName matchPattern) throws Exception {
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

  /**
   * Replaces ':' with '_'. JMX queries containing ':' are illegal.
   * 
   * @param destinationName
   * @return
   */
  public String normalize(String destinationName) {
    String qN = destinationName.substring(destinationName.indexOf("ID"));
    return qN.replaceAll(":", "_");
  }

  public static void main(String[] args) {
    try {
      RemoteJMXServer broker = new RemoteJMXServer();
      broker.initialize("org.apache.activemq", args[0], args[1]);
      if (broker.isClientReplyQueueAvailable(args[2])) {
        System.out.println("TempQueue:" + args[2] + " Exists");
      } else {
        System.out.println("TempQueue:" + args[2] + " Does not Exist");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
