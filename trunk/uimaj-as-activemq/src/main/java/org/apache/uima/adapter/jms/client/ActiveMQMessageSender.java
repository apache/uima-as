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
package org.apache.uima.adapter.jms.client;

import java.util.concurrent.ConcurrentHashMap;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.util.Level;

/**
 * Initializes JMS session and creates JMS MessageProducer to be used for sending messages to a
 * given destination. It extends BaseMessageSender which starts the worker thread and is tasked with
 * sending messages. The application threads share a common 'queue' with the worker thread. The
 * application threads add messages to the pendingMessageList 'queue' and the worker thread consumes
 * them.
 * 
 */
public class ActiveMQMessageSender extends BaseMessageSender {
  private static final Class CLASS_NAME = ActiveMQMessageSender.class;

  private volatile Connection connection = null;

  private Session session = null;

  private MessageProducer producer = null;

  private String destinationName = null;

  private ConcurrentHashMap<Destination, MessageProducer> producerMap = new ConcurrentHashMap<Destination, MessageProducer>();

  public ActiveMQMessageSender(Connection aConnection, String aDestinationName,
          BaseUIMAAsynchronousEngineCommon_impl engine) throws Exception {
    super(engine);
    connection = aConnection;
    destinationName = aDestinationName;
  }

  public synchronized MessageProducer getMessageProducer(Destination destination) throws Exception {
    if (producerMap.containsKey(destination)) {
      return (MessageProducer) producerMap.get(destination);
    }
    createSession();
    MessageProducer mProducer = session.createProducer(destination);
    mProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    producerMap.put(destination, mProducer);
    return mProducer;
  }
  /**
   * This is called when a new Connection is created after broker is restarted
   */
  public void setConnection(Connection aConnection) {
    connection = aConnection;
    cleanup();
    try {
      initializeProducer();
    } catch( Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "setConnection", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
    }
    
  }
  private String getBrokerURL() {
    try {
      return ((ActiveMQConnection) connection).getBrokerInfo().getBrokerURL();
    } catch (Exception ex) { /* handle silently. */
    }
    return "";
  }

  private void createSession() throws Exception {
    String broker = getBrokerURL();
    try {
      if (session == null || engine.producerInitialized == false) {
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      }
    } catch (JMSException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "createSession", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_client_failed_creating_session_INFO",
                new Object[] { destinationName, broker });
      }
      if (connection == null) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "createSession", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_client_connection_not_ready_INFO", new Object[] { broker });
        }
      } else if (((ActiveMQConnection) connection).isClosed()
              || ((ActiveMQConnection) connection).isClosing()) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME)
                  .logrb(Level.INFO, CLASS_NAME.getName(), "createSession",
                          JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAJMS_client_connection_closed_INFO",
                          new Object[] { destinationName, broker });
        }
      }
      throw e;
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * Creates a jms session object used to instantiate message producer
   */
  protected void initializeProducer() throws Exception {
    createSession();
    producer = getMessageProducer(session.createQueue(destinationName));
  }

  /**
   * Returns the full name of the destination queue
   */
  protected String getDestinationEndpoint() throws Exception {
    return ((ActiveMQDestination) producer.getDestination()).getPhysicalName();
  }

  /**
   * Returns jsm MessageProducer
   */
  public MessageProducer getMessageProducer() {
    if ( engine.running && engine.producerInitialized == false  ) {
      try {
        setConnection(engine.lookupConnection(getBrokerURL()).getConnection());
        initializeProducer();
        engine.producerInitialized = true;
      } catch( Exception e) {
        e.printStackTrace();
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "getMessageProducer", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
        }
      }
    } 
    return producer;
  }

  public TextMessage createTextMessage() throws Exception {
    if (session == null) {
    //	Force initialization of Producer
      initializeProducer();
    }
    return session.createTextMessage("");
  }

  public BytesMessage createBytesMessage() throws Exception {
    if (session == null) {
    //	Force initialization of Producer
      initializeProducer();
    }
    return session.createBytesMessage();
  }

  /**
   * Cleanup any jms resources used by the worker thread
   */
  protected void cleanup() { 
    try {
      if (session != null) {
        session.close();
        session = null;
      }
      if (producer != null) {
        producer.close();
        producer = null;
      }
    } catch (Exception e) {
      // Ignore we are shutting down
    } finally {
      producerMap.clear();
    }
  }
}