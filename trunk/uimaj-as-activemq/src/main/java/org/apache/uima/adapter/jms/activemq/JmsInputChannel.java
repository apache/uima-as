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

package org.apache.uima.adapter.jms.activemq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.InputChannel;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.Endpoint_impl;
import org.apache.uima.aae.controller.PrimitiveAnalysisEngineController;
import org.apache.uima.aae.controller.BaseAnalysisEngineController.ServiceState;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.InvalidMessageException;
import org.apache.uima.aae.handler.Handler;
import org.apache.uima.aae.handler.HandlerBase;
import org.apache.uima.aae.jmx.RemoteJMXServer;
import org.apache.uima.aae.jmx.ServiceInfo;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.message.UIMAMessage;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.message.JmsMessageContext;
import org.apache.uima.util.Level;
import org.springframework.jms.listener.SessionAwareMessageListener;

/**
 * Thin adapter for receiving JMS messages from Spring. It delegates processing of all messages to
 * the {@link Handler}. Each JMS Message is wrapped in transport neutral MessageContext
 * wrapper.
 * 
 */
public class JmsInputChannel implements InputChannel, JmsInputChannelMBean,
        SessionAwareMessageListener {
  /**
	 * 
	 */
  private static final long serialVersionUID = -3318400773113552290L;

  private static final Class CLASS_NAME = JmsInputChannel.class;

  private transient final CountDownLatch msgHandlerLatch = new CountDownLatch(1);

  private transient final CountDownLatch controllerLatch = new CountDownLatch(1);

  // Reference to the first Message Handler in the Chain.
  private transient Handler handler;

  // The name of the queue this Listener is expecting to receive messages from
  private String endpointName;

  // Reference to the Controller Object
  private transient AnalysisEngineController controller;

  private int sessionAckMode;

  private transient UimaDefaultMessageListenerContainer messageListener;

  private transient Session jmsSession;

  private String brokerURL = "";

  private ServiceInfo serviceInfo = null;

  private volatile boolean stopped = false;

  private volatile boolean channelRegistered = false;

  private List listenerContainerList = new ArrayList();

  private Object mux = new Object();

  private transient RemoteJMXServer remoteJMXServer = null;
  //  synchronizes initialization of RemotBroker
  private Object brokerMux = new Object();
  
  private ConcurrentHashMap<String, UimaDefaultMessageListenerContainer> failedListenerMap = new ConcurrentHashMap<String, UimaDefaultMessageListenerContainer>();

  //  A global flag that determines if we should create a connection to broker's MBeanServer to be
  //  able to determine if client's reply queue exists before processing a CAS. 
  public static transient boolean attachToBrokerMBeanServer=true;
  
  public AnalysisEngineController getController() {
    return controller;
  }

  public String getName() {
    return endpointName;
  }

  public void setController(AnalysisEngineController aController) throws Exception {

    this.controller = aController;
    if (!channelRegistered) {
      controller.addInputChannel(this);
    }
    controller.setInputChannel(this);
    controllerLatch.countDown();
  }

  public void setMessageHandler(Handler aHandler) {
    handler = aHandler;
    msgHandlerLatch.countDown();
  }

  public void setEndpointName(String anEndpointName) {
    endpointName = anEndpointName;
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
              "setEndpointName", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_service_listening__INFO", new Object[] { anEndpointName });

    }
  }

  /**
   * Validate message type contained in the JMS header.
   * 
   * @param aMessage
   *          - jms message retrieved from queue
   * @param properties
   *          - map containing message properties
   * @return - true if message Type is valid, false otherwise
   * @throws Exception
   */

  private boolean validMessageType(Message aMessage, Map properties) throws Exception {
    if (properties.containsKey(AsynchAEMessage.MessageType)) {
      int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
      if (msgType != AsynchAEMessage.Response && msgType != AsynchAEMessage.Request) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validMessageType", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_msgtype_in_message__INFO",
                  new Object[] { msgType, endpointName });
        }
        return false;

      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "validMessageType", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_msgtype_notin_message__INFO", new Object[] { endpointName });
      }
      return false;
    }

    return true;
  }

  private boolean isProcessRequest(Message aMessage) throws Exception {
    Map properties = ((ActiveMQMessage) aMessage).getProperties();
    if (properties.containsKey(AsynchAEMessage.MessageType)
            && properties.containsKey(AsynchAEMessage.Command)) {
      int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);

      if (msgType != AsynchAEMessage.Request || command != AsynchAEMessage.Process) {
        return false;
      }
      return true;
    }
    return false;
  }

  private boolean isRemoteRequest(Message aMessage) throws Exception {

    // Dont do checkpoints if a message was sent from a Cas Multiplier
    if (aMessage.propertyExists(AsynchAEMessage.CasSequence)) {
      return false;
    }

    Map properties = ((ActiveMQMessage) aMessage).getProperties();
    if (properties.containsKey(AsynchAEMessage.MessageType)
            && properties.containsKey(AsynchAEMessage.Command)
            && properties.containsKey(UIMAMessage.ServerURI)) {
      int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      boolean isRemote = aMessage.getStringProperty(UIMAMessage.ServerURI).startsWith("vm") == false;
      if (isRemote
              && msgType == AsynchAEMessage.Request
              && (command == AsynchAEMessage.Process || command == AsynchAEMessage.CollectionProcessComplete)) {
        return true;
      }
    }
    return false;
  }

  private boolean acceptsDeltaCas(Message aMessage) throws Exception {
    Map properties = ((ActiveMQMessage) aMessage).getProperties();
    boolean acceptsDeltaCas = false;
    if (properties.containsKey(AsynchAEMessage.AcceptsDeltaCas)) {
      acceptsDeltaCas = aMessage.getBooleanProperty(AsynchAEMessage.AcceptsDeltaCas);
    }
    return acceptsDeltaCas;
  }

  /**
   * Validate command contained in the header of the JMS Message
   * 
   * @param aMessage
   *          - JMS Message received
   * @param properties
   *          - Map containing header properties
   * @return - true if the command received is a valid one, false otherwise
   * @throws Exception
   */
  private boolean validCommand(Message aMessage, Map properties) throws Exception {
    if (properties.containsKey(AsynchAEMessage.Command)) {
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      if (command != AsynchAEMessage.Process && command != AsynchAEMessage.GetMeta
              && command != AsynchAEMessage.ReleaseCAS && command != AsynchAEMessage.Stop
              && command != AsynchAEMessage.Ping && command != AsynchAEMessage.ServiceInfo
              && command != AsynchAEMessage.CollectionProcessComplete) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validCommand", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_command_in_message__INFO",
                  new Object[] { command, endpointName });
        }
        return false;
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "validCommand",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_command_notin_message__INFO",
                new Object[] { endpointName });
      }
      return false;
    }

    return true;
  }

  /**
   * Validates payload in the JMS Message.
   * 
   * @param aMessage
   *          - JMS Message received
   * @param properties
   *          - Map containing header properties
   * @return - true if the payload is valid, false otherwise
   * @throws Exception
   */
  private boolean validPayload(Message aMessage, Map properties) throws Exception {
    if (properties.containsKey(AsynchAEMessage.Command)) {
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      if (command == AsynchAEMessage.GetMeta
              || command == AsynchAEMessage.CollectionProcessComplete
              || command == AsynchAEMessage.Stop || command == AsynchAEMessage.Ping
              || command == AsynchAEMessage.ServiceInfo || command == AsynchAEMessage.ReleaseCAS) {
        // Payload not included in GetMeta Request
        return true;
      }
    }

    if (properties.containsKey(AsynchAEMessage.Payload)) {
      int payload = aMessage.getIntProperty(AsynchAEMessage.Payload);
      if (payload != AsynchAEMessage.XMIPayload && payload != AsynchAEMessage.BinaryPayload
              && payload != AsynchAEMessage.CASRefID && payload != AsynchAEMessage.Exception
              && payload != AsynchAEMessage.Metadata) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validPayload", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_payload_in_message__INFO",
                  new Object[] { payload, endpointName });
        }

        return false;
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "validPayload",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_payload_notin_message__INFO",
                new Object[] { endpointName });
      }
      return false;
    }

    return true;
  }

  private boolean isStaleMessage(Message aMessage) throws JMSException {
    int command = aMessage.getIntProperty(AsynchAEMessage.Command);
    int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
    if (isStopped() || getController() == null || getController().getInProcessCache() == null) {
      // Shutting down
      return true;
    }
    if (command == AsynchAEMessage.Process && msgType == AsynchAEMessage.Response) {
      String casReferenceId = aMessage.getStringProperty(AsynchAEMessage.CasReference);
      if (!getController().getInProcessCache().entryExists(casReferenceId)) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "isStaleMessage",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_stale_message__FINE",
                  new Object[] { endpointName, casReferenceId,
                      aMessage.getStringProperty(AsynchAEMessage.MessageFrom) });
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Validates contents of the message. It checks if command, payload and message types contain
   * valid data.
   * 
   * @param aMessage
   *          - JMS Message to validate
   * @return - true if message is valid, false otherwise
   * @throws Exception
   */
  public boolean validMessage(Message aMessage) throws Exception {
    if (aMessage instanceof ActiveMQMessage) {
      Map properties = ((ActiveMQMessage) aMessage).getProperties();
      if (!validMessageType(aMessage, properties)) {
        int msgType = 0;
        if (properties.containsKey(AsynchAEMessage.MessageType)) {
          msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_msg_type__INFO",
                  new Object[] { getController().getComponentName(), msgType });
        }
        return false;
      }
      if (!validCommand(aMessage, properties)) {
        int command = 0;
        if (properties.containsKey(AsynchAEMessage.Command)) {
          command = aMessage.getIntProperty(AsynchAEMessage.Command);
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_cmd_type__INFO",
                  new Object[] { getController().getComponentName(), command });
        }
        return false;
      }
      if (!validPayload(aMessage, properties)) {
        int payload = 0;
        if (properties.containsKey(AsynchAEMessage.Payload)) {
          payload = aMessage.getIntProperty(AsynchAEMessage.Payload);
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_payload_type__INFO",
                  new Object[] { getController().getComponentName(), payload });
        }
        return false;
      }

      if (isStaleMessage(aMessage)) {
        if (sessionAckMode == Session.CLIENT_ACKNOWLEDGE) {
          aMessage.acknowledge();
        }
        return false;
      }
    }
    return true;
  }

  public void abort() {
  }

  private String decodeIntToString(String aTypeToDecode, int aValueToDecode) {
    if (AsynchAEMessage.MessageType.equals(aTypeToDecode)) {
      switch (aValueToDecode) {
        case AsynchAEMessage.Request:
          return "Request";
        case AsynchAEMessage.Response:
          return "Response";
      }
    } else if (AsynchAEMessage.Command.equals(aTypeToDecode)) {
      switch (aValueToDecode) {
        case AsynchAEMessage.Process:
          return "Process";
        case AsynchAEMessage.GetMeta:
          return "GetMetadata";
        case AsynchAEMessage.CollectionProcessComplete:
          return "CollectionProcessComplete";
        case AsynchAEMessage.ReleaseCAS:
          return "ReleaseCAS";
        case AsynchAEMessage.Stop:
          return "Stop";
        case AsynchAEMessage.Ping:
          return "Ping";
        case AsynchAEMessage.ServiceInfo:
          return "ServiceInfo";
      }

    } else if (AsynchAEMessage.Payload.equals(aTypeToDecode)) {
      switch (aValueToDecode) {
        case AsynchAEMessage.XMIPayload:
          return "XMIPayload";
        case AsynchAEMessage.BinaryPayload:
          return "BinaryPayload";
        case AsynchAEMessage.CASRefID:
          return "CASRefID";
        case AsynchAEMessage.Metadata:
          return "Metadata";
        case AsynchAEMessage.Exception:
          return "Exception";
          // 5/2013 xcas not used
//        case AsynchAEMessage.XCASPayload:
//          return "XCASPayload";
        case AsynchAEMessage.None:
          return "None";
      }
    }
    return "UNKNOWN";
  }

  private boolean ackMessageNow(Message aMessage) throws JMSException {
    if (sessionAckMode != Session.CLIENT_ACKNOWLEDGE) {
      return false;
    }

    if (aMessage.getIntProperty(AsynchAEMessage.Command) == AsynchAEMessage.GetMeta
            || aMessage.getIntProperty(AsynchAEMessage.Command) == AsynchAEMessage.CollectionProcessComplete
            || aMessage.getIntProperty(AsynchAEMessage.Command) == AsynchAEMessage.ReleaseCAS
            || aMessage.getIntProperty(AsynchAEMessage.Command) == AsynchAEMessage.ACK) {
      return true;
    }
    return false;
  }

  private boolean isCheckpointWorthy(Message aMessage) throws Exception {
    synchronized (mux) {
      // Dont do checkpoints if a message was sent from a Cas Multiplier
      if (aMessage.propertyExists(AsynchAEMessage.CasSequence)) {
        return false;
      }
      Map properties = ((ActiveMQMessage) aMessage).getProperties();
      if (properties.containsKey(AsynchAEMessage.MessageType)
              && properties.containsKey(AsynchAEMessage.Command)
              && properties.containsKey(UIMAMessage.ServerURI)) {
        int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
        int command = aMessage.getIntProperty(AsynchAEMessage.Command);
        if (msgType == AsynchAEMessage.Request
                && (command == AsynchAEMessage.Process || command == AsynchAEMessage.CollectionProcessComplete)) {
          return true;
        }
      }
      return false;

    }
  }
  /**
   * Checks if the incoming request requires response.
   * 
   * @param aMessage - incoming message to check
   * @return - true if reply is required, false otherwise
   */
  private boolean isReplyRequired(Message aMessage ) {
    try {
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      if (aMessage.getIntProperty(AsynchAEMessage.MessageType) == AsynchAEMessage.Request &&
          ( command == AsynchAEMessage.Process ||
            command == AsynchAEMessage.GetMeta ||
            command == AsynchAEMessage.CollectionProcessComplete ) ) {
        return true;
      }
    } catch( Exception e) {
      //  ignore
    }
    return false;
  }
  private boolean validEndpoint(JmsMessageContext messageContext) {
    return (messageContext.getEndpoint() != null
            //  Reply destination must be present
            && messageContext.getEndpoint().getDestination() != null);
  }
  /**
   * Determines if a given message should be processed or not. If the message
   * requires response, the code checks if the client's temp reply queue still 
   * exists. If so, the message must be processed. If the reply queue doesnt
   * exist, the client must have terminated and there is no need to process
   * the message. The code consults the broker (via JMX) to see if the queue
   * exists.
   * 
   * @param aMessage
   * @param messageContext
   * @return true if the message is of type Process, false otherwise
   * @throws Exception
   */
  private boolean processRequestMessage(Message aMessage, JmsMessageContext messageContext) throws Exception {
    //  check if we can reply to the client. The code uses connection to 
    //  broker's JMX server to lookup client's temp reply queue. If the
    //  queue does not exist the incoming message is dropped. No need to
    //  waste cycles when we know that the client has terminated. 
    if ( isReplyRequired(aMessage)
            //  Exclude request messages from a Cas Multiplier. We are not replying
            //  to CM. Only CM adds CasSequence to a request message
            && !messageContext.propertyExists(AsynchAEMessage.CasSequence)
            //  Reply destination must be present
            && validEndpoint(messageContext) ) {
      //  replace ':' with '_' to enable JMX query to work. ':' is an invalid char for queries
      String queueName = remoteJMXServer.normalize(messageContext.getEndpoint().getDestination().toString());       
      //  Check if reply queue provided in a message still exists in broker's
      //  JMX Server registry. In case the server is not available the call returns true.
      return remoteJMXServer.isClientReplyQueueAvailable(queueName);
    }
    return true; // Default, PROCESS THE MESSAGE
  }
  private boolean ackClient( JmsMessageContext messageContext ) throws Exception {
	  return ( !messageContext.propertyExists(AsynchAEMessage.CasSequence) &&
			   messageContext.getMessageStringProperty(UIMAMessage.ServerURI).startsWith("vm") == false &&
			   validEndpoint(messageContext) &&  
			   isReplyRequired((Message)messageContext.getRawMessage()) );
  }
  /**
   * Receives Messages from the JMS Provider. It checks the message header to determine the type of
   * message received. Based on the type, a MessageContext is created to facilitate access to the
   * transport specific message. Once the MessageContext is determined this routine delegates
   * handling of the message to the chain of MessageHandlers.
   * 
   * @param aMessage
   *          - JMS Message containing header and payload
   * @param aJmsSession
   *          - JMSSession object
   */
  public void onMessage(Message aMessage, Session aJmsSession) {
    String casRefId = null;

    if (isStopped()) {
      return;
    }

    try {
      // wait until message handlers are plugged in
      msgHandlerLatch.await();
    } catch (InterruptedException e) {
    }
    try {
      // wait until the controller is plugged in
      controllerLatch.await();
    } catch (InterruptedException e) {
    }
    long idleTime = 0;

    boolean doCheckpoint = false;

    String eN = endpointName;
    if (getController() != null) {
      eN = getController().getComponentName();
      if (eN == null) {
        eN = "";
      }
    }
    String command = "N/A";
    String messageType ="N/A";
    
    JmsMessageContext messageContext = null;

    int requestType = 0;
    try {
      // Wrap JMS Message in MessageContext
      messageContext = new JmsMessageContext(aMessage, endpointName);
      if (aMessage.getStringProperty(AsynchAEMessage.CasReference) == null) {
        casRefId = "CasReferenceId Not In Message";
      } else {
        casRefId = aMessage.getStringProperty(AsynchAEMessage.CasReference);
      }
      if (validMessage(aMessage)) {
        command = decodeIntToString(AsynchAEMessage.Command, aMessage
                .getIntProperty(AsynchAEMessage.Command));
        // Request or Response
        messageType = decodeIntToString(AsynchAEMessage.MessageType, aMessage
                .getIntProperty(AsynchAEMessage.MessageType));
      
        String msgSentFromIP = null;

        if (aMessage.getIntProperty(AsynchAEMessage.MessageType) == AsynchAEMessage.Response
                && aMessage.propertyExists(AsynchAEMessage.ServerIP)) {
          msgSentFromIP = aMessage.getStringProperty(AsynchAEMessage.ServerIP);
        }
        // System.out.println("***********************************************************************************"
        // +
        // "           \n**CONTROLLER::"+controller.getName()+"**** Received New Message From [ "+aMessage.getStringProperty(AsynchAEMessage.MessageFrom)+" ]**************"
        // +
        // "           \n**MSGTYPE::"+messageType+" COMMAND:"+command +
        // " Cas Reference Id::"+casRefId+
        // "           \n******************************************************************************");

        String msgFrom = (String) aMessage.getStringProperty(AsynchAEMessage.MessageFrom);
        if (controller != null && msgFrom != null) {
        	//	Send an ACK to a client. This serves dual purpose:
        	//  1 - tests for existence of temp reply queue
        	//  2 - notifies client that a CAS it sent is about to be processed by a service
        	//	ACK is sent for every request message
        	if ( ackClient(messageContext)  ) {
        		try {
        			//	Any exception while sending an ACK results in a dropped request
        			getController().getOutputChannel().sendReply(AsynchAEMessage.ServiceInfo,
                            messageContext.getEndpoint(), aMessage.getStringProperty(AsynchAEMessage.CasReference), true);
        		} catch( Exception ex) {
        			UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
		                    "produceTextMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                    "UIMAJMS_exception__WARNING", ex);
       		        //	The exception has already been logged in sendReply() method
       		        return;  // DONT PROCESS
        		}
        	}
        	if (msgSentFromIP != null) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(
                      Level.FINE,
                      CLASS_NAME.getName(),
                      "onMessage",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_recvd_new_message_with_ip__FINE",
                      new Object[] { controller.getComponentName(), msgFrom, msgSentFromIP,
                          messageType, command, casRefId });
            }
          } else {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(
                      Level.FINE,
                      CLASS_NAME.getName(),
                      "onMessage",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_recvd_new_message__FINE",
                      new Object[] { controller.getComponentName(), msgFrom, messageType, command,
                          casRefId });
            }
          }
        } else {
        }
        // Delegate processing of the message contained in the MessageContext to the
        // chain of handlers
        try {
          if (isRemoteRequest(aMessage)) {
            // Compute the idle time waiting for this request
            idleTime = getController().getIdleTime();

            // This idle time is reported to the client thus save it in the endpoint
            // object. This value will be fetched and added to the outgoing reply.
            messageContext.getEndpoint().setIdleTime(idleTime);
          }
        } catch (Exception e) {
        }

        // Determine if this message is a request and either GetMeta, CPC, or Process
        doCheckpoint = isCheckpointWorthy(aMessage);
        requestType = aMessage.getIntProperty(AsynchAEMessage.Command);
        // Checkpoint
        if (doCheckpoint) {
          getController().beginProcess(requestType);
        }

        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINEST,
                  CLASS_NAME.getName(),
                  "onMessage",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_new_msg_in__FINEST",
                  new Object[] { getController().getComponentName(), msgFrom, command, messageType,
                      casRefId });
        }
        if (handler != null) {
          handler.handle(messageContext);
        }
      } else {
        if (!isStaleMessage(aMessage)) {
          controller.getErrorHandlerChain().handle(new InvalidMessageException(),
                  HandlerBase.populateErrorContext(messageContext), controller);
        }
      }

    } catch (Throwable t) {
      if ( t instanceof Error) {
        t.printStackTrace();
        System.err.println("\n\n\n\tUIMA AS Service Caught Java Error. Exiting via System.exit(2)");
        System.err.flush();

        System.exit(2);
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "onMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", controller.getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "onMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", t);
      }
      controller.getErrorHandlerChain().handle(t, HandlerBase.populateErrorContext(messageContext),
              controller);
    } finally {
      // Call the end checkpoint for non-aggregates. For primitives the CAS has been fully processed
      // if we are here
      if (doCheckpoint && getController() instanceof PrimitiveAnalysisEngineController) {
        getController().endProcess(requestType);
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "onMessage",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_msg_processed__FINE",
                new Object[] { getController().getComponentName(), casRefId });
      }
    }
  }
 
  private String extractNodeName(String url) {
    int startPos = url.indexOf("//");
    //  Strip the protocol
    String temp = url.substring(startPos+2);
    int endPos = temp.indexOf(":");
    //  extract hostname from Broker URL
    return temp.substring(0, endPos).trim();
  }
  public int getSessionAckMode() {
    return sessionAckMode;
  }

  public String getServerUri() {
    return brokerURL;
  }

  public synchronized void setListenerContainer(UimaDefaultMessageListenerContainer messageListener) {
    this.messageListener = messageListener;
    System.setProperty("BrokerURI", messageListener.getBrokerUrl());
    if ( messageListener.getMessageSelector() !=null && messageListener.getMessageSelector().equals("Command=2001") ) {
      brokerURL = messageListener.getBrokerUrl();
      getController().getOutputChannel().setServerURI(brokerURL);
    }
    if (!listenerContainerList.contains(messageListener)) {
      listenerContainerList.add(messageListener);
    }
    if (getController() != null) {
      try {
        getController().addInputChannel(this);
        messageListener.setController(getController());
      } catch (Exception e) {
      }
    }
  }

  public ActiveMQConnectionFactory getConnectionFactory() {
    if (messageListener == null) {
      return null;
    } else {
      return (ActiveMQConnectionFactory) messageListener.getConnectionFactory();
    }
  }

  public void ackMessage(MessageContext aMessageContext) {
    if (aMessageContext != null && sessionAckMode == Session.CLIENT_ACKNOWLEDGE) {
      try {
        ((Message) aMessageContext.getRawMessage()).acknowledge();
      } catch (Exception e) {
        
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "ackMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", controller.getComponentName());

        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "ackMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_exception__WARNING",
                  e);
        }
      }
    }
  }

  public String getBrokerURL() {
    return brokerURL;
  }

  public String getInputQueueName() {
    if (messageListener != null)
      if (messageListener.getDestination() != null) {
        return messageListener.getDestination().toString();
      } else {
        return messageListener.getDestinationName();// getEndpointName();
      }
    else {
      return "";
    }
  }

  public ServiceInfo getServiceInfo() {
    if (serviceInfo == null) {
      serviceInfo = new ServiceInfo(false,controller);
      serviceInfo.setBrokerURL(getBrokerURL());
      serviceInfo.setInputQueueName(getName());
      if ( controller == null ) {
        serviceInfo.setState(ServiceState.INITIALIZING.name());
      } else {
        if ( controller.isCasMultiplier()) {
          serviceInfo.setCASMultiplier();
        }
      }
    }
    return serviceInfo;
  }

  public void setServerUri(String serverUri) {
    brokerURL = serverUri;
    if (getController() != null && getController() instanceof AggregateAnalysisEngineController) {
      ((AggregateAnalysisEngineController) getController()).getServiceInfo()
              .setBrokerURL(brokerURL);
    } else {
      ((PrimitiveAnalysisEngineController) getController()).getServiceInfo()
              .setBrokerURL(brokerURL);
    }
  }

  private void stopChannel(UimaDefaultMessageListenerContainer mL, boolean shutdownNow) throws Exception {
    String eName = mL.getEndpointName();
    if (eName != null) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "stopChannel",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_stopping_jms_transport__INFO",
                new Object[] { eName, shutdownNow });
      }
    }
    mL.delegateStop();
    if (shutdownNow) {
        mL.destroy(shutdownNow);
    }
   
    String selector = "";
    if (mL.getMessageSelector() != null) {
      selector = " Selector:" + mL.getMessageSelector();
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "stopChannel",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_stopped_jms_transport__INFO",
                new Object[] {getController().getComponentName(), mL.getDestination(), selector });
      }
  }

  private boolean doCloseChannel(UimaDefaultMessageListenerContainer mL, int channelsToClose) {
    // Check if we are closing just the input channel
    if (channelsToClose == InputChannel.InputChannels) {
      // Fetch the listener object
      ActiveMQDestination destination = (ActiveMQDestination) mL.getListenerEndpoint();
      // if this is a listener on a temp queue return false. We need to keep all temp
      // channels open to receive replies and/or notifications to free CASes
      // Keep the temp reply channel open
      if (destination != null && destination.isTemporary()) {
        return false;
      }
    }
    return true;
  }

  public void stop(boolean shutdownNow) throws Exception {
    stop(InputChannel.CloseAllChannels, shutdownNow);
    listenerContainerList.clear();
    failedListenerMap.clear();
    if ( remoteJMXServer != null ) {
      remoteJMXServer.disconnect();
      remoteJMXServer = null;
    }
  }
  public void disconnectListenersFromQueue() throws Exception {
	for (Object listenerObject : listenerContainerList) {
	  final UimaDefaultMessageListenerContainer mL = (UimaDefaultMessageListenerContainer) listenerObject;
	  	stopChannel(mL, false);
	}	  
  }
  public void setTerminating() {
  	 if ( listenerContainerList.size() > 0 ) {
  		 //	set a global static flag to stop spring's from automatic recovery on lost connection
  		 //	This method should be called when a service is shutting down
  		 ((UimaDefaultMessageListenerContainer)listenerContainerList.get(0)).setTerminating();
   	 }
  }
  public void terminate() {
	 try {
		 if ( listenerContainerList.size() > 0 ) {
	 		 ((UimaDefaultMessageListenerContainer)listenerContainerList.get(0)).closeConnection();
		 }
	 } catch( Exception e) {
	     UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "terminate", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", e);
	 }
	  
  }
  public synchronized void stop(int channelsToClose, boolean shutdownNow) throws Exception {
	  List<UimaDefaultMessageListenerContainer> listenersToRemove = new ArrayList<UimaDefaultMessageListenerContainer>();
    for (Object listenerObject : listenerContainerList) {
      final UimaDefaultMessageListenerContainer mL = (UimaDefaultMessageListenerContainer) listenerObject;
      if (mL != null && doCloseChannel(mL, channelsToClose)) {
    	  stopChannel(mL, shutdownNow);
        // Just in case check if the container still in the list. If so, add it to
        // another list that container listeners that have been stopped and need
        // to be removed from the listenerContainerList. Removing the listener from
        // the listenerContainerList in this iterator loop is not working. If for
        // example the iterator has two elements, after the first remove from the
        // listenerContainerList, the iterator stops event though there is still
        // one element left. Process removal of listeners outside of the iterator
        // loop
        if (listenerContainerList.contains(mL)) {
			    // When last listener, close shared connection
          if ( listenerContainerList.size() == 1 ) {
            try {
              mL.closeConnection();
            } catch( Exception e) {
              e.printStackTrace();
            }
          }
          listenersToRemove.add(mL);
        }
      } else {
        if (getController() != null) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "stop",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_transport_not_stopped__INFO",
                    new Object[] { getController().getComponentName() });
          }
        }
      }
    }
    // Remove listeners from the listenerContainerList
    for (UimaDefaultMessageListenerContainer mL : listenersToRemove) {
      if ( listenerContainerList.size() == 1 ) {
        ((UimaDefaultMessageListenerContainer)listenerContainerList.get(0)).closeConnection();
      }

      listenerContainerList.remove(mL);
    }
    listenersToRemove.clear();
    if (channelsToClose == InputChannel.CloseAllChannels) {
      stopped = true;
    }
  }

  public boolean isStopped() {
    return stopped;
  }

  public int getConcurrentConsumerCount() {
    return messageListener.getConcurrentConsumers();
  }

  private void testIfBrokerRunning(String aBrokerUrl) throws Exception {
    ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(aBrokerUrl);
    // Try to create a test connection to make sure that the broker is available
    Connection testConnection = null;
    try {
      testConnection = f.createConnection();
    } catch (Exception e) {
      throw e;
    } finally {
      if (testConnection != null) {
        // close test connection. Broker is running
        testConnection.close();
      }
    }
  }

  public void createListener(String aDelegateKey, Endpoint endpointToUpdate) throws Exception {
    if (getController() instanceof AggregateAnalysisEngineController) {
      Delegate delegate = ((AggregateAnalysisEngineController) getController())
              .lookupDelegate(aDelegateKey);
      if (delegate != null) {

        UimaDefaultMessageListenerContainer newListener = new UimaDefaultMessageListenerContainer();

        testIfBrokerRunning(delegate.getEndpoint().getServerURI());
        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory(delegate.getEndpoint().getServerURI());
        newListener.setConnectionFactory(f);
        newListener.setMessageListener(this);
        newListener.setController(getController());

        TempDestinationResolver resolver = new TempDestinationResolver();
        resolver.setConnectionFactory(f);
        resolver.setListener(newListener);
        newListener.setDestinationResolver(resolver);
        newListener.setAcceptMessagesWhileStopping(true);
        org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor executor = new org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor();
        executor.setCorePoolSize(delegate.getEndpoint().getConcurrentReplyConsumers());
        executor.setMaxPoolSize(delegate.getEndpoint().getConcurrentReplyConsumers());
        executor.setQueueCapacity(delegate.getEndpoint().getConcurrentReplyConsumers());
        executor.initialize();
        newListener.setConcurrentConsumers(delegate.getEndpoint().getConcurrentReplyConsumers());
        newListener.setTaskExecutor(executor);
        newListener.initialize();
        newListener.start();
        // Wait until the resolver plugs in the destination
        while (newListener.getDestination() == null) {
          synchronized (newListener) {
            newListener.wait(100);
          }
        }
        newListener.afterPropertiesSet();
        if ( controller != null && controller.isStopped() ) {
          newListener.destroy(true);  // shutdownNow
          //  we are aborting, the controller has been stopped
          return;
        }
        // Get the endpoint object for a given delegate key from the Aggregate
        Endpoint endpoint = ((AggregateAnalysisEngineController) getController()).lookUpEndpoint(
                aDelegateKey, false);
        // Override the reply destination.
        endpoint.setDestination(newListener.getDestination());
        if ( endpointToUpdate != null) {
          endpointToUpdate.setDestination(newListener.getDestination());
        }
        Object clone = ((Endpoint_impl) endpoint).clone();
        newListener.setTargetEndpoint((Endpoint) clone);
        endpoint.setStatus(Endpoint.OK);
      }
    }
  }
  public boolean isListenerActiveOnDestination(Destination destination ) {
    for (int i = 0; i < listenerContainerList.size(); i++) {
      UimaDefaultMessageListenerContainer mListener = (UimaDefaultMessageListenerContainer) listenerContainerList
              .get(i);
      if ( mListener.getDestination() != null && 
           mListener.getDestination() == destination &&
           mListener.isRunning()) {
        return true;
      }
    }
    return false;
  }
  /**
   * Given an endpoint name returns all listeners attached to this endpoint. There can be multiple
   * listeners on an endpoint each with a different selector to receive targeted messages like
   * GetMeta and Process.
   * 
   * @param anEndpointName
   *          - name of the endpoint that is used to find associated listener(s)
   * 
   * @return - list of listeners
   */
  private UimaDefaultMessageListenerContainer[] getListenersForEndpoint(String anEndpointName) {
    List<UimaDefaultMessageListenerContainer> listeners = new ArrayList<UimaDefaultMessageListenerContainer>();
    for (int i = 0; i < listenerContainerList.size(); i++) {
      UimaDefaultMessageListenerContainer mListener = (UimaDefaultMessageListenerContainer) listenerContainerList
              .get(i);
      if (mListener.getDestinationName() != null
              && mListener.getDestinationName().equals(anEndpointName)) {
        listeners.add(mListener);
      } else if (mListener.getDestination() != null
              && mListener.getDestination().toString().equals(anEndpointName)) {
        listeners.add(mListener);
      }
    }
    if (listeners.size() > 0) {
      UimaDefaultMessageListenerContainer[] listenerArray = new UimaDefaultMessageListenerContainer[listeners
              .size()];
      listeners.toArray(listenerArray);
      return listenerArray;
    }
    return null;
  }

  /**
   * 
   */
  public void destroyListener(final String anEndpointName, String aDelegateKey) {
    // check if delegate listener has already been placed in the failed listeners list
    // If so, nothing else to do here
    if (failedListenerMap.containsKey(aDelegateKey)) {
      return;
    }
    // Fetch all associated listeners.
    final UimaDefaultMessageListenerContainer[] mListeners = getListenersForEndpoint(anEndpointName);
    if (mListeners == null) {
      return;
    }
    // Stop each listener
    for (final UimaDefaultMessageListenerContainer mListener : mListeners) {
      if (!mListener.isRunning()) {
        continue; // Already Stopped
      }

      try {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)
                && mListener.getDestination() != null) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "destroyListener", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_stop_listener__INFO",
                  new Object[] { mListener.getDestination().toString() });
        }
        mListener.stop();
        if (getController() != null) {
          Endpoint endpoint = ((AggregateAnalysisEngineController) getController()).lookUpEndpoint(
                  aDelegateKey, false);
          endpoint.setStatus(Endpoint.FAILED);
          if (mListener.getConnectionFactory() != null) {
            if (getController() instanceof AggregateAnalysisEngineController) {
              if (!failedListenerMap.containsKey(aDelegateKey)) {
                failedListenerMap.put(aDelegateKey, mListener);
                listenerContainerList.remove(mListener);
              }
            }
          }
        }
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "destroyListener", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", controller.getComponentName());

          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "destroyListener", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
        }

      }
    }
  }

  public boolean isFailed(String aDelegateKey) {
    return failedListenerMap.containsKey(aDelegateKey);
  }
  public void removeDelegateFromFailedList( String aDelegateKey ) {
    if ( failedListenerMap.containsKey(aDelegateKey ) ) {
      failedListenerMap.remove(aDelegateKey);
    }
  }
  public boolean isListenerForDestination(String anEndpointName) {
    UimaDefaultMessageListenerContainer[] mListeners = getListenersForEndpoint(anEndpointName);
    if (mListeners == null) {
      return false;
    }
    return true;
  }

}
