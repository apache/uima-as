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

package org.apache.uima.adapter.jms.message;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.Endpoint_impl;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.message.UIMAMessage;
import org.apache.uima.cas.SerialFormat;

public class JmsMessageContext implements MessageContext {
  private static final Class CLASS_NAME = JmsMessageContext.class;

  private Message message;

  private Endpoint endpoint;

  private long messageArrivalTime = 0L;

  private String endpointName;

  public JmsMessageContext() {
    endpoint = new Endpoint_impl();

  }

  /**
   * Each new message could arrive from a new endpoint (e.g., a new client connecting to a service).
   * The constructor constructs a new endpoint to represent this.
   * @param aMessage
   * @param anEndpointName
   * @throws AsynchAEException
   */
  public JmsMessageContext(Message aMessage, String anEndpointName) throws AsynchAEException {
    this();
    endpointName = anEndpointName;
    message = aMessage;
    try {
      if (aMessage instanceof BytesMessage) {
        endpoint.setSerialFormat(SerialFormat.BINARY);  // may be inaccurate - will fix after deserializing
      } else if (aMessage instanceof TextMessage) {
        endpoint.setSerialFormat(SerialFormat.XMI);
      }

      String msgFrom = (String) aMessage.getStringProperty(AsynchAEMessage.MessageFrom);
      if (msgFrom != null) {
        endpoint.setEndpoint(msgFrom);
      }
      if (aMessage.getJMSReplyTo() != null) {
        endpoint.setDestination(aMessage.getJMSReplyTo());
      }
      if (aMessage.propertyExists(UIMAMessage.ServerURI)) {
        endpoint.setServerURI(aMessage.getStringProperty(UIMAMessage.ServerURI));
        endpoint.setRemote(endpoint.getServerURI().startsWith("vm") == false);
      }
      // Check if the client attached a special property that needs to be echoed back.
      // This enables the client to match the reply with the endpoint.
      if (aMessage.propertyExists(AsynchAEMessage.EndpointServer)) {
        endpoint.setRemote(true);
        endpoint.setEndpointServer(aMessage.getStringProperty(AsynchAEMessage.EndpointServer));
      }

    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  public String getEndpointName() {
    return endpointName;
  }

  public boolean propertyExists(String aKey) throws AsynchAEException {
    try {
      return message.propertyExists(aKey);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public Object getRawMessage() {
    return message;
  }

  public void setMessageArrivalTime(long anArrivalTime) {
    messageArrivalTime = anArrivalTime;
  }

  public long getMessageArrivalTime() {
    return messageArrivalTime;
  }

  public byte[] getByteMessage() throws AsynchAEException {
    try {
      if (message instanceof BytesMessage) {
        long payloadSize = ((BytesMessage) message).getBodyLength();
        byte[] payloadByteArray = new byte[(int) payloadSize];
        ((BytesMessage) message).readBytes(payloadByteArray);
        return payloadByteArray;
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
    return new byte[0];
  }

  public int getMessageIntProperty(String aMessagePropertyName) throws AsynchAEException {
    try {
      return message.getIntProperty(aMessagePropertyName);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  public long getMessageLongProperty(String aMessagePropertyName) throws AsynchAEException {
    try {
      return message.getLongProperty(aMessagePropertyName);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  public Object getMessageObjectProperty(String aMessagePropertyName) throws AsynchAEException {
    try {
      return message.getObjectProperty(aMessagePropertyName);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  public String getMessageStringProperty(String aMessagePropertyName) throws AsynchAEException {
    try {
      return message.getStringProperty(aMessagePropertyName);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  public boolean getMessageBooleanProperty(String aMessagePropertyName) throws AsynchAEException {
    try {
      return message.getBooleanProperty(aMessagePropertyName);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  public Object getObjectMessage() throws AsynchAEException {
    try {
      if (message instanceof ObjectMessage) {
        return ((ObjectMessage) message).getObject();
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
    return null;
  }

  public String getStringMessage() throws AsynchAEException {
    try {
      if (message instanceof TextMessage) {
        return ((TextMessage) message).getText();
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
    return null;
  }

}
