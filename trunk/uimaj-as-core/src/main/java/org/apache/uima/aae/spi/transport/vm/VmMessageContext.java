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

package org.apache.uima.aae.spi.transport.vm;

import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.Endpoint_impl;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.message.UIMAMessage;

public class VmMessageContext implements MessageContext {

  private Endpoint endpoint;

  private UimaVmMessage message = null;

  private String endpointName = null;

  public VmMessageContext(UimaVmMessage aMessage, String anEndpointName) throws AsynchAEException {
    message = aMessage;
    endpoint = new Endpoint_impl();
    endpointName = anEndpointName;
    try {
      String msgFrom = (String) aMessage.getStringProperty(AsynchAEMessage.MessageFrom);
      if (msgFrom != null) {
        endpoint.setEndpoint(msgFrom);
      }
      if (aMessage.containsProperty(UIMAMessage.ServerURI)) {
        String selectedServerURI = aMessage.getStringProperty(UIMAMessage.ServerURI);
        endpoint.setServerURI(selectedServerURI);
        endpoint.setRemote(endpoint.getServerURI().startsWith("vm") == false);
      }
      // Check if the client attached a special property that needs to be echoed back.
      // This enables the client to match the reply with the endpoint.
      if (aMessage.containsProperty(AsynchAEMessage.EndpointServer)) {
        endpoint.setRemote(true);
        endpoint.setEndpointServer(aMessage.getStringProperty(AsynchAEMessage.EndpointServer));
      }

    } catch (Exception e) {
      throw new AsynchAEException(e);
    }

  }

  public byte[] getByteMessage() throws AsynchAEException {
    return message.getStringCargo().getBytes();
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  public String getEndpointName() {
    return endpointName;
  }

  public long getMessageArrivalTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  public int getMessageIntProperty(String messagePropertyName) throws AsynchAEException {
    // TODO Auto-generated method stub
    return message.getIntProperty(messagePropertyName);
  }

  public long getMessageLongProperty(String messagePropertyName) throws AsynchAEException {
    return message.getLongProperty(messagePropertyName);
  }

  public Object getMessageObjectProperty(String messagePropertyName) throws AsynchAEException {
    return message.getObjectProperty(messagePropertyName);
  }

  public String getMessageStringProperty(String messagePropertyName) throws AsynchAEException {
    return message.getStringProperty(messagePropertyName);
  }

  public Object getObjectMessage() throws AsynchAEException {
    return message.getObjectProperty(AsynchAEMessage.Cargo);
  }

  public Object getRawMessage() {
    return message;
  }

  public String getStringMessage() throws AsynchAEException {
    return message.getStringCargo();
  }

  public boolean propertyExists(String key) throws AsynchAEException {
    return message.containsKey(key);
  }

  public void setMessageArrivalTime(long anArrivalTime) {
  }

  public boolean getMessageBooleanProperty(String messagePropertyName) throws AsynchAEException {
    return message.getBooleanProperty(messagePropertyName);
  }

}
