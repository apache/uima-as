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

package org.apache.uima.aae.message;

import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ErrorContext;

public interface MessageContext {
  public String getMessageStringProperty(String aMessagePropertyName) throws AsynchAEException;

  public int getMessageIntProperty(String aMessagePropertyName) throws AsynchAEException;

  public long getMessageLongProperty(String aMessagePropertyName) throws AsynchAEException;

  public Object getMessageObjectProperty(String aMessagePropertyName) throws AsynchAEException;

  public boolean getMessageBooleanProperty(String aMessagePropertyName) throws AsynchAEException;

  public Endpoint getEndpoint();

  public String getStringMessage() throws AsynchAEException;

  public Object getObjectMessage() throws AsynchAEException;

  public byte[] getByteMessage() throws AsynchAEException;

  public Object getRawMessage();

  public boolean propertyExists(String aKey) throws AsynchAEException;

  public void setMessageArrivalTime(long anArrivalTime);

  public long getMessageArrivalTime();

  public String getEndpointName();

}
