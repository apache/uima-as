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

package org.apache.uima.aae;

import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.jmx.ServiceInfo;
import org.apache.uima.aae.message.MessageContext;

public interface InputChannel extends Channel {
  public int getSessionAckMode();

  public void ackMessage(MessageContext aMessageContext);

  public String getServerUri();

  public void setServerUri(String aServerUri);

  public String getInputQueueName();

  public ServiceInfo getServiceInfo();

  public boolean isStopped();

  public int getConcurrentConsumerCount();

  public void destroyListener(String anEndpointName, String aDelegateKey);

  public void createListener(String aDelegateKey, Endpoint endpointToUpdate) throws Exception;

  public boolean isFailed(String aDelegateKey);

  public boolean isListenerForDestination(String anEndpointName);
  
  public void removeDelegateFromFailedList( String aDelegateKey );

  public void setTerminating();
  
  public void terminate();
  
  public void disconnectListenersFromQueue() throws Exception;
}
