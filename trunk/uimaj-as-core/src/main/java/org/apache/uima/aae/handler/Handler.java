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

package org.apache.uima.aae.handler;

import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.cas.CAS;

public interface Handler {
  public void setDelegate(Handler aHandler);

  public Handler getDelegate();

  public void removeDelegate(String aHandlerName);

  public void resequenceDelegateHandler(String aHandlerName, int aNewPositionInHandlerChain);

  public void delegate(MessageContext aMessageContext) throws AsynchAEException;

  public void invokeProcess(CAS aCAS, String casReferenceId1, String aCasReferenceId2,
          MessageContext aMessageContext, String aNewCasProducedBy) throws AsynchAEException;

  public boolean isHandlerForMessage(MessageContext aMessageContext, int anExpectedMessageType,
          int anExpectedCommand) throws AsynchAEException;

  public void handle(Object anObjectToHandle) throws AsynchAEException;

  // Deprecate below handlers
  public void handle(Object anObjectToHandle, String expectedOutputType) throws AsynchAEException;

  public boolean hasDelegateHandler();

  public String getName();

}
