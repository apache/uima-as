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
package org.apache.uima.aae.spi.transport;

import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.message.MessageContext;

public interface UimaMessage {

  public void addIntProperty(String aPropertyKey, int value);

  public void addStringProperty(String aPropertyKey, String value);

  public void addLongProperty(String aPropertyKey, long value);

  public int getIntProperty(String aPropertyKey);

  public long getLongProperty(String aPropertyKey);

  public String getStringProperty(String aPropertyKey);

  public boolean containsProperty(String aPropertyKey);

  public void addStringCargo(String aCargo);

  public String getStringCargo();

  public void addBooleanProperty(String aPropertyKey, boolean value);

  public boolean getBooleanProperty(String aPropertyKey);

  public void addObjectProperty(String aPropertyKey, Object value);

  public Object getObjectProperty(String aPropertyKey);

  public MessageContext toMessageContext(String anEndpointName) throws AsynchAEException;
}
