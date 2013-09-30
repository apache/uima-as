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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.MessageContext;
import org.apache.uima.aae.message.UimaMessageValidator;
import org.apache.uima.aae.spi.transport.UimaMessage;

/**
 * Wrapper for the Uima message. This wrapper is used for internal messaging between collocated Uima
 * AS service.
 * 
 * 
 */
public class UimaVmMessage extends ConcurrentHashMap<String, Object> implements UimaMessage {

  private static final long serialVersionUID = -8401361129814467875L;

  private String cargo;

  public void addIntProperty(String aPropertyKey, int value) {
    super.put(aPropertyKey, value);
  }

  public void addLongProperty(String aPropertyKey, long value) {
    super.put(aPropertyKey, value);
  }

  public void addStringProperty(String aPropertyKey, String value) {
    super.put(aPropertyKey, value);
  }

  public boolean containsProperty(String propertyKey) {
    return super.containsKey(propertyKey);
  }

  public int getIntProperty(String propertyKey) {
    Object value = super.get(propertyKey);
    if (value != null) {
      return Integer.valueOf((Integer) value);
    }
    return 0;
  }

  public String getStringProperty(String propertyKey) {
    Object value = super.get(propertyKey);
    if (value != null) {
      return (String) value;
    }
    return "";
  }

  public void addStringCargo(String aCargo) {
    cargo = aCargo;
  }

  public String getStringCargo() {
    return cargo;
  }

  public MessageContext toMessageContext(String anEndpointName) throws AsynchAEException {
    return new VmMessageContext(this, anEndpointName);
  }

  public long getLongProperty(String propertyKey) {
    Object value = super.get(propertyKey);
    if (value != null) {
      return Long.valueOf((Long) value);
    }
    return 0;
  }

  public void addBooleanProperty(String aPropertyKey, boolean value) {
    super.put(aPropertyKey, value);
  }

  public boolean getBooleanProperty(String aPropertyKey) {
    Object value = super.get(aPropertyKey);
    if (value != null) {
      return ((Boolean) value).booleanValue();
    }
    return false;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (Iterator it = entrySet().iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      if (buf.length() > 0) {
        buf.append(";");
      }
      String key = (String) entry.getKey();
      String value = null;
      buf.append(key).append("=");
      if (key.equals(AsynchAEMessage.Command)) {
        Integer val = (Integer) entry.getValue();
        value = UimaMessageValidator.decodeIntToString(AsynchAEMessage.Command, val.intValue());
        buf.append(value);
      } else if (key.equals(AsynchAEMessage.MessageType)) {
        Integer val = (Integer) entry.getValue();
        value = UimaMessageValidator.decodeIntToString(AsynchAEMessage.MessageType, val.intValue());
        buf.append(value);
      } else if (key.equals(AsynchAEMessage.Payload)) {
        Integer val = (Integer) entry.getValue();
        value = UimaMessageValidator.decodeIntToString(AsynchAEMessage.Payload, val.intValue());
        buf.append(value);
      } else {
        buf.append(entry.getValue());
      }
    }
    String cargo = getStringCargo();
    if (cargo != null && cargo.trim().length() > 0) {
      buf.append(";").append("cargo=").append(cargo);
    }
    return buf.toString();
  }

  public void addObjectProperty(String aPropertyKey, Object value) {
    super.put(aPropertyKey, value);
  }

  public Object getObjectProperty(String aPropertyKey) {
    return super.get(aPropertyKey);
  }

}
