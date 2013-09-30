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

package org.apache.uima.aae.error;

import java.util.HashMap;
import java.util.Iterator;

public class ErrorContext {
  public static final String THROWABLE_ERROR = "ThrowableError";
  public static final String ERROR_HANDLED = "ErrorHandled";
  private boolean handleSilently = false;

  private HashMap contextMap = new HashMap();

  public void add(HashMap aCtx) {
    contextMap.putAll(aCtx);
  }

  public void add(Object key, Object value) {
    contextMap.put(key, value);
  }

  public void handleSilently(boolean silent) {
    handleSilently = silent;
  }

  public boolean silentHandling() {
    return handleSilently;
  }

  public Object get(String key) {
    if (contextMap.containsKey(key)) {
      return contextMap.get(key);
    }
    return null;
  }

  public boolean containsKey(String key) {
    return contextMap.containsKey(key);
  }

  public Iterator getIterator() {
    return contextMap.keySet().iterator();
  }

  public void remove(String key) {
    contextMap.remove(key);
  }
}
