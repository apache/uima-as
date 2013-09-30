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

package org.apache.uima.dde.internal.hover;

public class KeyValuePair {
  
  protected Object parent;

  protected String label;

  protected String key;
  
  protected Object value;
  
  public KeyValuePair (String key, Object value) {
//    this.parent = parent;
//    this.label = label;
    this.key = key;
    this.value = value;
  }

  public KeyValuePair (String key, Object value, Object parent, String label) {
    this.parent = parent;
    this.label = label;
    this.key = key;
    this.value = value;
  }

  /**
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key the key to set
   */
  public void setKey(String key) {
    this.key = key;
  }

  /**
   * @return the label
   */
  public String getLabel() {
    return label;
  }

  /**
   * @param label the label to set
   */
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   * @return the parent
   */
  public Object getParent() {
    return parent;
  }

  /**
   * @param parent the parent to set
   */
  public void setParent(Object parent) {
    this.parent = parent;
  }

  /**
   * @return the value
   */
  public Object getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(Object value) {
    this.value = value;
  }

}
