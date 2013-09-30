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

package org.apache.uima.dde.internal.provider;

public class NameValuePair {
  
  static final public int  STATUS_NON_EDITABLE     = 0x0001;   // cannot be edited
  
  protected int statusFlags = 0;
  
  protected Object parent;

  protected int id;

  protected String name;

  protected Object value;

  protected Object type; // Java primitive class: Integer ,...

  public NameValuePair(Object parent, int id, String name, Object value, Object type) {
    this.parent = parent;
    this.id = id;
    this.name = name;
    this.value = value;
    this.type = type;
  }
  
  

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name    the name to set
   *          
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the type
   */
  public Object getType() {
    return type;
  }

  /**
   * @param type
   *          the type to set
   */
  public void setType(Object type) {
    this.type = type;
  }

  /**
   * @return the value
   */
  public Object getValue() {
    return value;
  }

  /**
   * @param value
   *          the value to set
   */
  public void setValue(Object value) {
    this.value = value;
  }

  /**
   * @return the parent
   */
  public Object getParent() {
    return parent;
  }

  /**
   * @param parent
   *          the parent to set
   */
  public void setParent(Object parent) {
    this.parent = parent;
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @param id
   *          the id to set
   */
  public void setId(int id) {
    this.id = id;
  }



  /**
   * @return the statusFlags
   */
  public int getStatusFlags() {
    return statusFlags;
  }



  /**
   * @param statusFlags the statusFlags to set
   */
  public void setStatusFlags(int statusFlags) {
    this.statusFlags = statusFlags;
  }

}
