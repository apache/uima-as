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

import java.io.Serializable;

import org.apache.uima.aae.error.ErrorResultTDs.TD;

public interface ErrorResult extends Serializable {
  public void setRootCause(Throwable aRootCause);

  public void addComponentKeyPath(String aComponentKeyPath);

  public void addComponentKeyPath(String aComponentKeyPath, boolean terminated, boolean disabled);

  public void setTerminated();

  public void setDisabled();

  /**
   * Returns the underlying root cause first reported as an error
   * 
   * @return Throwable
   */
  public Throwable getRootCause();

  /**
   * Returns a path consisting of a list of component key names
   * 
   * @return a path consisting of a list of component key names
   */
  public ErrorResultComponentPath getComponentKeyPath();

  /**
   * Returns true is any termination occurred with this error
   * 
   * @return true is any termination occurred with this error
   */
  public boolean wasTerminated();

  /**
   * Returns true if any disabling occured with this error
   * 
   * @return true if any disabling occured with this error
   */
  public boolean wasDisabled();

  /**
   * Returns a collection of paths to the components that were terminated or disabled
   * 
   * @return a collection of paths to the components that were terminated or disabled
   */
  public ErrorResultTDs getTDs();

}
