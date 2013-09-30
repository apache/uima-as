/**
 * 
 */
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.uima.aae.error.ErrorResultTDs.TD;
import org.apache.uima.aae.error.ErrorResultTDsImpl.TDImpl;

public class ErrorResultBaseImpl implements ErrorResult {
  private static final long serialVersionUID = -964940732231472225L;

  private ErrorResultComponentPath resultPath = new ErrorResultComponentPathImpl();

  private ErrorResultTDs resultTDs = new ErrorResultTDsImpl();

  private boolean wasTerminated = false;

  private boolean wasDisabled = false;

  private Throwable rootCause;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#addComponentKeyPath(java.lang.String)
   */
  public void addComponentKeyPath(String aComponentKeyPath) {
    addComponentKeyPath(aComponentKeyPath, false, false);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#addComponentKeyPath(java.lang.String)
   */
  public void addComponentKeyPath(String aComponentKeyPath, boolean terminated, boolean disabled) {
    resultPath.add(aComponentKeyPath, 0);
    wasTerminated = terminated;
    wasDisabled = disabled;
    if (wasTerminated || wasDisabled) {
      TD td = new TDImpl(aComponentKeyPath, wasTerminated, wasDisabled);
      resultTDs.add(td);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#getComponentKeyPath()
   */
  public ErrorResultComponentPath getComponentKeyPath() {
    return resultPath;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#getRootCause()
   */
  public Throwable getRootCause() {
    return rootCause;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#getTDs()
   */
  public ErrorResultTDs getTDs() {
    return resultTDs;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#setDisabled()
   */
  public void setDisabled() {
    wasDisabled = true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#setRootCause(java.lang.Throwable)
   */
  public void setRootCause(Throwable aThrowable) {
    Throwable t = aThrowable, prev = null;
    while ((t = t.getCause()) != null) {
      prev = t;
    }
    rootCause = (prev == null) ? aThrowable : prev;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#setTerminated()
   */
  public void setTerminated() {
    wasTerminated = true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#wasDisabled()
   */
  public boolean wasDisabled() {
    return wasDisabled;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.aae.error.ErrorResult#wasTerminated()
   */
  public boolean wasTerminated() {
    return wasTerminated;
  }


}
