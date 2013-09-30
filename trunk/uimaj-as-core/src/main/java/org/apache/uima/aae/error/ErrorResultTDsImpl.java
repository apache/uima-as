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

public class ErrorResultTDsImpl implements ErrorResultTDs {
  /**
	 * 
	 */
  private static final long serialVersionUID = -5385443370047862288L;

  private List list = new ArrayList();

  public void add(TD aTD) {
    this.add(aTD, 0);
  }

  public void add(TD aTD, int anInsertPosition) {
    list.add(anInsertPosition, aTD);
  }

  public TD get(int aPosition) {
    if (aPosition >= 0 && aPosition <= list.size()) {
      return (TD) list.get(aPosition);
    }
    return null;
  }

  public Iterator iterator() {
    return list.iterator();
  }

  public static class TDImpl implements TD {
    /**
		 * 
		 */
    private static final long serialVersionUID = 2160592875336611921L;

    private String path;

    private boolean wasTerminated;

    private boolean wasDisabled;

    public TDImpl(String aPath, boolean terminated, boolean disabled) {
      path = aPath;
      wasDisabled = disabled;
      wasTerminated = terminated;
    }

    public String gatPath() {
      return path;
    }

    public void setTerminated() {
      wasTerminated = true;
    }

    public void setDisabled() {
      wasDisabled = true;
    }

    public boolean wasTerminated() {
      return wasTerminated;
    }

    public boolean wasDisabled() {
      return wasDisabled;
    }
  }
}
