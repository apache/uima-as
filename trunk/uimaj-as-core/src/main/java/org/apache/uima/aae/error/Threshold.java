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

import java.util.Arrays;

public class Threshold {
  private int threshold;

  private String action;

  private int window;

  private int maxRetries;

  private boolean continueOnRetryFailure;

  private long errorCount;

  private long errorSequences[];

  public Threshold() {
  }

  // Copy constructor used when a duplicate object has R/W storage so cannot be shared
  private Threshold(Threshold t) {
    window = t.window;
    threshold = t.threshold;
    action = t.action;
    maxRetries = t.maxRetries;
    continueOnRetryFailure = t.continueOnRetryFailure;
    if (window >= threshold && threshold > 1) {
      errorSequences = new long[threshold - 1];
      Arrays.fill(errorSequences, -window);
    }
  }

  public long getWindow() {
    return window;
  }

  public void setWindow(long aWindow) {
    window = (int) aWindow;
  }

  public long getThreshold() {
    return threshold;
  }

  public void setThreshold(long aCount) {
    threshold = (int) aCount;
  }

  public Threshold initialize() {
    // Need to save error sequences if watching for more than 1 error in a window
    // If first use of this instance, initialize array with values outside the window
    // If shared by another delegate create a copy
    if (window >= threshold && threshold > 1) {
      if (errorSequences == null) {
        errorSequences = new long[threshold - 1];
        Arrays.fill(errorSequences, -window);
        return this;
      } else {
        return new Threshold(this);     // Original in use so make a copy
      } 
    } else {
       return this;
   }
  }

  public boolean exceeded(long value) {
    if (threshold == 0) {
      return false;
    }
    return (value >= threshold - 1);
  }

  public boolean exceededWindow(long casCount) {
    if (threshold == 0) {
      return false;
    }
    ++errorCount;

    // If no window check if total errors have REACHED the threshold
    if (errorSequences == null) {
      return (errorCount >= threshold);
    }
    // Insert in array by replacing one that is outside the window.
    int i = threshold - 1;
    while (--i >= 0) {
      if (errorSequences[i] <= casCount - window) {
        errorSequences[i] = casCount;
        return false;
      }
    }
    // If insert fails then have reached threshold.
    // Should not be called again after returning true as may return false!
    // But may be called again if no action specified, but then it doesn't matter.
    return true;
  }

  public boolean maxRetriesExceeded(long value) {
    return (value >= maxRetries);
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public boolean getContinueOnRetryFailure() {
    return continueOnRetryFailure;
  }

  public void setContinueOnRetryFailure(boolean continueOnRetryFailure) {
    this.continueOnRetryFailure = continueOnRetryFailure;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }
}
