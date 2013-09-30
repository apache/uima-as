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

package org.apache.uima.aae.jmx;

import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.util.Formatter;

import org.apache.uima.aae.controller.AnalysisEngineController;

public class ServicePerformance implements ServicePerformanceMBean {
  private static final long serialVersionUID = 1L;

  private static final String label = "Service Performance";

  private long idleTime = 0;

  private long numberOfCASesProcessed = 0;

  private long casDeserializationTime = 0;

  private long casSerializationTime = 0;

  private long analysisTime = 0;

  private long maxSerializationTime = 0;

  private long maxDeserializationTime = 0;

  private long maxAnalysisTime = 0;

  private long casPoolWaitTime = 0;

  private long shadowCasPoolWaitTime = 0;

  private long timeSpentInCMGetNext = 0;

  private Object sem = new Object();

  private AnalysisEngineController controller;

  private boolean isRemoteDelegate = false;

  private long uptime = System.nanoTime();

  long lastUpdate = System.nanoTime();

  private int processThreadCount = 1;

  private Object waitmux = new Object();

  private boolean waitingForCAS = false;

  private long totalWaitTimeForCAS = 0;

  private long lastCASWaitTimeUpdate = 0;

  private Object shadowPoolMux = new Object();

  private boolean waitingForSPCAS = false;

  private long lastSPCASWaitTimeUpdate = 0;

  private Object getNextMux = new Object();

  private boolean waitingInGetNext = false;

  private long lastGetNextWaitTimeUpdate = 0;

  private long totalGetNextWaitTime = 0;

  private int replyThreadCount = 1;
  

  public ServicePerformance() {
  }

  public ServicePerformance(AnalysisEngineController aController) {
    controller = aController;
  }

  public void setRemoteDelegate() {
    isRemoteDelegate = true;
  }

  public String getLabel() {
    return label;
  }

  public synchronized void reset() {
    idleTime = 0;
    numberOfCASesProcessed = 0;
    casDeserializationTime = 0;
    casSerializationTime = 0;
    casPoolWaitTime = 0;
    shadowCasPoolWaitTime = 0;
    analysisTime = 0;
    maxSerializationTime = 0;
    maxDeserializationTime = 0;
    maxAnalysisTime = 0;
    timeSpentInCMGetNext = 0;
    uptime = System.nanoTime();
  }

  public void setIdleTime(long anIdleTime) {
    synchronized (sem) {
      idleTime = anIdleTime;
    }
  }

  public double getIdleTime() {

    if (controller != null) {
      // Force update of the idle time
      return ((double) controller.getIdleTime() / (double) 1000000);
    } else {
      synchronized (sem) {
        return ((double) idleTime / (double) 1000000);
      }

    }
  }

  public long getRawIdleTime() {
    return idleTime;
  }

  public void incrementIdleTime(long anIdleTime) {
    synchronized (sem) {
      idleTime += anIdleTime;
      lastUpdate = System.nanoTime();
    }
  }

  /**
   * Adjust the analysis time. This method is called when a reply is received from a remote
   * delegate. Each reply message containing a CAS include the current actual analysis time This is
   * not a delta, its the running analysis time.
   * 
   * @param anAnalysisTime
   */
  public synchronized void setAnalysisTime(long anAnalysisTime) {
    analysisTime = anAnalysisTime;
  }

  public synchronized void incrementAnalysisTime(long anAnalysisTime) {
    synchronized (sem) {
      if (maxAnalysisTime < anAnalysisTime) {
        maxAnalysisTime = anAnalysisTime;
      }
      analysisTime += anAnalysisTime;
    }
  }

  public synchronized double getAnalysisTime() {
    if (controller != null) {
      return ((double) controller.getAnalysisTime() / (double) 1000000);
    } else {
      synchronized (sem) {
        return (double) analysisTime / (double) 1000000;
      }

    }

  }

  public synchronized long getRawAnalysisTime() {
    if (controller != null) {
      return controller.getAnalysisTime();
    } else {
      synchronized (sem) {
        return analysisTime;
      }
    }
  }

  public long getNumberOfCASesProcessed() {
    return numberOfCASesProcessed;
  }

  public synchronized void incrementNumberOfCASesProcessed() {
    numberOfCASesProcessed++;
  }

  public synchronized double getCasDeserializationTime() {
    if (casDeserializationTime > 0) {
      return ((double) casDeserializationTime / (double) 1000000);
    }
    return 0.0;
  }

  public synchronized long getRawCasDeserializationTime() {
    return casDeserializationTime;
  }

  public synchronized void incrementCasDeserializationTime(long aCasDeserializationTime) {
    if (maxDeserializationTime < aCasDeserializationTime) {
      maxDeserializationTime = aCasDeserializationTime;
    }
    casDeserializationTime += aCasDeserializationTime;
  }

  public double getCasSerializationTime() {
    return ((double) casSerializationTime / (double) 1000000);
  }

  public long getRawCasSerializationTime() {
    return casSerializationTime;
  }

  public synchronized void incrementCasSerializationTime(long casSerializationTime) {
    if (maxSerializationTime < casSerializationTime) {
      maxSerializationTime = casSerializationTime;
    }
    this.casSerializationTime += casSerializationTime;
  }

  public double getMaxSerializationTime() {
    return (double) maxSerializationTime / (double) 1000000;
  }

  public double getMaxDeserializationTime() {
    return (double) maxDeserializationTime / (double) 1000000;
  }

  public double getMaxAnalysisTime() {
    return (double) maxAnalysisTime / (double) 1000000;
  }

  public void incrementCasPoolWaitTime(long aCasPoolsWaitTime) {
    synchronized (sem) {
      casPoolWaitTime += aCasPoolsWaitTime;
    }
  }

  public double getCasPoolWaitTime() {
    synchronized (sem) {
      if (controller != null) {
        return (double) getTimeWaitingForCAS() / (double) 1000000;
      } else {
        return (double) casPoolWaitTime / (double) 1000000;
      }
    }
  }

  public long getRawCasPoolWaitTime() {
    synchronized (sem) {
      return casPoolWaitTime;
    }
  }

  public double getShadowCasPoolWaitTime() {
    return ((double) getTimeWaitingForShadowPoolCAS() / (double) 1000000);
  }

  public double getTimeSpentInCMGetNext() {
    // Force update of the wait time
    return ((double) getTimeWaitingInGetNext() / (double) 1000000);
  }

  public void beginWaitOnCASPool() {
    synchronized (waitmux) {
      if (!waitingForCAS) {
        waitingForCAS = true;
        lastCASWaitTimeUpdate = System.nanoTime();
      }
    }
  }

  public void endWaitOnCASPool() {
    synchronized (waitmux) {
      long delta = (System.nanoTime() - lastCASWaitTimeUpdate);
      totalWaitTimeForCAS += delta;
      waitingForCAS = false;
    }
  }

  public long getTimeWaitingForCAS() {
    synchronized (waitmux) {
      long now = System.nanoTime();
      if (waitingForCAS) {
        long delta = (System.nanoTime() - lastCASWaitTimeUpdate);
        totalWaitTimeForCAS += delta;
        lastCASWaitTimeUpdate = now;
      }
      return totalWaitTimeForCAS;
    }
  }

  public void beginWaitOnShadowCASPool() {
    synchronized (shadowPoolMux) {
      if (!waitingForSPCAS) {
        waitingForSPCAS = true;
        lastSPCASWaitTimeUpdate = System.nanoTime();
      }
    }
  }

  public void endWaitOnShadowCASPool() {
    synchronized (shadowPoolMux) {
      long delta = (System.nanoTime() - lastSPCASWaitTimeUpdate);
      shadowCasPoolWaitTime += delta;
      waitingForSPCAS = false;
    }
  }

  public long getTimeWaitingForShadowPoolCAS() {
    synchronized (shadowPoolMux) {
      long now = System.nanoTime();
      if (waitingForSPCAS) {
        long delta = (System.nanoTime() - lastSPCASWaitTimeUpdate);
        shadowCasPoolWaitTime += delta;
        lastSPCASWaitTimeUpdate = now;
      }
      return shadowCasPoolWaitTime;
    }

  }

  public void beginGetNextWait() {
    synchronized (getNextMux) {
      if (!waitingInGetNext) {
        waitingInGetNext = true;
        lastGetNextWaitTimeUpdate = System.nanoTime();
      } else {

      }
    }
  }

  public void endGetNextWait() {
    synchronized (getNextMux) {
      long delta = (System.nanoTime() - lastGetNextWaitTimeUpdate);
      totalGetNextWaitTime += delta;
      waitingInGetNext = false;
    }
  }

  public long getTimeWaitingInGetNext() {
    synchronized (getNextMux) {
      long now = System.nanoTime();
      if (waitingInGetNext) {
        long delta = (System.nanoTime() - lastGetNextWaitTimeUpdate);
        totalGetNextWaitTime += delta;
        lastGetNextWaitTimeUpdate = now;
      }
      return totalGetNextWaitTime;
    }

  }

  public int getProcessThreadCount() {
    return processThreadCount;
  }

  public void setProcessThreadCount(int processThreadCount) {
    this.processThreadCount = processThreadCount;
  }
  public int getReplyThreadCount() {
    return replyThreadCount;
  }

  public void setReplyThreadCount(int replyThreadCount) {
    this.replyThreadCount = replyThreadCount;
  }

}
