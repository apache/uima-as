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

import org.apache.uima.cas.SerialFormat;

/**
 * This class exposes client side statistics via JMX
 * 
 */
public class UimaASClientInfo implements UimaASClientInfoMBean {

  private String applicationName;

  private long idleTime;

  private long serializationTime;

  private long deserializationTime;

  private long totalCasesProcessed;

  private long totalCasesRequested;

  private long totalProcessTime;

  private long totalTimeWaitingForReply;

  private long totalResponseLatencyTime;

  private long totalTimeWaitingForCas;

  private long maxSerializationTime;

  private long maxDeserializationTime;

  private long maxIdleTime;

  private long maxProcessTime;

  private long maxTimeWaitingForReply;

  private long maxResponseLatencyTime;

  private long maxTimeWaitingForCas;

  private String endpointName;

  private int replyWindowSize;

  private int casPoolSize;

  private long _metaTimeoutErrorCount;

  private long _processTimeoutErrorCount;

  private long _processErrorCount;

  private long _metaErrorCount;

  private SerialFormat serialFormat = SerialFormat.XMI;  // default
  public synchronized void reset() {
    idleTime = 0;
    serializationTime = 0;
    deserializationTime = 0;
    totalCasesProcessed = 0;
    totalCasesRequested = 0;
    totalProcessTime = 0;
    totalTimeWaitingForReply = 0;
    totalResponseLatencyTime = 0;
    totalTimeWaitingForCas = 0;
    maxSerializationTime = 0;
    maxDeserializationTime = 0;
    maxIdleTime = 0;
    maxProcessTime = 0;
    maxTimeWaitingForReply = 0;
    maxResponseLatencyTime = 0;
    maxTimeWaitingForCas = 0;
    _metaTimeoutErrorCount = 0;
    _processTimeoutErrorCount = 0;
    _processErrorCount = 0;
    _metaErrorCount = 0;
  }

  public synchronized long getMetaErrorCount() {
    return _metaErrorCount;
  }

  public synchronized void incrementMetaErrorCount() {
    _metaErrorCount++;
  }

  public synchronized long getMetaTimeoutErrorCount() {
    return _metaTimeoutErrorCount;
  }

  public synchronized void incrementMetaTimeoutErrorCount() {
    _metaTimeoutErrorCount++;
  }

  public synchronized long getProcessTimeoutErrorCount() {
    return _processTimeoutErrorCount;
  }

  public synchronized void incrementProcessTimeoutErrorCount() {
    _processTimeoutErrorCount++;
  }

  public synchronized long getProcessErrorCount() {
    return _processErrorCount;
  }

  public synchronized void incrementProcessErrorCount() {
    _processErrorCount++;
  }

  public String getEndpointName() {
    return endpointName;
  }

  public void setEndpointName(String endpointName) {
    this.endpointName = endpointName;
  }

  public int getReplyWindowSize() {
    return replyWindowSize;
  }

  public void setReplyWindowSize(int replyWindowSize) {
    this.replyWindowSize = replyWindowSize;
  }

  public int getCasPoolSize() {
    return casPoolSize;
  }

  public void setCasPoolSize(int casPoolSize) {
    this.casPoolSize = casPoolSize;
  }

  public UimaASClientInfo() {

  }

  public String getApplicationName() {
    return applicationName;
  }

  private String format(double aValue) {
    return String.format("%,.2f ms", aValue);
  }

  public synchronized String getAverageDeserializationTime() {
    if (totalCasesProcessed > 0) {
      double floatValue = (double) ((double) deserializationTime / (double) totalCasesProcessed)
              / (double) 1000000;
      return format(floatValue);
    } else
      return "0.0";
  }

  public synchronized String getAverageIdleTime() {
    if (totalCasesProcessed > 0) {
      double floatValue = (double) ((double) idleTime / (double) totalCasesProcessed)
              / (double) 1000000;
      return format(floatValue);
    } else
      return "0.0";
  }

  public synchronized String getAverageSerializationTime() {
    if (totalCasesProcessed > 0) {
      double floatValue = (double) ((double) serializationTime / (double) totalCasesProcessed)
              / (double) 1000000;
      return format(floatValue);
    } else
      return "0.0";
  }

  public synchronized String getAverageTimeToProcessCas() {
    if (totalCasesProcessed > 0) {
      double floatValue = (double) ((double) totalProcessTime / (double) totalCasesProcessed)
              / (double) 1000000;
      return format(floatValue);
    } else
      return "0.0";
  }

  public synchronized String getAverageTimeWaitingForReply() {
    return (totalCasesProcessed > 0) ? format(((double) totalTimeWaitingForReply / (double) totalCasesProcessed)
            / (double) 1000000)
            : "0.0";
  }

  public synchronized String getAverageResponseLatencyTime() {
    return (totalCasesProcessed > 0) ? format(((double) totalResponseLatencyTime / (double) totalCasesProcessed)
            / (double) 1000000)
            : "0.0";
  }

  public synchronized String getAverageTimeWaitingForCas() {
    return (totalCasesRequested > 0) ? format(((double) totalTimeWaitingForCas / (double) totalCasesRequested)
            / (double) 1000000)
            : "0.0";
  }

  public synchronized String getMaxDeserializationTime() {
    return format((double) maxDeserializationTime / (double) 1000000);
  }

  public synchronized String getMaxIdleTime() {
    return format((double) maxIdleTime / (double) 1000000);
  }

  public synchronized String getMaxProcessTime() {
    return format((double) maxProcessTime / (double) 1000000);
  }

  public synchronized String getMaxSerializationTime() {
    return format((double) maxSerializationTime / (double) 1000000);
  }

  public synchronized String getMaxTimeWaitingForReply() {
    return format((double) maxTimeWaitingForReply / (double) 1000000);
  }

  public synchronized String getMaxResponseLatencyTime() {
    return format((double) maxResponseLatencyTime / (double) 1000000);
  }

  public synchronized String getMaxTimeWaitingForCas() {
    return format((double) maxTimeWaitingForCas / (double) 1000000);
  }

  public synchronized String getTotalDeserializationTime() {
    return format((double) deserializationTime / (double) 1000000);
  }

  public synchronized String getTotalIdleTime() {
    return format((double) idleTime / (double) 1000000);
  }

  public synchronized long getTotalNumberOfCasesProcessed() {
    return totalCasesProcessed;
  }

  public synchronized long getTotalNumberOfCasesRequested() {
    return totalCasesRequested;
  }

  public synchronized String getTotalSerializationTime() {
    return format((double) serializationTime / (double) 1000000);
  }

  public synchronized String getTotalTimeToProcess() {
    return format((double) totalProcessTime / (double) 1000000);
  }

  public synchronized String getTotalTimeWaitingForReply() {
    return format((double) totalTimeWaitingForReply / (double) 1000000);
  }

  public synchronized String getTotalResponseLatencyTime() {
    return format((double) totalResponseLatencyTime / (double) 1000000);
  }

  public synchronized String getTotalTimeWaitingForCas() {
    return format((double) totalTimeWaitingForCas / (double) 1000000);
  }

  public void setApplicationName(String anApplicationName) {
    applicationName = anApplicationName;
  }

  public synchronized void incrementTotalIdleTime(long anIdleTime) {
    if (maxIdleTime < anIdleTime) {
      maxIdleTime = anIdleTime;
    }
    idleTime += anIdleTime;
  }

  public synchronized void incrementTotalNumberOfCasesProcessed() {
    totalCasesProcessed++;
  }

  public synchronized void incrementTotalSerializationTime(long aSerializationTime) {

    if (maxSerializationTime < aSerializationTime) {
      maxSerializationTime = aSerializationTime;
    }
    serializationTime += aSerializationTime;
  }

  public synchronized void incrementTotalTimeToProcess(long aTimeToProcess) {
    if (maxProcessTime < aTimeToProcess) {
      maxProcessTime = aTimeToProcess;
    }
    totalProcessTime += aTimeToProcess;
  }

  public synchronized void incrementTotalDeserializationTime(long aDeserializationTime) {
    if (maxDeserializationTime < aDeserializationTime) {
      maxDeserializationTime = aDeserializationTime;
    }
    deserializationTime += aDeserializationTime;
  }

  public synchronized void incrementTotalTimeWaitingForReply(long aTimeWaitingForReply) {
    if (maxTimeWaitingForReply < aTimeWaitingForReply) {
      maxTimeWaitingForReply = aTimeWaitingForReply;
    }
    totalTimeWaitingForReply += aTimeWaitingForReply;
  }

  public synchronized void incrementTotalResponseLatencyTime(long aResponseLatencyTime) {
    if (maxResponseLatencyTime < aResponseLatencyTime) {
      maxResponseLatencyTime = aResponseLatencyTime;
    }
    totalResponseLatencyTime += aResponseLatencyTime;
  }

  public synchronized void incrementTotalTimeWaitingForCas(long aTimeWaitingForCas) {
    totalCasesRequested++;
    if (maxTimeWaitingForCas < aTimeWaitingForCas) {
      maxTimeWaitingForCas = aTimeWaitingForCas;
    }
    totalTimeWaitingForCas += aTimeWaitingForCas;
  }

  public SerialFormat getSerialization() {
    return serialFormat;
  }

  public void setSerialization(SerialFormat serialFormat) {
    this.serialFormat = serialFormat;
  }
}
