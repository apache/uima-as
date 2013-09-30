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

package org.apache.uima.aae.client;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.cas.CAS;
import org.apache.uima.util.ProcessTrace;

public class UimaASProcessStatusImpl implements UimaASProcessStatus {

  private static final long serialVersionUID = -5101356145458558249L;

  ProcessTrace prT;

  HashMap eventMap = new HashMap();

  List exceptionList = new ArrayList();

  List failedList = new ArrayList();

  HashMap resultHM = new HashMap();

  private boolean isSkipped = false;

  public boolean isProcessed = true;

  private String casReferenceId;

  private String parentCasId;

  transient private CAS cas;

  private List<AnalysisEnginePerformanceMetrics> performanceMetricsList;
  
  public UimaASProcessStatusImpl(ProcessTrace p) {
    this(p, null, null);
  }

  public UimaASProcessStatusImpl(ProcessTrace p, CAS cas, String aCasReferenceId) {
    this(p, cas, aCasReferenceId, null);
  }

  public UimaASProcessStatusImpl(ProcessTrace p, CAS cas, String aCasReferenceId,
          String aParentCasReferenceId) {
    prT = p;
    casReferenceId = aCasReferenceId;
    parentCasId = aParentCasReferenceId;
    this.cas = cas;
  }

  public UimaASProcessStatusImpl(ProcessTrace p, boolean aSkip) {
    prT = p;
    isSkipped = aSkip;
  }

  public CAS getCAS() {
	  return cas;
  }
  public boolean isException() {
    if (failedList.size() > 0) {
      return true;
    } else {
      return false;
    }
  }

  public String getStatusMessage() {
    if (failedList.size() > 0)
      return "failed";
    return "success";
  }

  public List getExceptions() {
    return exceptionList;
  }

  public List getFailedComponentNames() {

    return failedList;
  }

  public void addEventStatus(String aEventName, String aResultS, Throwable aE) {
    EventLog eL = new EventLog(aResultS, aE);
    eventMap.put(aEventName, eL);
    if (!aResultS.equalsIgnoreCase("success")) {
      failedList.add(aEventName);
      exceptionList.add(aE);

    }
    resultHM.put(aEventName, aE);
  }

  public ProcessTrace getProcessTrace() {
    return prT;
  }

  public void printEventLog() {
    Iterator iter = eventMap.entrySet().iterator();
    while (iter.hasNext()) {
      Object obj = iter.next();
      String key = (String) ((Map.Entry) obj).getKey();
      EventLog eL = (EventLog) ((Map.Entry) obj).getValue();
      System.out.println(" EVENT " + key + "  Result " + eL.status);
      if (eL.exception != null) {
        eL.exception.printStackTrace();
      }

    }
    for (int j = 0; j < failedList.size(); j++) {

      System.out.println(" failed component name " + failedList.get(j));
    }
  }

  static class EventLog {
    String status;

    // Exception exception;
    Throwable exception;

    // public EventLog( String stats , Exception e ) {
    public EventLog(String stats, Throwable e) {
      status = stats;
      exception = e;
    }

  }

  /**
   * Gets whether an entity has beed skipped during processing
   * 
   * @return true if an entity was skipped, false otherwise
   */
  public boolean isEntitySkipped() {
    return isSkipped;
  }

  public String getCasReferenceId() {
    return casReferenceId;
  }

  public String getParentCasReferenceId() {
    return parentCasId;
  }

  public void setPerformanceMetrics(List<AnalysisEnginePerformanceMetrics> pm) {
    performanceMetricsList = pm;;
  }
  
  public List<AnalysisEnginePerformanceMetrics> getPerformanceMetricsList() {
    return performanceMetricsList;
  }
  
  /**
   * Show class variable names and their current values. Uses reflection to obtain a list of
   * variables from the class.
   */
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.getClass().getName());
    sb.append(" Object {");
    sb.append(System.getProperty("line.separator"));
    // Fetch all variables of this class
    Field[] fields = this.getClass().getDeclaredFields();
    // Show the name of each variable and its value
    for (Field field : fields) {
      sb.append("  ");
      try {
        sb.append(field.getName());
        sb.append(": ");
        sb.append(field.get(this));
      } catch (IllegalAccessException ex) {
        System.out.println(ex);
      }
      sb.append(System.getProperty("line.separator"));
    }
    sb.append("}");

    return sb.toString();
  }
}
