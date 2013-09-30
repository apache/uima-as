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
package org.apache.uima.aae.jmx.monitor;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.util.Level;

/**
 * This class implements {@link JmxMonitorListener} interface. It provides basic formatting for
 * UIMA-AS service metrics. All metrics are formatted and logged into a uima log if one is defined.
 * Otherwise, the output is dumped to stdout.
 * 
 * 
 */
public class SingleLineUimaJmxMonitorListener implements JmxMonitorListener {

  private static final Class CLASS_NAME = JmxMonitorListener.class;

  private boolean firsttime = true;

  private double lastSampleTime = 0;

  private double period;

  /**
   * Constructor 
   */
  public SingleLineUimaJmxMonitorListener() {
  }

  /**
   * Callback method called by the JmxMonitor after each checkpoint.
   * 
   * @param sampleTime
   *          - last checkpoint time
   * @param metrics
   *          - an array of ServiceMetrics objects, each holding metrics for a specific UIMA AS
   *          service.
   */
  public void onNewMetrics(long sampleTime, ServiceMetrics[] metrics) {

    String items;

    if (firsttime) {
      firsttime = false;
      items = "\tTimestamp";
      for (ServiceMetrics serviceMetrics : metrics) {
        String srvName = serviceMetrics.getServiceName();
        srvName = srvName.substring(0, srvName.indexOf("_Service Performance"));
        if (serviceMetrics.isTopLevelService()) {
          srvName = "(S)" + srvName;
        } else if (serviceMetrics.isServiceRemote()) {
          srvName = "(R)" + srvName;
        }
        items = items + "\t" + srvName + "-CPU";
        items = items + "\t" + srvName + "-Idle";
        items = items + "\t" + srvName + "-CASes";
        items = items + "\t" + srvName + "-InQ";
        if (!serviceMetrics.isTopLevelService()) {
          items = items + "\t" + srvName + "-RepQ";
        }
        if (serviceMetrics.isTopLevelService()
                || (serviceMetrics.isCasMultiplier() && serviceMetrics.isServiceRemote())) {
          items = items + "\t" + srvName + "-CPW";
        }
        if (serviceMetrics.isCasMultiplier() && !serviceMetrics.isServiceRemote()) {
          items = items + "\t" + srvName + "-CmFreeCP";
        }
        if (serviceMetrics.isTopLevelService()) {
          items = items + "\t" + srvName + "-SvcFreeCP";
        }
      }
      UIMAFramework.getLogger(CLASS_NAME).log(Level.INFO, items);
    }

    items = "\t";
    items = items + format(sampleTime / 1000000000.0);
    period = (sampleTime - lastSampleTime) / 1000000.0;
    lastSampleTime = sampleTime;
    for (ServiceMetrics serviceMetrics : metrics) {
      items = items + "\t" + format(serviceMetrics.getAnalysisTime() / period);
      items = items + "\t" + format(serviceMetrics.getIdleTime() / period);
      items = items + "\t" + serviceMetrics.getProcessCount();
      items = items + "\t" + serviceMetrics.getInputQueueDepth();
      if (!serviceMetrics.isTopLevelService()) {
        items = items + "\t" + serviceMetrics.getReplyQueueDepth();
      }
      if (serviceMetrics.isTopLevelService()) {
        items = items + "\t" + format(serviceMetrics.getCasPoolWaitTime() / period);
      } else if (serviceMetrics.isCasMultiplier()) {
        if (serviceMetrics.isServiceRemote()) {
          items = items + "\t" + format(serviceMetrics.getShadowCasPoolWaitTime() / period);
        } else {
          items = items + "\t" + serviceMetrics.getCmFreeCasInstanceCount();
        }
      }
      if (serviceMetrics.isTopLevelService()) {
        items = items + "\t" + serviceMetrics.getSvcFreeCasInstanceCount();
      }

    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
      UIMAFramework.getLogger(CLASS_NAME).log(Level.INFO, items);
    }
  }

  private String format(double value) {
    NumberFormat formatter = new DecimalFormat();
    // formatter.setRoundingMode(java.math.RoundingMode.UP);
    // formatter.setMinimumFractionDigits(0);
    // formatter.setMaximumFractionDigits(0);

    return formatter.format(value);
  }

}
