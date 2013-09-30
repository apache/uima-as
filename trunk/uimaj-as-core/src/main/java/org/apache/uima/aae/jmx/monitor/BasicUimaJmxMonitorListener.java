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
public class BasicUimaJmxMonitorListener implements JmxMonitorListener {

  private static final Class CLASS_NAME = JmxMonitorListener.class;

  private int maxNameLength = 0;

  /**
   * Constructor
   * 
   * @param aMaxNameLength
   *          - the longest name of the UIMA-AS service. This is use to pad other names so that the
   *          output is easier to read.
   * 
   */
  public BasicUimaJmxMonitorListener(int aMaxNameLength) {
    maxNameLength = aMaxNameLength;
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

    for (ServiceMetrics serviceMetrics : metrics) {
      // Log metrics including shadow CAS pool metric for remote CAS multiplier. Filter out the top
      // level service
      if (serviceMetrics.isCasMultiplier() && serviceMetrics.isServiceRemote()
              && !serviceMetrics.isTopLevelService()) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.INFO,
                  CLASS_NAME.getName(),
                  "run",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_idle_time_shadow_cas_pool_INFO",
                  new Object[] { format((double) sampleTime / 1000000),
                      padName(serviceMetrics.getServiceName()), serviceMetrics.isCasMultiplier(),
                      serviceMetrics.isServiceRemote(), format(serviceMetrics.getIdleTime()),
                      serviceMetrics.getProcessCount(), serviceMetrics.getInputQueueDepth(),
                      serviceMetrics.getReplyQueueDepth(),
                      format(serviceMetrics.getShadowCasPoolWaitTime()),
                      format(serviceMetrics.getAnalysisTime()),
                      serviceMetrics.getProcessThreadCount(),
                      serviceMetrics.getCmFreeCasInstanceCount(),
                      serviceMetrics.getSvcFreeCasInstanceCount() });
        }
      } else {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.INFO,
                  CLASS_NAME.getName(),
                  "run",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_idle_time_cas_pool_INFO",
                  new Object[] { format((double) sampleTime / 1000000),
                      padName(serviceMetrics.getServiceName()), serviceMetrics.isCasMultiplier(),
                      serviceMetrics.isServiceRemote(), format(serviceMetrics.getIdleTime()),
                      serviceMetrics.getProcessCount(), serviceMetrics.getInputQueueDepth(),
                      serviceMetrics.getReplyQueueDepth(),
                      format(serviceMetrics.getCasPoolWaitTime()),
                      format(serviceMetrics.getAnalysisTime()),
                      serviceMetrics.getProcessThreadCount(),
                      serviceMetrics.getCmFreeCasInstanceCount(),
                      serviceMetrics.getSvcFreeCasInstanceCount() });
        }
      }
    }
    // Log group delimiter to make it easier to see metrics for this interval
    onNewSamplingInterval();
  }

  public void onNewSamplingInterval() {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "run",
              UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_marker_INFO", new Object[] {});
    }
  }

  private String format(double value) {
    NumberFormat formatter = new DecimalFormat();
    formatter.setMinimumFractionDigits(0);
    formatter.setMaximumFractionDigits(0);

    return formatter.format(value);
  }

  private String format2(double value) {
    NumberFormat formatter = new DecimalFormat("0.00");

    return formatter.format(value);
  }

  private String padName(String aName) {
    StringBuffer name = new StringBuffer(aName);
    if (aName.length() < maxNameLength) {
      for (int i = aName.length(); i < maxNameLength; i++) {
        name.append(' ');
      }
    }
    return name.toString();
  }

}
