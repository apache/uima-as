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

package org.apache.uima.aae.monitor;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.aae.monitor.statistics.Statistic;
import org.apache.uima.aae.monitor.statistics.Statistics;

public class BaseMonitor implements Monitor {
  private Map thresholds = null;

  public BaseMonitor(String jmxServerURI) throws Exception {
  }

  public void setThresholds(Map aThresholdMap) {
    thresholds = aThresholdMap;
  }

  public Map getThresholds() {
    return thresholds;
  }

  public void addStatistic(String aName, Statistic aStatistic, Endpoint anEndpoint) {
  }

  public Statistic getStatistic(String key) {
    // TODO Auto-generated method stub
    return null;
  }

  public void addStatistic(String key, Statistic aStatistic) {
    // TODO Auto-generated method stub

  }

  public LongNumericStatistic getLongNumericStatistic(String aComponent, String aStatisticName) {
    // TODO Auto-generated method stub
    return null;
  }

  public Statistic getStatistic(String aComponentName, String aStatisticName) {
    // TODO Auto-generated method stub
    return null;
  }

  public Statistics getStatistics(String aComponentName) {
    // TODO Auto-generated method stub
    return null;
  }

  public void incrementCount(String aComponent, String aStatisticName) {
    // TODO Auto-generated method stub

  }

  public void resetCountingStatistic(String aComponent, String aStatisticName) {
    // TODO Auto-generated method stub

  }

  public long componentMapSize() {
    return 0;
  }

  public long thresholdMapSize() {
    return 0;
  }

}
