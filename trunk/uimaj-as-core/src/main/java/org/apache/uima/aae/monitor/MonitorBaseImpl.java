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

import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.aae.monitor.statistics.Statistic;
import org.apache.uima.aae.monitor.statistics.Statistics;

public class MonitorBaseImpl implements Monitor {
  private Map componentStatistics = new HashMap();

  private Map thresholds = null;

  public void setThresholds(Map aThresholdMap) {
    thresholds = aThresholdMap;
  }

  public Map getThresholds() {
    return thresholds;
  }

  public void addStatistic(String aComponentName, Statistic aStatistic) {
    if (componentStatistics.containsKey(aComponentName)) {
      Statistics stats = getStatistics(aComponentName);
      if (!stats.containsKey(aStatistic.getName())) {
        stats.put(aStatistic.getName(), aStatistic);
      }
    } else {
      Statistics stats = new Statistics();
      stats.put(aStatistic.getName(), aStatistic);
      componentStatistics.put(aComponentName, stats);
    }
  }

  public LongNumericStatistic getLongNumericStatistic(String aComponent, String aStatisticName) {
    LongNumericStatistic countStat = (LongNumericStatistic) this.getStatistic(aComponent,
            aStatisticName);
    if (countStat == null) {
      countStat = new LongNumericStatistic(aStatisticName);
      addStatistic(aComponent, countStat);

    }
    // return (LongNumericStatistic) this.getStatistic(aComponent, aStatisticName);
    return countStat;
  }

  public synchronized void incrementCount(String aComponent, String aStatisticName) {
    LongNumericStatistic countStat = getLongNumericStatistic(aComponent, aStatisticName);
    if (countStat == null) {
      countStat = new LongNumericStatistic(aStatisticName);
      addStatistic(aComponent, countStat);
    }
    countStat.increment();

  }

  public Statistics getStatistics(String aComponentName) {
    return (Statistics) componentStatistics.get(aComponentName);
  }

  public synchronized void resetCountingStatistic(String aComponent, String aStatisticName) {
    LongNumericStatistic countStat = getLongNumericStatistic(aComponent, aStatisticName);
    if (countStat != null) {
      synchronized (countStat) {
        countStat.reset();
      }
    }
  }

  public Statistic getStatistic(String aComponentName, String aStatisticName) {

    if (componentStatistics.containsKey(aComponentName)) {
      Statistics stats = getStatistics(aComponentName);
      if (stats.containsKey(aStatisticName)) {
        return (Statistic) stats.get(aStatisticName);
      }
    } else {
      LongNumericStatistic countStat = new LongNumericStatistic(aStatisticName);
      addStatistic(aComponentName, countStat);
      return (Statistic) countStat;
    }

    return null;
  }

  /*
   * public void addStatistic(String key, Statistic aStatistic) { if ( !monitorMap.containsKey(key))
   * { monitorMap.put(key, aStatistic); } }
   * 
   * public Statistic getStatistic(String key) { if ( monitorMap.containsKey(key)) { return
   * (Statistic)monitorMap.get(key); } return null; }
   */
  public long componentMapSize() {
    if (componentStatistics != null) {
      return (long) componentStatistics.size();
    }
    return 0;
  }

  public long thresholdMapSize() {
    if (thresholds != null) {
      return thresholds.size();
    }
    return 0;

  }
}
