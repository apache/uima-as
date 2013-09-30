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

import java.util.Map;

import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.aae.monitor.statistics.Statistic;
import org.apache.uima.aae.monitor.statistics.Statistics;

public interface Monitor {
  public static final String MetadataRequestTimeoutCount = "MetadataRequestTimeoutCount";

  public static final String ProcessRequestTimeoutCount = "ProcessRequestTimeoutCount";

  public static final String ErrorCount = "ErrorCount";

  public static final String ProcessErrorCount = "ProcessErrorCount";

  public static final String TotalProcessErrorCount = "TotalProcessErrorCount";

  public static final String ProcessErrorRetryCount = "ProcessErrorRetryCount";

  public static final String GetMetaErrorCount = "GetMetaErrorCount";

  public static final String GetMetaErrorRetryCount = "GetMetaErrorRetryCount";

  public static final String CpCErrorCount = "CpcErrorCount";

  public static final String ProcessCount = "ProcessCount";

  public static final String IdleTime = "IdleTime";

  public static final String TotalDeserializeTime = "TotalDeserializeTime";

  public static final String TotalSerializeTime = "TotalSerializeTime";

  public static final String TotalAEProcessTime = "TotalAEProcessTime";

  public void addStatistic(String key, Statistic aStatistic);

  // public Statistic getStatistic( String key );

  public Statistic getStatistic(String aComponentName, String aStatisticName);

  public Statistics getStatistics(String aComponentName);

  public LongNumericStatistic getLongNumericStatistic(String aComponent, String aStatisticName);

  public void resetCountingStatistic(String aComponent, String aStatisticName);

  public void incrementCount(String aComponent, String aStatisticName);

  public void setThresholds(Map aThresholdMap);

  public Map getThresholds();

  public long componentMapSize();

  public long thresholdMapSize();

}
