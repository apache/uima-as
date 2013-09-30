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

package org.apache.uima.aae.controller;

import java.io.IOException;

import org.apache.uima.resource.ManagementObject;

/**
 * Defines the JMX management interface for the C++ service.
 */
public interface UimacppServiceManagementMBean extends ManagementObject, java.io.Serializable {

  public String getQueueBrokerURL() throws IOException;

  public String getQueueName() throws IOException;

  public String getAEDescriptor() throws IOException;

  public int getAEInstances() throws IOException;

  public long getErrorsGetMeta() throws IOException;

  public long getErrorsProcessCas() throws IOException;

  public long getErrorsCPC() throws IOException;

  public long getTotalNumCasProcessed() throws IOException;

  public long getTimingGetMeta() throws IOException;

  public long getTimingCPC() throws IOException;

  public long getTimingSerialization() throws IOException;

  public long getTimingAnnotatorProcess() throws IOException;

  public long getTimingDeserialization() throws IOException;

  public long getTimingMessageProcessing() throws IOException;

  public long getTimingIdle() throws IOException;

  public void resetStats() throws IOException;

  public void increaseAEInstances(int num);

  public void decreaseAEInstances(int num);

  public void shutdown() throws IOException;
  
  public void quiesceAndShutdown() throws IOException;

}
