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

package org.apache.uima.aae;

import java.util.Map;
import java.util.Properties;

import org.apache.uima.cas.CAS;
import org.apache.uima.resource.CasManager;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;

public interface AsynchAECasManager {
  public void addMetadata(ProcessingResourceMetaData meta);

  public ProcessingResourceMetaData getMetadata() throws ResourceInitializationException;

  public Map getMetadataAsMap() throws ResourceInitializationException;

  public void setMetadata(ProcessingResourceMetaData meta);

  public void initialize(int aCasPoolSize, String aContext) throws Exception;

  public void initialize(String aContext) throws Exception;

  public void initialize(int aCasPoolSize, String aContextName,
          Properties aPerformanceTuningSettings) throws Exception;

  public CAS getNewCas();

  public CAS getNewCas(String aContext);

  public CasManager getInternalCasManager();

  public boolean isInitialized();

  public void setInitialized(boolean initialized);

  public String getCasManagerContext();

  public ResourceManager getResourceManager();

  public void setInitialFsHeapSize(long aSizeInBytes);

  public long getInitialFsHeapSize();

}
