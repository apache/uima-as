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

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.CasDefinition;
import org.apache.uima.resource.CasManager;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.FsIndexCollection;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;
import org.apache.uima.util.Level;
import org.apache.uima.util.XMLInputSource;

public class AsynchAECasManager_impl implements AsynchAECasManager {
  private static final Class CLASS_NAME = AsynchAECasManager_impl.class;

  private String contextName;

  private CasManager casManager;

  private int casPoolSize = 1;

  private volatile boolean initialized;

  private Map descriptorMap;

  private Map processingResourceMap = new HashMap();

  private ResourceManager resourceManager;

  private long initialHeapSize = 0;// 2000000; // Default

  boolean disableJCasCache = false;
  
  public AsynchAECasManager_impl(ResourceManager aResourceManager, Map aDescriptorMap) {
    this(aResourceManager);
    descriptorMap = aDescriptorMap;
  }

  public AsynchAECasManager_impl(ResourceManager aResourceManager) {
    casManager = new EECasManager_impl(aResourceManager);
    aResourceManager.setCasManager(casManager);
    resourceManager = aResourceManager;
  }

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  public void initialize(String aContextName) throws Exception {

    initialize(getCasPoolSize(), aContextName);
  }

  public void initialize(int aCasPoolSize, String aContextName) throws Exception {
    initialize(aCasPoolSize, aContextName, new Properties());
  }

  public synchronized void initialize(int aCasPoolSize, String aContextName,
          Properties aPerformanceTuningSettings) throws Exception {
    if (initialHeapSize > 0 && !aPerformanceTuningSettings.containsKey(UIMAFramework.CAS_INITIAL_HEAP_SIZE)) {
      aPerformanceTuningSettings.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, Integer.valueOf(
              (int) initialHeapSize).toString());
    }
    if ( disableJCasCache ) {
      aPerformanceTuningSettings.setProperty(UIMAFramework.JCAS_CACHE_ENABLED, String.valueOf(Boolean.FALSE));
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.CONFIG)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG, CLASS_NAME.getName(), "initialize",
              UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_primary_cas_pool_init__CONFIG",
              new Object[] { aCasPoolSize, aContextName,aPerformanceTuningSettings.get(UIMAFramework.CAS_INITIAL_HEAP_SIZE) });
    }
    // Create CAS Pool for incoming messages
    casManager.defineCasPool(aContextName, aCasPoolSize, aPerformanceTuningSettings);
    contextName = aContextName;

    setInitialized(true);
  }

  public int getCasPoolSize() {
    return casPoolSize;
  }

  public void setCasPoolSize(int casPoolSize) {
    this.casPoolSize = casPoolSize;
    setInitialized(true);
  }

  public String getCasManagerContext() {
    return contextName;
  }

  public CasManager getInternalCasManager() {
    return casManager;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  public void addMetadata(ProcessingResourceMetaData meta) {
    casManager.addMetaData(meta);
  }

  public void setMetadata(ProcessingResourceMetaData meta) {
    addMetadata(meta);
  }

  /**
   * Constructs and returns a <code>ProcessingResourceMetaData</code> object that contains the type
   * system, indexes, and type priorities definitions for the CAS.
   * 
   * @return processing resource metadata object containing the relevant parts of the CAS definition
   */
  public ProcessingResourceMetaData getMetadata() throws ResourceInitializationException {
    CasDefinition casDefinition = casManager.getCasDefinition();
    ProcessingResourceMetaData md = UIMAFramework.getResourceSpecifierFactory()
            .createProcessingResourceMetaData();
    md.setTypeSystem(casDefinition.getTypeSystemDescription());
    md.setTypePriorities(casDefinition.getTypePriorities());
    FsIndexCollection indColl = UIMAFramework.getResourceSpecifierFactory()
            .createFsIndexCollection();
    indColl.setFsIndexes(casDefinition.getFsIndexDescriptions());
    md.setFsIndexCollection(indColl);
    return md;
  }

  public Map getMetadataAsMap() throws ResourceInitializationException {
    return processingResourceMap;
  }

  public void setMetadata(String aDescriptorName) throws Exception {
    AnalysisEngineDescription specifier = UIMAFramework.getXMLParser()
            .parseAnalysisEngineDescription(new XMLInputSource(new File(aDescriptorName)));

    AnalysisEngineMetaData meta = specifier.getAnalysisEngineMetaData();
    addMetadata(meta);
  }

  private void addProcessingResourceMetadata(String mapkey, String aDescriptorName)
          throws Exception {
    AnalysisEngineDescription specifier = UIMAFramework.getXMLParser()
            .parseAnalysisEngineDescription(new XMLInputSource(new File(aDescriptorName)));

    AnalysisEngineMetaData meta = specifier.getAnalysisEngineMetaData();
    addMetadata(meta);
    processingResourceMap.put(mapkey, meta);
  }

  public CAS getNewCas() {
    return casManager.getCas(contextName);
  }

  public CAS getNewCas(String aContext) {
    return casManager.getCas(aContext);
  }

  public void setInitialFsHeapSize(long aSizeInBytes) {
    // Heap size is defined in terms of bytes. Uima core expects number of cells.
    // Each cell is 4 bytes. Divide heapSize expressed in bytes by 4.
    initialHeapSize = aSizeInBytes / 4;
  }

  public long getInitialFsHeapSize() {
    return initialHeapSize;
  }
  
  public void setDisableJCasCache(boolean disableJCasCache) {
    this.disableJCasCache = disableJCasCache;   
  }
  public boolean getDisableJCasCache() {
    return disableJCasCache;
  }
}
