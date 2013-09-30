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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContextAdmin;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.impl.CasManager_impl;

public class EECasManager_impl extends CasManager_impl {
  Map casPoolMap = new HashMap();
  protected long initialCasHeapSize = 0;
  protected boolean disableJCasCaching = false;
  public EECasManager_impl(ResourceManager aResourceManager) {
    super(aResourceManager);
  }

  public void disableJCasCaching() {
    disableJCasCaching = true;
  }
  public void setInitialCasHeapSize(long anInitialCasHeapSize) {
    // Heap size is defined in terms of bytes. Uima core expects number of cells.
    // Each cell is 4 bytes. Divide heapSize expressed in bytes by 4.
    initialCasHeapSize = anInitialCasHeapSize / 4;
  }

  public void defineCasPool(String aRequestorContextName, int aMinimumSize,
    Properties aPerformanceTuningSettings) throws ResourceInitializationException {
    if (aPerformanceTuningSettings == null) {
      aPerformanceTuningSettings = new Properties();
    }
    if (initialCasHeapSize > 0) {
      aPerformanceTuningSettings.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, String.valueOf(
              initialCasHeapSize));
    }
    if ( disableJCasCaching ) {
      aPerformanceTuningSettings.setProperty(UIMAFramework.JCAS_CACHE_ENABLED, String.valueOf(
              Boolean.FALSE));
    }
    synchronized (CasManager_impl.class) {
      super.defineCasPool(aRequestorContextName, aMinimumSize, aPerformanceTuningSettings);
    }
  }

  @Override
  public void defineCasPool(UimaContextAdmin aRequestorContext, int aMinimumSize,
    Properties aPerformanceTuningSettings) throws ResourceInitializationException {
    // synchronize on class level lock for mutual exclusion between different instances of the
    // object
    if (aPerformanceTuningSettings == null) {
      aPerformanceTuningSettings = new Properties();
    }
    if (initialCasHeapSize > 0) {
      aPerformanceTuningSettings.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE,
              String.valueOf(initialCasHeapSize));
    }
    if ( disableJCasCaching ) {
      aPerformanceTuningSettings.setProperty(UIMAFramework.JCAS_CACHE_ENABLED, String.valueOf(
              Boolean.FALSE));
    }
    synchronized (CasManager_impl.class) {
      super.defineCasPool(aRequestorContext, aMinimumSize, aPerformanceTuningSettings);
    }
  }

  public void setPoolSize(String aRequestorContextName, int aSize) {
    casPoolMap.put(aRequestorContextName, aSize);

  }

  public int getCasPoolSize(String aRequestorContextName, int aMinimumSize) {
    int theSize = aMinimumSize;
    if (casPoolMap.containsKey(aRequestorContextName)) {
      theSize += ((Integer) casPoolMap.get(aRequestorContextName)).intValue() - 1;
    }
    return theSize;
  }

  protected void finalize() throws Throwable {
    super.finalize();
    getCasToCasPoolMap().clear();
    casPoolMap.clear();
  }

  public void cleanUp() {
  }
}
