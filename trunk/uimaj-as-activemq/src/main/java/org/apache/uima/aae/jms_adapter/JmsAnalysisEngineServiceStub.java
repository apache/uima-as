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

package org.apache.uima.aae.jms_adapter;

import java.util.HashMap;
import java.util.Map;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.client.UimaASStatusCallbackListener;
import org.apache.uima.aae.client.UimaAsBaseCallbackListener;
import org.apache.uima.aae.client.UimaAsynchronousEngine;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.activemq.JmsOutputChannel;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngine_impl;
import org.apache.uima.analysis_engine.AnalysisEngineServiceStub;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.EntityProcessStatus;
import org.apache.uima.resource.Parameter;
import org.apache.uima.resource.Resource;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceProcessException;
import org.apache.uima.resource.ResourceServiceException;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.util.Level;

public class JmsAnalysisEngineServiceStub extends UimaAsBaseCallbackListener implements
        AnalysisEngineServiceStub {
  
  private static final Class CLASS_NAME = JmsAnalysisEngineServiceStub.class;

  public static final String PARAM_BROKER_URL = "brokerUrl";

  public static final String PARAM_ENDPOINT = "endpoint";

  public static final String PARAM_TIMEOUT = "timeout";

  public static final String PARAM_GETMETA_TIMEOUT = "getmetatimeout";

  public static final String PARAM_CPC_TIMEOUT = "cpctimeout";

  public static final String PARAM_RETRY = "retry";

  public static final String PARAM_BIN_SERIALIZTION = "binary_serialization";

  public static final String PARAM_IGNORE_PROCESS_ERRORS = "ignore_process_errors";

  private Object mux = new Object();

  private boolean cpcReceived;

  private boolean ignoreErrors = false;

  private int retry = 0;

  private UimaAsynchronousEngine uimaEEEngine;

  public JmsAnalysisEngineServiceStub(Resource owner, Parameter[] parameters)
          throws ResourceInitializationException {
    // read parameters
    String brokerUrl = null;
    String endpoint = null;
    int timeout = 0;
    int getMetaTimeout = 0;
    int cpcTimeout = 0;

    String binary_serialization = null;
    for (int i = 0; i < parameters.length; i++) {
      if (PARAM_BROKER_URL.equalsIgnoreCase(parameters[i].getName())) {
        brokerUrl = parameters[i].getValue();
      } else if (PARAM_ENDPOINT.equalsIgnoreCase(parameters[i].getName())) {
        endpoint = parameters[i].getValue();
      } else if (PARAM_TIMEOUT.equalsIgnoreCase(parameters[i].getName())) {
        timeout = Integer.parseInt(parameters[i].getValue());
      } else if (PARAM_BIN_SERIALIZTION.equalsIgnoreCase(parameters[i].getName())) {
        binary_serialization = parameters[i].getValue();
      } else if (PARAM_GETMETA_TIMEOUT.equalsIgnoreCase(parameters[i].getName())) {
        getMetaTimeout = Integer.parseInt(parameters[i].getValue());
      } else if (PARAM_CPC_TIMEOUT.equalsIgnoreCase(parameters[i].getName())) {
        cpcTimeout = Integer.parseInt(parameters[i].getValue());
      } else if (PARAM_RETRY.equalsIgnoreCase(parameters[i].getName())) {
        retry = Integer.parseInt(parameters[i].getValue());
        retry = (retry < 0) ? 0 : retry;
      } else if (PARAM_IGNORE_PROCESS_ERRORS.equalsIgnoreCase(parameters[i].getName())) {
        ignoreErrors = parameters[i].getValue().equalsIgnoreCase("true");
      }
    }

    // initialize UIMA EE Engine
    Map appCtxt = new HashMap();
    appCtxt.put(UimaAsynchronousEngine.ServerUri, brokerUrl);
    appCtxt.put(UimaAsynchronousEngine.ENDPOINT, endpoint);
    appCtxt.put(UimaAsynchronousEngine.CasPoolSize, 0);
    if (timeout > 0) {
      appCtxt.put(UimaAsynchronousEngine.Timeout, timeout);
    }
    if (getMetaTimeout > 0) {
      appCtxt.put(UimaAsynchronousEngine.GetMetaTimeout, getMetaTimeout);
    }
    if (cpcTimeout > 0) {
      appCtxt.put(UimaAsynchronousEngine.CpcTimeout, cpcTimeout);
    }
    if (binary_serialization != null && binary_serialization.equalsIgnoreCase("true")) {
      appCtxt.put(UimaAsynchronousEngine.SERIALIZATION_STRATEGY, "binary");
    }
    uimaEEEngine = new BaseUIMAAsynchronousEngine_impl();
    uimaEEEngine.addStatusCallbackListener(this);
    uimaEEEngine.initialize(appCtxt);
  }

  /**
   * @see org.apache.uima.resource.service.ResourceServiceStub#callGetMetaData()
   */
  public ResourceMetaData callGetMetaData() throws ResourceServiceException {
    // metadata already retrieved during initialization
    try {
      //return uimaEEEngine.getMetaData();
      
      ResourceMetaData  rmd = uimaEEEngine.getMetaData();
      if ( rmd != null ) {
        ((ProcessingResourceMetaData)rmd).getOperationalProperties().setMultipleDeploymentAllowed(true);
        return rmd;
      }
      
    } catch (ResourceInitializationException e) {
      throw new ResourceServiceException(e);
    }
    
    throw new ResourceServiceException(new Exception("Uima AS getMetaData() call failed."));
  }

  /**
   * @see org.apache.uima.analysis_engine.service.AnalysisEngineServiceStub#callGetAnalysisEngineMetaData()
   */
  public AnalysisEngineMetaData callGetAnalysisEngineMetaData() throws ResourceServiceException {
    return (AnalysisEngineMetaData) callGetMetaData();
  }

  /**
   * @see org.apache.uima.analysis_engine.service.AnalysisEngineServiceStub#callProcess(CAS)
   */
  public void callProcess(CAS aCAS) throws ResourceServiceException {
    int tries = retry;
    while (true) {
      try {
        uimaEEEngine.sendAndReceiveCAS(aCAS);
        return;
      } catch (Throwable e) {
        if (tries-- > 0) {
          UIMAFramework.getLogger().log(Level.INFO, "Retrying callProcess on remote AS service...");
          continue;
        }
        if (!ignoreErrors) {
          throw new ResourceServiceException(e);
        }
      }
    }
  }

  /**
   * @see CasObjectProcessorServiceStub#callProcessCas(CAS)
   */
  public void callProcessCas(CAS aCAS) throws ResourceServiceException {
    callProcess(aCAS);
  }

  /**
   * @see org.apache.uima.resource.service.impl.ResourceServiceStub#destroy()
   */
  public void destroy() {
    try {
      uimaEEEngine.stop();
    } catch (Exception e) {
      if (UIMAFramework.getLogger().isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger().log(Level.WARNING, e.getMessage(), e);
      }
    }
  }

  /**
   * @see org.apache.uima.collection.impl.service.CasObjectProcessorServiceStub#callBatchProcessComplete()
   */
  public void callBatchProcessComplete() throws ResourceServiceException {
    // Not supported. Do nothing, rather than throw an exception, since this is called
    // in the normal course of CPE processing.
  }

  /**
   * @see org.apache.uima.collection.impl.service.CasObjectProcessorServiceStub#callCollectionProcessComplete()
   */
  public void callCollectionProcessComplete() throws ResourceServiceException {
    try {
      cpcReceived = false;
      uimaEEEngine.collectionProcessingComplete();
      // make this routine synchronous
      synchronized (mux) {
        while (!cpcReceived) {
          try {
            mux.wait();
          } catch (InterruptedException e) {
            // Only here if something interrupts this thread
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                      "callCollectionProcessComplete", JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_exception__WARNING",
                      e);
            }
          }
        }
      }
    } catch (ResourceProcessException e) {
      throw new ResourceServiceException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.uima.aae.client.UimaASStatusCallbackListener#collectionProcessComplete(org.apache
   * .uima.collection.EntityProcessStatus)
   */
  public void collectionProcessComplete(EntityProcessStatus aStatus) {
    synchronized (mux) {
      cpcReceived = true;
      mux.notifyAll();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.uima.aae.client.UimaASStatusCallbackListener#entityProcessComplete(org.apache.uima
   * .cas.CAS, org.apache.uima.collection.EntityProcessStatus)
   */
  public void entityProcessComplete(CAS aCas, EntityProcessStatus aStatus) {
    // not used
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.uima.aae.client.UimaASStatusCallbackListener#initializationComplete(org.apache.uima
   * .collection.EntityProcessStatus)
   */
  public void initializationComplete(EntityProcessStatus aStatus) {
    // not used
  }

}
