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

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.util.Level;

public class Controller implements ControllerMBean {
  private static final Class CLASS_NAME = ControllerMBean.class;

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private AnalysisEngineController controller;

  public Controller(AnalysisEngineController aController) {
    controller = aController;
  }

  public void completeProcessingAndStop() {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
              "completeProcessingAndStop", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_jmx_stop_called__INFO", new Object[] { controller.getComponentName() });
    }

    controller.quiesceAndStop();
  }

  public void stopNow() {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "stopNow",
              UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_jmx_stopNow_called__INFO",
              new Object[] { controller.getComponentName() });
    }
    controller.terminate();
  }

}