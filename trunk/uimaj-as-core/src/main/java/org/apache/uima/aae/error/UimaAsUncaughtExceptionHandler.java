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
package org.apache.uima.aae.error;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.util.Level;

public class UimaAsUncaughtExceptionHandler implements UncaughtExceptionHandler {
  private static final Class<UimaAsUncaughtExceptionHandler> CLASS_NAME = UimaAsUncaughtExceptionHandler.class;
  private volatile boolean handledOOMalready;
  String uimaAsService;
  
  public UimaAsUncaughtExceptionHandler(String uimaAsService) {
    this.uimaAsService = uimaAsService;
  }

  private boolean isTerminalError(Throwable t) {
    return (t instanceof OutOfMemoryError || 
            t instanceof LinkageError ||
            t instanceof NoClassDefFoundError ||
            t instanceof NoSuchMethodError );
  }
  public void uncaughtException(Thread t, Throwable e) {
    if ( !handledOOMalready ) {   // the OOM has already been reported 
      if ( e instanceof OutOfMemoryError ) {
        handledOOMalready = true;
      }
      //  the following are best effort attempts to show the errors. If the error
      //  is OOM, dumping trace or logging may cause another OOM.
      e.printStackTrace();
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
              "uncaughtException", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_uncaught_error_WARNING", new Object[] {uimaAsService,e});
      if ( isTerminalError(e) ) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "uncaughtException", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_terminal_error_WARNING",new Object[] {uimaAsService});
        System.exit(2);
      }
    } else {
      System.exit(2);  // repeated OOM, just exit
    }
  }

}
