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

package org.apache.uima.adapter.jms.activemq;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.BrokerService;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.UimaEEAdminContext;
import org.apache.uima.aae.controller.ControllerCallbackListener;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.util.Level;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class UimaEEAdminSpringContext implements UimaEEAdminContext, ApplicationListener {
  private static final Class CLASS_NAME = UimaEEAdminSpringContext.class;

  private FileSystemXmlApplicationContext springContainer = null;

  private BrokerService service;

  private boolean isShutdown;

  private ConcurrentHashMap<String, ListenerEntry> listenerMap = new ConcurrentHashMap<String, ListenerEntry>();

  private ControllerCallbackListener cl;
  
  public UimaEEAdminSpringContext(FileSystemXmlApplicationContext aSpringContainer, ControllerCallbackListener listener) {
    springContainer = aSpringContainer;
    this.cl = listener;
    String beanNames[] = springContainer
            .getBeanNamesForType(org.apache.uima.adapter.jms.activemq.UimaDefaultMessageListenerContainer.class);
    for (int i = 0; beanNames != null && i < beanNames.length; i++) {
      try {
        UimaDefaultMessageListenerContainer listenerContainer = ((UimaDefaultMessageListenerContainer) springContainer
                .getBean(beanNames[i]));
        ListenerEntry listenerEntry = new ListenerEntry();
        listenerEntry.setListenerContainer(listenerContainer);
        listenerMap.put(listenerContainer.getDestinationName(), listenerEntry);
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "UimaEEAdminSpringContext", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
        }
      }
    }
  }

  public void onTerminate(String reason, EventTrigger cause) {
	  if ( cl != null ) {
		  cl.notifyOnTermination(reason, cause);
	  }
  }
  public void setBroker(BrokerService aBrokerService) {
    service = aBrokerService;
  }

  public ApplicationContext getSpringContainer() {
    return springContainer;
  }

  public int getConcurrentConsumerCount(String anEndpointName) {
    try {
      if (anEndpointName != null && anEndpointName.trim().length() > 0
              && springContainer.isActive() && listenerMap.containsKey(anEndpointName)) {
        ListenerEntry listenerEntry = null;

        listenerEntry = ((ListenerEntry) listenerMap.get(anEndpointName));
        if (listenerEntry != null && listenerEntry.isStopped() == false) {
          UimaDefaultMessageListenerContainer listenerContainer = listenerEntry
                  .getListenerContainer();
          return listenerContainer.getConcurrentConsumers();
        }
      }
    } catch (Exception e) {
    }
    return -1;
  }

  /**
   * Stops a listener thread on a given endpoint
   */
  public synchronized void stopListener(String anEndpointName) {
    try {
      if (anEndpointName != null && anEndpointName.trim().length() > 0
              && springContainer.isActive() && listenerMap.containsKey(anEndpointName)) {
        ListenerEntry listenerEntry = null;

        listenerEntry = ((ListenerEntry) listenerMap.get(anEndpointName));
        if (listenerEntry != null && listenerEntry.isStopped() == false) {
          listenerEntry.setStopped(true);
          if (listenerMap.get(anEndpointName) != null) {
            ListenerEntry entry = (ListenerEntry) listenerMap.get(anEndpointName);
            if (entry != null) {
              UimaDefaultMessageListenerContainer listenerContainer = entry.getListenerContainer();
              spinThreadForListenerShutdown(listenerContainer);
            }
          }
        }
      }
    } catch (Exception e) {
    }
  }

  private void spinThreadForListenerShutdown(
          final UimaDefaultMessageListenerContainer listenerContainer) {
    // Spin a shutdown thread to terminate listener. The thread is needed due
    // to Spring.
    new Thread() {
      public void run() {
        try {
          listenerContainer.setAutoStartup(false);
          listenerContainer.setRecoveryInterval(0);
          listenerContainer.shutdown();
          listenerContainer.destroy();
          String eName = listenerContainer.getEndpointName();
          if (eName != null) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, this.getClass().getName(),
                      "spinThreadForListenerShutdown.run()", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_stop_listener__INFO", new Object[] { eName });
            }
          }
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "run", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_exception__WARNING", e);
          }
        }
      }
    }.start();
  }

  public void onApplicationEvent(ApplicationEvent anEvent) {
    shutdown();
  }

  /**
   * Stops the Spring Container
   */
  public void shutdown() {
    if (springContainer.isActive()) {
      isShutdown = true;
      // Spin a thread so that the Spring container can shut itself down
      new Thread("Spring Container Shutdown Thread") {
        public void run() {
          try {
            springContainer.destroy();
            springContainer = null;
          } catch (Exception e) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                      "shutdown", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_exception__WARNING", e);
            }
          }
        }
      }.start();
    }
    listenerMap.clear();
  }

  public boolean isShutdown() {
    return isShutdown;
  }

  protected static class ListenerEntry {
    private boolean stopped = false;

    private UimaDefaultMessageListenerContainer listenerContainer = null;

    protected UimaDefaultMessageListenerContainer getListenerContainer() {
      return listenerContainer;
    }

    protected void setListenerContainer(UimaDefaultMessageListenerContainer listenerContainer) {
      this.listenerContainer = listenerContainer;
    }

    protected boolean isStopped() {
      return stopped;
    }

    protected void setStopped(boolean stopped) {
      this.stopped = stopped;
    }

  }

}
