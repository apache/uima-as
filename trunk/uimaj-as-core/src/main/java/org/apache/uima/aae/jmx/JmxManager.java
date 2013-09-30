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

package org.apache.uima.aae.jmx;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.uima.UIMAFramework;
import org.apache.uima.internal.util.JmxMBeanAgent;
import org.apache.uima.util.Level;

public class JmxManager implements JmxManagement {
  private String jmxDomain = "";

  public JmxManager(String aDomain) {
    jmxDomain = aDomain;
  }

  /**
   * Removes all objects from the MBeanServer with t
   * 
   * @param aDomain
   */
  public void unregisterDomainObjects(String aDomain) {
    //  use class level locking to make sure one thread at time executes the code
    synchronized(JmxManager.class) {
      if (!isInitialized()) {
        return;
      }
      try {
        Set set = getMBeansInDomain(aDomain);
        Iterator it = set.iterator();
        while (it.hasNext()) {
          ObjectName on = (ObjectName) it.next();
          try {
            unregisterMBean(on);
          } catch (Exception e) {
          } // Dont care if there is an exception
        }
      } catch (Exception e) {
        UIMAFramework.getLogger().logrb(Level.WARNING, JmxMBeanAgent.class.getName(),
                "unregisterDomainObjects", LOG_RESOURCE_BUNDLE,
                "UIMA_JMX_failed_to_register_mbean__WARNING", e);
      }
    }
  }

  private Set getMBeansInDomain(String aDomain) throws Exception {
    ObjectName objName = new ObjectName(aDomain);
    return ((MBeanServer) platformMBeanServer).queryNames(objName, null);
  }

  public String getJmxDomain() {
    return jmxDomain;
  }

  public void setJmxDomain(String aJmxDomain) {
    jmxDomain = aJmxDomain;
  }

  public MBeanServer getMBeanServer() {
    if (!isInitialized()) {
      return null;
    }
    return (MBeanServer) platformMBeanServer;
  }

  public void registerMBean(Object anMBeanToRegister, ObjectName aName) throws Exception {
    if (!isInitialized()) {
      return;
    }

    try {
      //  use class level locking to make sure one thread at time executes the code
      synchronized(JmxManager.class) {
        if (((MBeanServer) platformMBeanServer).isRegistered(aName)) {
          ((MBeanServer) platformMBeanServer).unregisterMBean(aName);
        }
        ((MBeanServer) platformMBeanServer).registerMBean(anMBeanToRegister, aName);
      }
    } catch (Exception e) {
      UIMAFramework.getLogger()
              .logrb(Level.WARNING, JmxMBeanAgent.class.getName(), "registerMBean",
                      LOG_RESOURCE_BUNDLE, "UIMA_JMX_failed_to_register_mbean__WARNING", e);
    }
  }

  private boolean isInitialized() {
    if (!jmxAvailable || platformMBeanServer == null) // means we couldn't find the required classes
                                                      // and methods
    {
      UIMAFramework.getLogger().logrb(Level.CONFIG, JmxManager.class.getName(), "isInitialized",
              LOG_RESOURCE_BUNDLE, "UIMA_JMX_platform_mbean_server_not_available__CONFIG");
      return false;
    }
    return true;
  }

  public void unregisterMBean(ObjectName anMBeanToUnregister) {
    //  use class level locking to make sure one thread at time executes the code
    synchronized(JmxManager.class) {
      if (!isInitialized()) {
        return;
      }
      try {
        if (((MBeanServer) platformMBeanServer).isRegistered(anMBeanToUnregister)) {
          ((MBeanServer) platformMBeanServer).unregisterMBean(anMBeanToUnregister);
        }
      } catch (Exception e) {
        UIMAFramework.getLogger()
                .logrb(Level.WARNING, JmxMBeanAgent.class.getName(), "registerMBean",
                        LOG_RESOURCE_BUNDLE, "UIMA_JMX_failed_to_register_mbean__WARNING", e);
      }
    }
  }

  public void destroy() throws Exception {
    unregisterDomainObjects("org.apache.uima:type=ee.jms.services,*");
  }

  public void initialize(Map anInitMap) throws Exception {
  }

  /** Class and Method handles for reflection */
  private static Class mbeanServerClass;

  private static Class objectNameClass;

  private static Constructor objectNameConstructor;

  private static Method isRegistered;

  private static Method registerMBean;

  private static Method unregisterMBean;

  /**
   * Set to true if we can find the required JMX classes and methods
   */
  private static boolean jmxAvailable;

  /**
   * The platform MBean server if one is available (Java 1.5 only)
   */
  private static Object platformMBeanServer;

  /** Get class/method handles */
  static {
    try {
      mbeanServerClass = Class.forName("javax.management.MBeanServer");
      objectNameClass = Class.forName("javax.management.ObjectName");
      objectNameConstructor = objectNameClass.getConstructor(new Class[] { String.class });
      isRegistered = mbeanServerClass.getMethod("isRegistered", new Class[] { objectNameClass });
      registerMBean = mbeanServerClass.getMethod("registerMBean", new Class[] { Object.class,
          objectNameClass });
      unregisterMBean = mbeanServerClass.getMethod("unregisterMBean",
              new Class[] { objectNameClass });
      jmxAvailable = true;
    } catch (ClassNotFoundException e) {
      // JMX not available
      jmxAvailable = false;
    } catch (NoSuchMethodException e) {
      // JMX not available
      jmxAvailable = false;
    }

    // try to get platform MBean Server (Java 1.5 only)
    try {
      Class managementFactory = Class.forName("java.lang.management.ManagementFactory");
      Method getPlatformMBeanServer = managementFactory.getMethod("getPlatformMBeanServer",
              new Class[0]);
      platformMBeanServer = getPlatformMBeanServer.invoke(null, (Object[]) null);
    } catch (Exception e) {
      platformMBeanServer = null;
    }
  }

  /**
   * resource bundle for log messages
   */
  private static final String LOG_RESOURCE_BUNDLE = "org.apache.uima.impl.log_messages";

}
