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
package org.apache.uima.aae.jmx.monitor;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.jmx.ServiceInfoMBean;
import org.apache.uima.aae.jmx.ServicePerformanceMBean;
import org.apache.uima.aae.spi.transport.vm.UimaVmQueueMBean;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.util.Level;
import org.apache.uima.util.impl.CasPoolManagementImplMBean;

/**
 * Collects metrics from UIMA-AS Service MBeans at defined intervals and passes the metrics for
 * formatting to the registered {@link JmxMonitorListener}
 * 
 */
public class JmxMonitor implements Runnable {
  private static final Class CLASS_NAME = JmxMonitor.class;

  public static final String SamplingInterval = "uima.jmx.monitor.interval";

  public static final String FormatterListener = "uima.jmx.monitor.formatter";

  private boolean running = false;

  private MBeanServerConnection mbsc;

  private ObjectName uimaServicePattern;

  private ObjectName uimaServiceQueuePattern;

  private ObjectName uimaServiceTempQueuePattern;

  private Set<ObjectName> servicePerformanceNames;

  private long interval;

  private ConcurrentHashMap<ObjectName, StatEntry> stats = new ConcurrentHashMap<ObjectName, StatEntry>();

  private long startTime = System.nanoTime();

  private List<JmxMonitorListener> listeners = new ArrayList<JmxMonitorListener>();

  private int maxNameLength = 0;

  private boolean verbose = false;

  private ThreadMXBean threads = null;

  private RuntimeMXBean runtime = null;

  private OperatingSystemMXBean os = null;

  private int serviceCount = 0;

  /**
   * Creates a connection to an MBean Server identified by <code>remoteServerURI</code>
   * 
   * @param remoteServerURI
   *          - URI to MBeanServer
   * @return - connection to MBeanServer
   * 
   * @throws Exception
   */
  private MBeanServerConnection getServerConnection(String remoteServerURI) throws Exception {
    JMXServiceURL url = new JMXServiceURL(remoteServerURI);
    JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
    MBeanServerConnection server = jmxc.getMBeanServerConnection();
    return server;
  }

  /**
   * Registers custom {@link JmxMonitorListener}
   * 
   * @param listener
   *          - listener to receive metrics for formatting
   */
  public void addJmxMonitorListener(JmxMonitorListener listener) {
    listeners.add(listener);
  }

  private void showServerEnvironment(RuntimeMXBean runtime) {
    echo("\nRemote JVM Info: \n\tJVM::" + runtime.getVmName() + "\n\tJVM Vendor::"
            + runtime.getVmVendor() + "\n\tJVM Version::" + runtime.getVmVersion() + "\n\n");
  }

  /**
   * Passes metrics to all registered {@link JmxMonitorListener} objects
   * 
   * @param uptime
   *          - time when the metrics were collected
   * @param metrics
   *          - current metrics
   */
  public void notifyListeners(long uptime, ServiceMetrics[] metrics) {
    for (JmxMonitorListener listener : listeners) {
      listener.onNewMetrics(uptime, metrics);
    }
  }

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose() {
    verbose = true;
  }

  // Should be called after initialize()
  public int getMaxServiceNameLength() {
    return maxNameLength;
  }

  private CasPoolManagementImplMBean getServiceCasPoolMBean(String labelToMatch,
          Set<ObjectName> names) {
    for (ObjectName name : names) {
      // Check if the current name is the Service Performance MBean
      if (name.toString().endsWith(labelToMatch.trim())) {
        return (CasPoolManagementImplMBean) MBeanServerInvocationHandler.newProxyInstance(mbsc,
                name, CasPoolManagementImplMBean.class, true);
      }
    }
    return null; // not found
  }

  /**
   * Connects to a remote JMX server identified by given <code>remoteServerURI</code>. Creates
   * proxies for all UIMA AS ServicePerformance MBeans found in the JMX server registry. If UIMA AS
   * service MBean indicates a remote service, this method connects to a remote Broker and creates a
   * proxy to an input queue for that service.
   * 
   * 
   * @param remoteServerURI
   * @param samplingInterval
   * @throws Exception
   */
  public void initialize(String remoteServerURI, long samplingInterval) throws Exception {
    interval = samplingInterval;
    // Connect to the remote JMX Server
    try {
      mbsc = getServerConnection(remoteServerURI);
    } catch (Exception e) {
      System.out.println("Unable to Connect To Jmx Server. URL:" + remoteServerURI);
      throw e;
    }
    System.out.println(">>> Connected To Jmx Server. URL:" + remoteServerURI);
    // Fetch remote JVM's MXBeans
    ObjectName runtimeObjName = new ObjectName(ManagementFactory.RUNTIME_MXBEAN_NAME);
    ObjectName threadObjName = new ObjectName(ManagementFactory.THREAD_MXBEAN_NAME);
    ObjectName osObjName = new ObjectName(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
    Set<ObjectName> mbeans = mbsc.queryNames(threadObjName, null);

    mbeans = mbsc.queryNames(runtimeObjName, null);
    for (ObjectName name : mbeans) {
      runtime = (RuntimeMXBean) MBeanServerInvocationHandler.newProxyInstance(mbsc, name,
              RuntimeMXBean.class, true);
    }

    mbeans = mbsc.queryNames(osObjName, null);
    for (ObjectName name : mbeans) {
      os = (OperatingSystemMXBean) MBeanServerInvocationHandler.newProxyInstance(mbsc, name,
              OperatingSystemMXBean.class, true);
    }

    System.out.println(runtime.getVmName() + "::" + runtime.getVmVendor() + "::"
            + runtime.getVmVersion());
    // Construct query string to fetch UIMA-AS MBean names from the JMX Server registry
    uimaServicePattern = new ObjectName("org.apache.uima:type=ee.jms.services,*");
    // Construct query string to fetch Queue MBean names from the JMX Server registry
    uimaServiceQueuePattern = new ObjectName(
            "org.apache.activemq:BrokerName=localhost,Type=Queue,*");
    uimaServiceTempQueuePattern = new ObjectName(
            "org.apache.activemq:BrokerName=localhost,Type=TempQueue,*");
    // Fetch UIMA AS MBean names from the JMX Server that match the name pattern
    Set<ObjectName> names = new HashSet<ObjectName>(mbsc.queryNames(uimaServicePattern, null));
    String key = "";
    if (verbose)
      System.out.println("\nFound UIMA AS Services Managed by JMX Server:" + remoteServerURI);

    // Find all Service Performance MBeans
    for (ObjectName name : names) {

      // Set up a key for matching Service Performance MBean names
      String perfKey = "_Service Performance";
      // Check if the current name is the Service Performance MBean
      if (name.toString().endsWith(perfKey)) {
        if (servicePerformanceNames == null) {
          servicePerformanceNames = new HashSet<ObjectName>();
        }
        // Reduce the Set to Service Performance MBeans only
        servicePerformanceNames.add(name);
        // Create a proxy object for the Service Performance MBean
        ServicePerformanceMBean perfMBeanProxy = (ServicePerformanceMBean) MBeanServerInvocationHandler
                .newProxyInstance(mbsc, name, ServicePerformanceMBean.class, true);
        // Extract the service name from the MBean name
        int beginIndx = name.toString().indexOf(perfKey);
        key = name.toString().substring(0, beginIndx);

        int nameLen = name.getKeyProperty("name").length();
        if (nameLen > maxNameLength) {
          maxNameLength = nameLen;
        }

        // Create a proxy object to the Service Info MBean
        ServiceInfoMBean infoMBeanProxy = getServiceInfoMBean(names, key);
        key = key.substring(key.indexOf(",name=") + ",name=".length());
        // Create a proxy to the service CasPool object.
        CasPoolManagementImplMBean casPoolMBeanProxy = null;
        //	Get Service CAS Pool MBean Proxy. This only make sense for top level service.
        //	This is a Cas Pool for deserializing CASes from remote clients
        if (infoMBeanProxy.isTopLevel()) {
          if (infoMBeanProxy.isAggregate()) {
            casPoolMBeanProxy = getServiceCasPoolMBean("AggregateContext", names);
          } else {
            casPoolMBeanProxy = getServiceCasPoolMBean("PrimitiveAEService", names);
          }
        }
        StatEntry entry = null;
        if (casPoolMBeanProxy != null) {
          entry = new StatEntry(perfMBeanProxy, infoMBeanProxy, casPoolMBeanProxy);
        } else {
          entry = new StatEntry(perfMBeanProxy, infoMBeanProxy);
        }

        String location = "Collocated";
        // If a service is co-located in the same JVM fetch the service queue proxy
        if (infoMBeanProxy.getBrokerURL().startsWith("Embedded Broker")) {
          // Get Co-located CM Cas Pool
          if (infoMBeanProxy.isCASMultiplier()) {
            // Create a proxy to the Cas Multiplier CasPool object.
            CasPoolManagementImplMBean casPoolMBean = 
            	getServiceCasPoolMBean(infoMBeanProxy.getCmRegisteredName(), names);
            if (casPoolMBean != null) {
              entry.setCasPoolMBean(casPoolMBean);
            }
          }

          // Create a proxy to the service queue MBean
          UimaVmQueueMBean queueProxy = getQueueMBean(mbsc, infoMBeanProxy.getInputQueueName());
          if (queueProxy != null) {
            entry.setInputQueueInfo(queueProxy);
          }
          UimaVmQueueMBean replyQueueProxy = getQueueMBean(mbsc, infoMBeanProxy.getReplyQueueName());
          if (replyQueueProxy != null) {
            entry.setReplyQueueInfo(replyQueueProxy);
          }

        } else {
          // The MBean is for a remote service. Connect to this service Broker and create a proxy
          // to an input queue for the service. Assumption: the broker registers JMX server on
          // port 1099.
          location = "Remote";
          int spos = infoMBeanProxy.getBrokerURL().indexOf("//");
          int endpos = infoMBeanProxy.getBrokerURL().lastIndexOf(":");

          String remoteHostname = infoMBeanProxy.getBrokerURL().substring(spos + 2, endpos);
          if (verbose)
            System.out.println("Connecting to a remote JMX Server: " + remoteHostname + " key:"
                    + key);
          String remoteJMX = "service:jmx:rmi:///jndi/rmi://" + remoteHostname + ":1099/jmxrmi";
          MBeanServerConnection remoteServer = getServerConnection(remoteJMX);
          QueueViewMBean inputQueueProxy = getQueueMBean(remoteServer, infoMBeanProxy
                  .getInputQueueName(), uimaServiceQueuePattern);
          if (inputQueueProxy != null) {
            entry.setInputQueueInfo(inputQueueProxy);
          } else {
            System.out.println("Unable to find Input Queue:" + infoMBeanProxy.getInputQueueName()
                    + " In JMX Registry:" + remoteJMX);
          }
          QueueViewMBean replyQueueProxy = null;
          if (infoMBeanProxy.isAggregate()) {
            replyQueueProxy = getQueueMBean(mbsc, infoMBeanProxy.getReplyQueueName(),
                    uimaServiceQueuePattern);
          } else {
            replyQueueProxy = getQueueMBean(remoteServer, infoMBeanProxy.getReplyQueueName(),
                    uimaServiceTempQueuePattern);

          }
          if (replyQueueProxy != null) {
            entry.setReplyQueueInfo(replyQueueProxy);
          } else {
            System.out.println("Unable to find Reply Queue:" + infoMBeanProxy.getReplyQueueName()
                    + " In JMX Registry:" + remoteJMX);
          }
        }
        if (verbose)
          System.out.println("\nUIMA AS Service:" + key + "[>>> " + location
                  + " <<<]\n\tService Broker:" + infoMBeanProxy.getBrokerURL() + "\n\tQueue Name:"
                  + infoMBeanProxy.getInputQueueName());
        serviceCount++;
        stats.put(name, entry);

      }

    }
  }

  protected int getServiceCount() {
    return serviceCount;
  }

  /**
   * Returns a proxy object to an UIMA-AS Service Info MBean identified by a <code>key</code>
   * 
   * @param names
   *          - list of MBean names
   * @param key
   *          - target name to find in the list of MBeans
   * @return - proxy to MBean identified by <code>key</code>
   */
  private ServiceInfoMBean getServiceInfoMBean(Set<ObjectName> names, String key) {
    String target = key + "_Service Info";
    for (ObjectName name : names) {
      if (name.toString().equals(target)) {
        return (ServiceInfoMBean) MBeanServerInvocationHandler.newProxyInstance(mbsc, name,
                ServiceInfoMBean.class, true);
      }
    }
    return null;
  }

  private CasPoolManagementImplMBean getCasPoolMBean(Set<ObjectName> names, String target) {
    for (ObjectName name : names) {
      if (name.toString().endsWith(target)) {
        return (CasPoolManagementImplMBean) MBeanServerInvocationHandler.newProxyInstance(mbsc,
                name, CasPoolManagementImplMBean.class, true);
      }
    }
    return null;
  }

  private QueueViewMBean getQueueMBean(MBeanServerConnection server, String key,
          ObjectName matchPattern) throws Exception {
    Set<ObjectName> queues = new HashSet<ObjectName>(server.queryNames(matchPattern, null));
    String target = "Destination=" + key;
    for (ObjectName name : queues) {

      if (name.toString().endsWith(target)) {
        System.out.println("Creating Proxy for Queue:" + name.toString());
        return (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(server, name,
                QueueViewMBean.class, true);
      }
    }
    return null;
  }

  private UimaVmQueueMBean getQueueMBean(MBeanServerConnection server, String aPattern)
          throws Exception {
    ObjectName name = new ObjectName(aPattern);
    System.out.println("Creating Proxy for Queue:" + name.toString());
    return (UimaVmQueueMBean) MBeanServerInvocationHandler.newProxyInstance(server, name,
            UimaVmQueueMBean.class, true);
  }

  protected ServiceMetrics[] collectStats(boolean initial, long uptime) {
    int cmFreeCasInstanceCount = 0;
    ServiceMetrics[] metrics = null;
    int index = 0;
    List<ServiceMetrics> metricsList = new ArrayList<ServiceMetrics>();
    // iterate over all Performance MBeans to retrieve current metrics
    for (ObjectName name : servicePerformanceNames) {
      try {
        // Fetch previous metrics for service identified by 'name'
        StatEntry entry = stats.get(name);
        if (entry == null) {
          continue;
        }
        ServiceInfoMBean serviceInfo = entry.getServiceInfoMBeanProxy();
        CasPoolManagementImplMBean getServiceCasPoolMBeanProxy = entry
                .getServiceCasPoolMBeanProxy();
        boolean isRemote = serviceInfo.getBrokerURL().startsWith("tcp:");
        boolean topLevel = serviceInfo.isTopLevel();

        // Get the current reading from MBeans
        double idleTime = entry.getServicePerformanceMBeanProxy().getIdleTime();
        double casPoolWaitTime = entry.getServicePerformanceMBeanProxy().getCasPoolWaitTime();
        double shadowCasPoolWaitTime = entry.getServicePerformanceMBeanProxy()
                .getShadowCasPoolWaitTime();
        double analysisTime = entry.getServicePerformanceMBeanProxy().getAnalysisTime();
        long processCount = entry.getServicePerformanceMBeanProxy().getNumberOfCASesProcessed();
        QueueViewMBean inputQueueInfo = entry.getInputQueueInfo();
        QueueViewMBean replyQueueInfo = entry.getReplyQueueInfo();

		cmFreeCasInstanceCount = -1;
        if (serviceInfo.isCASMultiplier() && !isRemote && entry.getCasPoolMBean() != null) {
        	try {
        		cmFreeCasInstanceCount = entry.getCasPoolMBean().getAvailableInstances();
        	} catch( Exception e) {
        		//swallow this for now. 
        	}
        }
        long inputQueueDepth = -1;
        UimaVmQueueMBean vmInputQueueInfo = null;

        // if the service is colocated, get the bean object for the internal (non-jms) queue
        if (entry.isVmQueue) {
          vmInputQueueInfo = entry.getVmInputQueueInfo();
          if (vmInputQueueInfo != null) {
            inputQueueDepth = vmInputQueueInfo.getQueueSize();
          }
        } else {
          // service is top level and uses JMS queue
          inputQueueInfo = entry.getInputQueueInfo();
          if (inputQueueInfo != null) {
            inputQueueDepth = inputQueueInfo.getQueueSize();
          }
        }
        long replyQueueDepth = -1; // -1 means not available
        UimaVmQueueMBean vmReplyQueueInfo = null;
        if (entry.isVmQueue) {
          vmReplyQueueInfo = entry.getVmReplyQueueInfo();
          if (vmReplyQueueInfo != null) {
            replyQueueDepth = vmReplyQueueInfo.getQueueSize();
          }
        } else {
          replyQueueInfo = entry.getReplyQueueInfo();
          if (replyQueueInfo != null) {
            replyQueueDepth = replyQueueInfo.getQueueSize();
          }
        }
        // compute the delta idle time by subtracting previously reported idle time from the
        // current idle time
        double deltaIdleTime = 0;
        if (initial == false) {
          deltaIdleTime = idleTime - entry.getIdleTime();
        }

        double deltaAnalysisTime = 0;
        if (analysisTime > 0) {
          deltaAnalysisTime = analysisTime - entry.getAnalysisTime();
        }

        ServiceMetrics serviceMetrics = new ServiceMetrics();
        serviceMetrics.setCasMultiplier(entry.getServiceInfoMBeanProxy().isCASMultiplier());
        serviceMetrics.setServiceRemote(isRemote);
        serviceMetrics.setTopLevelService(topLevel);
        serviceMetrics.setTimestamp((double) uptime / 1000000);
        serviceMetrics.setIdleTime(deltaIdleTime);
        serviceMetrics.setServiceName(name.getKeyProperty("name"));
        serviceMetrics.setProcessCount(processCount - entry.getLastCASCount());
        serviceMetrics.setInputQueueDepth(inputQueueDepth);
        serviceMetrics.setReplyQueueDepth(replyQueueDepth);
        serviceMetrics.setProcessThreadCount(entry.getServicePerformanceMBeanProxy()
                .getProcessThreadCount());
        serviceMetrics.setAnalysisTime(deltaAnalysisTime);
        serviceMetrics.setCmFreeCasInstanceCount(cmFreeCasInstanceCount);
        // The service cas pool proxy is only valid for aggregates and top level primitives
		serviceMetrics.setSvcFreeCasInstanceCount(-1);
        if (getServiceCasPoolMBeanProxy != null) {
        	try {
                serviceMetrics.setSvcFreeCasInstanceCount(getServiceCasPoolMBeanProxy
                        .getAvailableInstances());
        	} catch( Exception e) {
        	}
        }
        // populate shadow CAS pool metric for remote CAS multiplier. Filter out the top level
        // service
        if (entry.getServiceInfoMBeanProxy().isCASMultiplier() && isRemote && !topLevel) {
          serviceMetrics.setShadowCasPoolWaitTime(shadowCasPoolWaitTime
                  - entry.getTotalSCPWaitTime());
          entry.setSCPWaitTime(shadowCasPoolWaitTime);
        } else {
          // populate CAS pool metric
          serviceMetrics.setCasPoolWaitTime(casPoolWaitTime - entry.getTotalCPWaitTime());
          entry.setCPWaitTime(casPoolWaitTime);
        }
        // Add metrics collected from the service to the array of metrics
        // in the current sampling (interval). The metrics array will
        // be provided to all listeners plugged into this monitor.
//        metrics[index++] = serviceMetrics;
        metricsList.add(serviceMetrics);
        // Save current metrics for the next delta
        entry.setIdleTime(idleTime);
        entry.incrementCASCount(processCount);
        entry.incrementAnalysisTime(analysisTime);

      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "collectStats", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
        }
      }
    } // for
    if ( metricsList.size() > 0 ) {
      metrics = new ServiceMetrics[metricsList.size()];
      metricsList.toArray(metrics);
    }
    return metrics;
  }

  /**
   * Retrieves metrics from UIMA-AS MBeans at defined interval.
   * 
   */
  public void run() {
    running = true;
    boolean initial = true;
    while (running) {
      long sampleStart = System.nanoTime();
      long uptime = sampleStart - startTime;
      try {
        if (mbsc != null) {
          mbsc.getMBeanCount(); // test the server
        }
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "run", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
        }
      }
      ServiceMetrics[] metrics = collectStats(initial, uptime);
      if ( metrics != null ) {
        initial = false;
        // Notify listeners with current metrics collected from MBeans
        notifyListeners(uptime, metrics);
      }

      // compute wait time till next sample
      long sampleEnd = System.nanoTime();
      long time2wait;
      long timeLost = (200000 + sampleEnd - sampleStart) / 1000000;
      if (interval > timeLost) {
        time2wait = interval - timeLost;
      } else {
        // Increase interval to a multiple of the requested interval
        long newInterval = interval * (1 + (timeLost / interval));
        time2wait = newInterval - timeLost;
      }

      synchronized (this) {
        try {
          wait(time2wait);
        } catch (InterruptedException e) {
          running = false;
        }
      }
    }

    running = false;
  }

  public boolean isRunning() {
    return running;
  }

  public void doStop() {
    running = false;
  }

  /*
   * For simplicity, we declare "throws Exception". Real programs will usually want finer-grained
   * exception handling.
   */
  public static void main(String[] args) throws Exception {

    JmxMonitor monitor = new JmxMonitor();
    if (System.getProperty("verbose") != null) {
      // Shows a list of UIMA-AS services found in a given JMX server
      monitor.setVerbose();
    }
    long samplingFrequency = 1000; // default sampling every 1000 ms

    if (args.length == 0 || args.length > 2) {
      printUsage();
      System.exit(-1);
    }

    if (args.length == 3) {
      // The second parameter is the sampling frequency
      samplingFrequency = Long.parseLong(args[2]);
    }

    echo("\nJMX Sampling Frequency:" + samplingFrequency);
    // Use the URI provided in the first arg to connect to the JMX server.
    // Also define sampling frequency. The monitor will collect the metrics
    // at this interval.
    monitor.initialize(args[0], samplingFrequency);
    // Create listener
    BasicUimaJmxMonitorListener listener = new BasicUimaJmxMonitorListener(monitor
            .getMaxServiceNameLength());
    // Plug in the monitor listener
    monitor.addJmxMonitorListener(listener);
    // Create and start the monitor thread
    Thread t = new Thread(monitor);
    t.start();

  }

  private static void printUsage() {
    System.out
            .println("Usage: \njava -cp %UIMA_HOME%/lib/*;%UIMA_HOME%/apache-activemq-5.4.1/* -Djava.util.logging.config.file=%UIMA_HOME%/config/MonitorLogger.properties    org.apache.uima.aae.jmx.monitor.JmxMonitor uri <frequency>\nuri - jmx server URI- required\nfrequency - how often the checkpoint is done. Default: 1000ms - optional");
  }

  private static void echo(String msg) {
    System.out.println(msg);
  }

  private static class StatEntry {
    ServicePerformanceMBean servicePerformanceMBeanProxy;

    ServiceInfoMBean serviceInfoMBeanProxy;

    QueueViewMBean inputQueueInfo;

    UimaVmQueueMBean vmInputQueueInfo;

    QueueViewMBean replyQueueInfo;

    UimaVmQueueMBean vmReplyQueueInfo;

    CasPoolManagementImplMBean casPoolMBeanProxy;

    CasPoolManagementImplMBean serviceCasPoolMBeanProxy;

    String name = "";

    boolean isVmQueue = true;

    double lastIdleTime = 0;

    double waitTimeOnCMGetNext = 0;

    long casProcessedCount = 0;

    double cpWaitTime;

    double scpWaitTime;

    double analysisTime = 0.0;

    public StatEntry(ServicePerformanceMBean perfBean, ServiceInfoMBean infoBean) {
      this(perfBean, infoBean, null);
    }

    public StatEntry(ServicePerformanceMBean perfBean, ServiceInfoMBean infoBean,
            CasPoolManagementImplMBean casPoolMBeanProxy) {
      servicePerformanceMBeanProxy = perfBean;
      serviceCasPoolMBeanProxy = casPoolMBeanProxy;
      serviceInfoMBeanProxy = infoBean;
      if (!infoBean.getBrokerURL().startsWith("Embedded Broker")) {
        isVmQueue = false; // This is JMS queue
      }
    }

    public CasPoolManagementImplMBean getServiceCasPoolMBeanProxy() {
      return serviceCasPoolMBeanProxy;
    }

    public void setInputQueueInfo(QueueViewMBean queueView) {
      inputQueueInfo = queueView;
    }

    public void setInputQueueInfo(UimaVmQueueMBean queueView) {
      vmInputQueueInfo = queueView;
    }

    public QueueViewMBean getInputQueueInfo() {
      return inputQueueInfo;
    }

    public UimaVmQueueMBean getVmInputQueueInfo() {
      return vmInputQueueInfo;
    }

    public void setReplyQueueInfo(QueueViewMBean queueView) {
      replyQueueInfo = queueView;
    }

    public void setReplyQueueInfo(UimaVmQueueMBean queueView) {
      vmReplyQueueInfo = queueView;
    }

    public QueueViewMBean getReplyQueueInfo() {
      return replyQueueInfo;
    }

    public UimaVmQueueMBean getVmReplyQueueInfo() {
      return vmReplyQueueInfo;
    }

    public void setCasPoolMBean(CasPoolManagementImplMBean anMBean) {
      casPoolMBeanProxy = anMBean;
    }

    public CasPoolManagementImplMBean getCasPoolMBean() {
      return casPoolMBeanProxy;
    }

    public double getIdleTime() {
      return lastIdleTime;
    }

    public void incrementCASCount(long aCASCount) {
      casProcessedCount = +aCASCount;
    }

    public long getLastCASCount() {
      return casProcessedCount;
    }

    public void setIdleTime(double lastIdleTime) {
      this.lastIdleTime = lastIdleTime;
    }

    public ServicePerformanceMBean getServicePerformanceMBeanProxy() {
      return servicePerformanceMBeanProxy;
    }

    public ServiceInfoMBean getServiceInfoMBeanProxy() {
      return serviceInfoMBeanProxy;
    }

    public void setName(String aName) {
      name = aName;
    }

    public String getName() {
      return name;
    }

    public void incrementWaitTimeOnCMGetNext(double aWaitTimeOnCMGetNext) {
      waitTimeOnCMGetNext = +aWaitTimeOnCMGetNext;
    }

    public double getWaitTimeOnCMGetNext() {
      return waitTimeOnCMGetNext;
    }

    public double getTotalCPWaitTime() {
      return cpWaitTime;
    }

    public void setCPWaitTime(double aWaitTime) {
      cpWaitTime = aWaitTime;
    }

    public double getTotalSCPWaitTime() {
      return scpWaitTime;
    }

    public void setSCPWaitTime(double aWaitTime) {
      scpWaitTime = aWaitTime;
    }

    public double getAnalysisTime() {
      return analysisTime;
    }

    public void incrementAnalysisTime(double anAnalysisTime) {
      analysisTime = +anAnalysisTime;
    }
  }

}
