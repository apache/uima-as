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

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.jmx.ManagementContext; //import org.apache.activemq.memory.UsageListener;
import org.apache.uima.UIMAFramework;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.util.Level;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
/**
 * 
 * @deprecated
 *
 */
public class BrokerDeployer implements ApplicationListener {
  private static final Class CLASS_NAME = BrokerDeployer.class;

  private static final int BASE_JMX_PORT = 1200;

  private static final int MAX_PORT_THRESHOLD = 200;

  private static BrokerService service = new BrokerService();

  private Object semaphore = new Object();

  private long maxBrokerMemory = 0;

  private String brokerURI;

  private TransportConnector tcpConnector = null;

  private TransportConnector httpConnector = null;

  private Object brokerInstanceMux = new Object();
  
  public BrokerDeployer(long maxMemoryinBytes) throws Exception {
    maxBrokerMemory = maxMemoryinBytes;
    startInternalBroker();
  }

  public BrokerDeployer() throws Exception {
    startInternalBroker();
  }

  public BrokerService getBroker() {
	  synchronized( brokerInstanceMux ) {
		    return service;
	  }
  }

	public void startInternalBroker() throws Exception {
		TransportConnector connector = null;
		synchronized (brokerInstanceMux) {
			if (maxBrokerMemory > 0) {
				System.out
						.println("Configuring Internal Broker With Max Memory Of:"
								+ maxBrokerMemory);
				if (UIMAFramework.getLogger(CLASS_NAME)
						.isLoggable(Level.CONFIG)) {
					UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG,
							CLASS_NAME.getName(), "startInternalBroker",
							JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
							"UIMAJMS_broker_memory__CONFIG",
							new Object[] { maxBrokerMemory });
				}
			}
			String[] connectors = service.getNetworkConnectorURIs();
			if (connectors != null) {
				for (int i = 0; i < connectors.length; i++) {
					System.out
							.println("ActiveMQ Broker Started With Connector:"
									+ connectors[i]);
				}
				brokerURI = service.getMasterConnectorURI();
			} else {

				String connectorList = "";
				service.setPersistent(false);
				int startPort = BASE_JMX_PORT;
				if (System.getProperties().containsKey(
						"com.sun.management.jmxremote.port")) {
					startPort = Integer.parseInt(System
							.getProperty("com.sun.management.jmxremote.port"));

				}
				while (startPort < MAX_PORT_THRESHOLD && !openPort(startPort)) {
					startPort++;
				}
				if (startPort < (startPort + MAX_PORT_THRESHOLD)) {
					service.getManagementContext().setConnectorPort(
							startPort);
					service.setUseJmx(true);
					System.setProperty("com.sun.management.jmxremote.port",
							String.valueOf(startPort));

					System.out
							.println("JMX Console connect URI:  service:jmx:rmi:///jndi/rmi://localhost:"
									+ startPort + "/jmxrmi");
					if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(
							Level.CONFIG)) {
						UIMAFramework
								.getLogger(CLASS_NAME)
								.logrb(
										Level.CONFIG,
										CLASS_NAME.getName(),
										"startInternalBroker",
										JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
										"UIMAJMS_jmx_uri__CONFIG",
										new Object[] { "service:jmx:rmi:///jndi/rmi://localhost:"
												+ startPort + "/jmxrmi" });
					}
				}

				brokerURI = generateInternalURI("tcp", 18810, true, false);

				// Wait until sucessfull adding connector to the broker
				// Sleeps for 1sec between retries until success
				int timeBetweenRetries = 1000;
				boolean tcpConnectorAcquiredValidPort = false;
				while (!tcpConnectorAcquiredValidPort) {
					try {
						tcpConnector = service.addConnector(brokerURI);
						tcpConnectorAcquiredValidPort = true;
					} catch (Exception e) {
						synchronized (this) {
							wait(timeBetweenRetries);
						}
					} // silence InstanceAlreadyExistsException

				}
				System.out.println("Adding TCP Connector:"
						+ tcpConnector.getConnectUri());
				if (UIMAFramework.getLogger(CLASS_NAME)
						.isLoggable(Level.CONFIG)) {
					UIMAFramework.getLogger(CLASS_NAME).logrb(
							Level.CONFIG,
							CLASS_NAME.getName(),
							"startInternalBroker",
							JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
							"UIMAJMS_adding_connector__CONFIG",
							new Object[] { "Adding TCP Connector",
									tcpConnector.getConnectUri() });
				}
				connectorList = tcpConnector.getName();
				if (System.getProperty("StompSupport") != null) {
					String stompURI = generateInternalURI("stomp", 61613,
							false, false);
					connector = service.addConnector(stompURI);
					System.out.println("Adding STOMP Connector:"
							+ connector.getConnectUri());
					if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(
							Level.CONFIG)) {
						UIMAFramework.getLogger(CLASS_NAME).logrb(
								Level.CONFIG,
								CLASS_NAME.getName(),
								"startInternalBroker",
								JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
								"UIMAJMS_adding_connector__CONFIG",
								new Object[] { "Adding STOMP Connector",
										connector.getConnectUri() });
					}
					connectorList += "," + connector.getName();
				}
				if (System.getProperty("HTTP") != null) {
					String stringPort = System.getProperty("HTTP");
					int p = Integer.parseInt(stringPort);
					String httpURI = generateInternalURI("http", p, false, true);
					httpConnector = service.addConnector(httpURI);
					System.out.println("Adding HTTP Connector:"
							+ httpConnector.getConnectUri());
					if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(
							Level.CONFIG)) {
						UIMAFramework.getLogger(CLASS_NAME).logrb(
								Level.CONFIG,
								CLASS_NAME.getName(),
								"startInternalBroker",
								JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
								"UIMAJMS_adding_connector__CONFIG",
								new Object[] { "Adding HTTP Connector",
										httpConnector.getConnectUri() });
					}
					connectorList += "," + httpConnector.getName();
				}
				service.start();
				System.setProperty("ActiveMQConnectors", connectorList);
				System.out.println("Broker Service Started - URL:"
						+ service.getVmConnectorURI());
				if (UIMAFramework.getLogger(CLASS_NAME)
						.isLoggable(Level.CONFIG)) {
					UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG,
							CLASS_NAME.getName(), "startInternalBroker",
							JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
							"UIMAJMS_broker_started__CONFIG",
							new Object[] { service.getVmConnectorURI() });
				}
			}
		}

		// Allow the connectors some time to start
		synchronized (semaphore) {
			semaphore.wait(1000);
		}
		// System.out.println("JMX Server Port:"+service.getManagementContext().getRmiServerPort());
		// setConnectorPort(startPort);

	}

  private boolean openPort(int aPort) {
    ServerSocket ssocket = null;
    try {
      ssocket = new ServerSocket(aPort);
      return true;
    } catch (Exception e) {
      return false;
    } finally {
      try {
        if (ssocket != null) {
          ssocket.close();
        }
      } catch (IOException ioe) {
      }
    }

  }

  /**
   * Generates a unique port for the Network Connector that will be plugged into the internal
   * Broker. This connector externalizes the internal broker so that remote delegates can reply back
   * to the Aggregate. This method tests port 18810 for availability and it fails increments the
   * port by one until a port is valid.
   * 
   * @return - Broker URI with a unique port
   */
  private String generateInternalURI(String aProtocol, int aDefaultPort, boolean cacheURL,
          boolean oneTry) throws Exception {
    boolean success = false;
    int openPort = aDefaultPort;
    ServerSocket ssocket = null;

    while (!success) {
      try {
        ssocket = new ServerSocket(openPort);
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.CONFIG)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG, CLASS_NAME.getName(),
                  "generateInternalURI", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_port_available__CONFIG", new Object[] { openPort });
        }
        String uri = aProtocol + "://"
                + ssocket.getInetAddress().getLocalHost().getCanonicalHostName() + ":" + openPort;
        success = true;
        if (cacheURL) {
          System.setProperty("BrokerURI", uri);

        }
        return uri;
      } catch (BindException e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.CONFIG)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.CONFIG, CLASS_NAME.getName(),
                  "generateInternalURI", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_port_not_available__CONFIG", new Object[] { openPort });
        }
        if (oneTry) {
          System.out.println("Given port:" + openPort + " is not available for " + aProtocol);
          throw e;
        }
        openPort++;
      } catch (Exception e) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "generateInternalURI", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
        }
        if (oneTry) {
          throw e;
        }
      } finally {
        try {
          if (ssocket != null) {
            ssocket.close();
          }
        } catch (IOException ioe) {
        }
      }
    }
    return null;

  }

  /**
   * Stops the ActiveMQ broker. This method waits for 1 second to allow the broker to cleanup
   * objects from JMX Server.
   * 
   */
	public void stop() {
		Object monitor = new Object();
		synchronized (brokerInstanceMux) {
			if (service != null) {
				try {
					// if ( usageListener != null )
					// {
					// service.getMemoryManager().removeUsageListener(usageListener);
					// }
					if (tcpConnector != null) {
						tcpConnector.stop();
						System.out.println("Broker Connector:"
								+ tcpConnector.getUri().toString()
								+ " is stopped");
					}
					if (httpConnector != null) {
						System.out.println("Broker Stopping HTTP Connector:"
								+ httpConnector.getUri().toString());
						httpConnector.stop();
						System.out.println("Broker Connector:"
								+ httpConnector.getUri().toString()
								+ " is stopped");
					}
					service.getManagementContext().stop();
					service.stop();
					Broker broker = service.getBroker();
					while (!broker.isStopped()) {
						synchronized (monitor) {
							try {
								monitor.wait(20); // wait for the broker to
													// terminate
							} catch (Exception e) {
							}
						}

					}
					System.out.println("Broker is stopped");
					broker = null;
					service = null;
				} catch (Exception e) {
					if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(
							Level.WARNING)) {
						UIMAFramework.getLogger(CLASS_NAME).logrb(
								Level.WARNING, CLASS_NAME.getName(), "stop",
								JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
								"UIMAJMS_exception__WARNING", e);
					}

				}
			}

		}
	}

  /**
   * Callback method invoked by Spring container during its lifecycle changes Ignore all events
   * except for ContextClosedEvent which indicates the container has shutdown. In this case, stop
   * the internal ActiveMQ broker.
   * 
   * @param anEvent
   *          - an event object
   */
  public void onApplicationEvent(ApplicationEvent anEvent) {
    if (anEvent instanceof ContextClosedEvent) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.INFO,
                CLASS_NAME.getName(),
                "onApplicationEvent",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_container_terminated__INFO",
                new Object[] { ((ContextClosedEvent) anEvent).getApplicationContext()
                        .getDisplayName() });
      }
      stop();
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "onApplicationEvent", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_broker_stopped__INFO", new Object[] { brokerURI });
      }
    }
  }

}
