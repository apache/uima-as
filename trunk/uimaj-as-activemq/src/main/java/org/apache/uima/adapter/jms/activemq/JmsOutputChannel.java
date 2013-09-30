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

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.management.ServiceNotFoundException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.Channel;
import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.InputChannel;
import org.apache.uima.aae.OutputChannel;
import org.apache.uima.aae.SerializerCache;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaSerializer;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.controller.PrimitiveAnalysisEngineController;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.ErrorContext;
import org.apache.uima.aae.error.MessageTimeoutException;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.error.UimaEEServiceException;
import org.apache.uima.aae.jmx.ServiceInfo;
import org.apache.uima.aae.jmx.ServicePerformance;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.UIMAMessage;
import org.apache.uima.aae.monitor.Monitor;
import org.apache.uima.aae.monitor.statistics.LongNumericStatistic;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.resource.metadata.ProcessingResourceMetaData;
import org.apache.uima.util.Level;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.uima.resource.ResourceProcessException;


import com.thoughtworks.xstream.XStream;

public class JmsOutputChannel implements OutputChannel {

  private static final Class CLASS_NAME = JmsOutputChannel.class;

  private static final long INACTIVITY_TIMEOUT = 5; // MINUTES

  private CountDownLatch controllerLatch = new CountDownLatch(1);

  private ActiveMQConnectionFactory connectionFactory;

  // Name of the external queue this service uses to receive messages
  private String serviceInputEndpoint;

  // Name of the internal queue this services uses to receive messages from delegates
  private String controllerInputEndpoint;

  // Name of the queue used by Cas Multiplier to receive requests to free CASes
  private String secondaryInputEndpoint;

  // The service controller
  private AnalysisEngineController analysisEngineController;

  // Cache containing connections to destinations this service interacts with
  // Each entry in this cache has an inactivity timer that times amount of time
  // elapsed since the last time a message was sent to the destination.
  private ConcurrentHashMap connectionMap = new ConcurrentHashMap();

  private String serverURI;

  private String serviceProtocolList = "";

  private volatile boolean aborting = false;

  private Destination freeCASTempQueue;

  private String hostIP = null;

  // By default every message will have expiration time added
  private volatile boolean addTimeToLive = true;

  private XStream xstream = new XStream();

  private Semaphore connectionSemaphore = new Semaphore(1);
  
  public JmsOutputChannel() {
    try {
    	if( System.getenv("IP") != null ) {
   		  hostIP = System.getenv("IP");
    	} else {
   	      hostIP = InetAddress.getLocalHost().getHostAddress();
    	}
    } catch (Exception e) { /* silently deal with this error */
    }
    // Check the environment for existence of NoTTL tag. If present,
    // the deployer of the service wants to avoid message expiration.
    if (System.getProperty("NoTTL") != null) {
      addTimeToLive = false;
    }

  }
  /**
   * Sets the ActiveMQ Broker URI
   */
  public void setServerURI(String aServerURI) {
    serverURI = aServerURI;
  }

  protected void setFreeCasQueue(Destination destination) {
    freeCASTempQueue = destination;
  }

  public String getServerURI() {
    return serverURI;
  }

  public String getName() {
    return "";
  }

  /**
   * 
   * @param connectionFactory
   */
  public void setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  public void setServiceInputEndpoint(String anEnpoint) {
    serviceInputEndpoint = anEnpoint;
  }

  public void setSecondaryInputQueue(String anEndpoint) {
    secondaryInputEndpoint = anEndpoint;
  }

  public ActiveMQConnectionFactory getConnectionFactory() {
    return this.connectionFactory;
  }

  public void initialize() throws AsynchAEException {
    if (getAnalysisEngineController() instanceof AggregateAnalysisEngineController) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "initialize",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_connector_list__FINE",
                new Object[] { System.getProperty("ActiveMQConnectors") });
      }
      // Aggregate controller set this System property at startup in
      serviceProtocolList = System.getProperty("ActiveMQConnectors");
    }
  }

  /**
   * Serializes CAS using indicated Serializer.
   * 
   * @param aCAS
   *          - CAS instance to serialize
   * @param aSerializerKey
   *          - a key identifying which serializer to use
   * @return - String - serialized CAS as String
   * @throws Exception
   */
  public String serializeCAS(boolean isReply, CAS aCAS, String aCasReferenceId,
          SerialFormat aSerializerKey) throws Exception {

    long start = getAnalysisEngineController().getCpuTime();

    String serializedCas = null;
    //  Fetch dedicated Serializer associated with this thread
    UimaSerializer serializer = SerializerCache.lookupSerializerByThreadId();

    if (isReply || (aSerializerKey == SerialFormat.XMI)) {
      CacheEntry cacheEntry = getAnalysisEngineController().getInProcessCache()
              .getCacheEntryForCAS(aCasReferenceId);

      XmiSerializationSharedData serSharedData;
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "serializeCAS",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_serialize_cas__FINE",
                new Object[] { aCasReferenceId });
      }
      if (isReply) {
        serSharedData = cacheEntry.getDeserSharedData();
        
        if (cacheEntry.acceptsDeltaCas()
                && (cacheEntry.getMarker() != null && cacheEntry.getMarker().isValid())) {
          serializedCas = serializer.serializeCasToXmi(aCAS, serSharedData, cacheEntry
                  .getMarker());
          cacheEntry.setSentDeltaCas(true);
        } else {
          serializedCas = serializer.serializeCasToXmi(aCAS, serSharedData);
          cacheEntry.setSentDeltaCas(false);
        }
        // if marker is invalid, create a fresh marker.
        if (cacheEntry.getMarker() != null && !cacheEntry.getMarker().isValid()) {
          cacheEntry.setMarker(aCAS.createMarker());
        }
        if ( !cacheEntry.sentDeltaCas() ) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "serializeCAS",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_serialize_cas__FINEST",
                      new Object[] { aCasReferenceId, "FULL Cas serialized and sent." });
          }
        }
      } else {
        serSharedData = cacheEntry.getDeserSharedData();
        if (serSharedData == null) {
          serSharedData = new XmiSerializationSharedData();
          cacheEntry.setXmiSerializationData(serSharedData);
        }
        serializedCas = serializer.serializeCasToXmi(aCAS, serSharedData);
        int maxOutgoingXmiId = serSharedData.getMaxXmiId();
        // Save High Water Mark in case a merge is needed
        getAnalysisEngineController().getInProcessCache().getCacheEntryForCAS(aCasReferenceId)
                .setHighWaterMark(maxOutgoingXmiId);
      }

    } 
//      else if ("xcas".equalsIgnoreCase(aSerializerKey)) {
//      // Default is XCAS
//      ByteArrayOutputStream bos = new ByteArrayOutputStream();
//      try {
//        serializer.serializeToXCAS(bos, aCAS, null, null, null);
//        serializedCas = bos.toString();
//      } catch (Exception e) {
//        throw e;
//      } finally {
//        bos.close();
//      }
//    }

    LongNumericStatistic statistic;
    if ((statistic = getAnalysisEngineController().getMonitor().getLongNumericStatistic("",
            Monitor.TotalSerializeTime)) != null) {
      statistic.increment(getAnalysisEngineController().getCpuTime() - start);
    }

    return serializedCas;
  }

  /**
   * This method verifies that the destination (queue) exists. It opens a connection the a broker,
   * creates a session and a message producer. Finally, using the message producer, sends an empty
   * message to a queue. This API support enables checking for existence of the reply (temp) queue
   * before any processing of a cas is done. This is an optimization to prevent expensive processing
   * if the client destination is no longer available.
   */
  public void bindWithClientEndpoint(Endpoint anEndpoint) throws Exception {
    // check if the reply endpoint is a temp destination
    if (anEndpoint.getDestination() != null) {
      // create message producer if one doesnt exist for this destination
      JmsEndpointConnection_impl endpointConnection = getEndpointConnection(anEndpoint);
      // Create empty message
      TextMessage tm = endpointConnection.produceTextMessage("");
      // test sending a message to reply endpoint. This tests existence of
      // a temp queue. If the client has been shutdown, this will fail
      // with an exception.
      endpointConnection.send(tm, 0, false);
    }
  }

  private long getInactivityTimeout(String destination, String brokerURL) {
    if (System.getProperty(JmsConstants.SessionTimeoutOverride) != null) {
      try {
        long overrideTimeoutValue = Long.parseLong(System
                .getProperty(JmsConstants.SessionTimeoutOverride));
        // endpointConnection.setInactivityTimeout(overrideTimeoutValue); // If the connection is
        // not used within this interval it will be removed
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "getEndpointConnection",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_override_connection_timeout__FINE",
                  new Object[] { analysisEngineController, overrideTimeoutValue, destination,
                      brokerURL });
        }
        return overrideTimeoutValue;
      } catch (NumberFormatException e) {
        /* ignore. use the default */}
    } else {
      // endpointConnection.setInactivityTimeout(INACTIVITY_TIMEOUT); // If the connection is not
      // used within this interval it will be removed
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME)
                .logrb(
                        Level.FINE,
                        CLASS_NAME.getName(),
                        "getEndpointConnection",
                        JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_connection_timeout__FINE",
                        new Object[] { analysisEngineController, INACTIVITY_TIMEOUT, destination,
                            brokerURL });
      }
    }
    return (int) INACTIVITY_TIMEOUT; // default
  }
  /**
   * Stop JMS connection and close all sessions associated with this connection
   * 
   * @param brokerConnectionEntry
   */
  private void invalidateConnectionAndEndpoints(BrokerConnectionEntry brokerConnectionEntry ) {
    Connection conn = brokerConnectionEntry.getConnection();
    try {
       if ( conn != null && ((ActiveMQConnection)conn).isClosed()) {
           for (Entry<Object, JmsEndpointConnection_impl> endpoints : brokerConnectionEntry.endpointMap
                   .entrySet()) {
              endpoints.getValue().close(); // close session and producer
           }
           brokerConnectionEntry.getConnection().stop();
           brokerConnectionEntry.getConnection().close();
           brokerConnectionEntry.setConnection(null);
       }
    } catch (Exception e) {
      // Ignore this for now. Attempting to close connection that has been closed
      // Ignore we are shutting down
    } finally {
       brokerConnectionEntry.endpointMap.clear();
       connectionMap.remove(brokerConnectionEntry.getBrokerURL());
       if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
         UIMAFramework.getLogger(CLASS_NAME).logrb(
                 Level.INFO,
                 CLASS_NAME.getName(),
                 "invalidateConnectionAndEndpoints",
                 JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                 "UIMAJMS_service_closing_connection__INFO",
                 new Object[] { getAnalysisEngineController().getComponentName(),
                   brokerConnectionEntry.getBrokerURL() });
       }
    }
    brokerConnectionEntry.setConnection(null);

  }
  private String getDestinationName(Endpoint anEndpoint) {
    String destination = anEndpoint.getEndpoint();
    if (anEndpoint.getDestination() != null
            && anEndpoint.getDestination() instanceof ActiveMQDestination) {
      destination = ((ActiveMQDestination) anEndpoint.getDestination()).getPhysicalName();
    }
    return destination;
  }
  private String getLookupKey(Endpoint anEndpoint) {
    String key = anEndpoint.getEndpoint() + anEndpoint.getServerURI();
    String destination = getDestinationName(anEndpoint);
    if ( anEndpoint.getDelegateKey() != null ) {
      key = anEndpoint.getDelegateKey() + "-"+destination;
    } else {
      key = "Client-"+destination;
    }
    return key;
  }
  private BrokerConnectionEntry createConnectionEntry(String brokerURL)  {
    BrokerConnectionEntry brokerConnectionEntry = new BrokerConnectionEntry();
    connectionMap.put(brokerURL, brokerConnectionEntry);
    ConnectionTimer connectionTimer = new ConnectionTimer(brokerConnectionEntry);
    connectionTimer.setAnalysisEngineController(getAnalysisEngineController());
    brokerConnectionEntry.setConnectionTimer(connectionTimer);
    brokerConnectionEntry.setBrokerURL(brokerURL);
    return brokerConnectionEntry;
  }
  /**
   * Returns {@link JmsEndpointConnection_impl} instance bound to a destination defined in the
   * {@link Endpoint} The endpoint identifies the destination that should receive the message. This
   * method references a cache that stores active connections. Active connections are those that are
   * fully bound and being used for communication. The key to locate the entry in the connection
   * cache is the queue name + broker URI. This uniquely identifies the destination. If an entry
   * does not exist in the cache, this routine will create a new connection, initialize it, and
   * cache it for future use. The cache is purely for optimization, to prevent opening a connection
   * for every message which is a costly operation. Instead the connection is opened, cached and
   * reused. The {@link JmsEndpointConnection_impl} instance is stored in the cache, and uses a
   * timer to make sure stale connection are removed. If a connection is not used in a given time
   * interval, the connection is considered stale and is dropped from the cache.
   * 
   * @param anEndpoint
   *          - endpoint configuration containing connection information to a destination
   * @return -
   * @throws AsynchAEException
   */
  private synchronized JmsEndpointConnection_impl getEndpointConnection(Endpoint anEndpoint)
          throws AsynchAEException, ServiceShutdownException, ConnectException {
	  
    try {
      controllerLatch.await();
    } catch (InterruptedException e) {
    }
    JmsEndpointConnection_impl endpointConnection = null;
    String brokerConnectionURL = null;
    
    
    try {
        connectionSemaphore.acquire();
        //  If sending a Free Cas Request to a remote Cas Multiplier always use the CM's
        //  broker
        if ( anEndpoint.isFreeCasEndpoint() && anEndpoint.isCasMultiplier() && anEndpoint.isReplyEndpoint()) {
          brokerConnectionURL = anEndpoint.getServerURI();
        } else {
          //  If this is a reply to a client, use the same broker URL that manages this service input queue.
          //  Otherwise this is a request so use a broker specified in the endpoint object.
          brokerConnectionURL = (anEndpoint.isReplyEndpoint()) ? serverURI : anEndpoint.getServerURI();
          
        }
        String key = getLookupKey(anEndpoint);
        String destination = getDestinationName(anEndpoint);

        // First get a Map containing destinations managed by a broker provided by the client
        BrokerConnectionEntry brokerConnectionEntry = null;
        boolean startInactivityReaperTimer = false;
        if (connectionMap.containsKey(brokerConnectionURL)) {
          brokerConnectionEntry = (BrokerConnectionEntry) connectionMap.get(brokerConnectionURL);
          // Findbugs thinks that the above may return null, perhaps due to a race condition. Add
          // the null check just in case
          if (brokerConnectionEntry == null) {
            throw new AsynchAEException("Controller:"
                    + getAnalysisEngineController().getComponentName()
                    + " Unable to Lookup Broker Connection For URL:" + brokerConnectionURL);
          } 
          brokerConnectionEntry.setBrokerURL(brokerConnectionURL);
          if ( JmsEndpointConnection_impl.connectionClosedOrFailed(brokerConnectionEntry) ) {
        	  brokerConnectionEntry.getConnectionTimer().cancelTimer();
            invalidateConnectionAndEndpoints(brokerConnectionEntry);
            brokerConnectionEntry = createConnectionEntry(brokerConnectionURL);
            startInactivityReaperTimer = true;
//            System.out.println(">>>>>> Connection Map Size:"+connectionMap.size());
          }
        } else {
          brokerConnectionEntry = createConnectionEntry(brokerConnectionURL);
          //System.out.println("---------------- New Broker "+brokerConnectionURL);
//          System.out.println(">>>>>> Connection Map Size:"+connectionMap.size());
//          long replyQueueInactivityTimeout = getInactivityTimeout(destination, brokerConnectionURL);
          startInactivityReaperTimer = true;
        }
        if ( startInactivityReaperTimer ) {
            long inactivityTimeout = getInactivityTimeout(destination, brokerConnectionURL);
            brokerConnectionEntry.getConnectionTimer().setInactivityTimeout(inactivityTimeout);
            // Start the FixedRate timer which wakes up at regular intervals defined by 'replyQueueInactivityTimeout'.
            // The purpose is to find inactive jms sessions. All sessions found the be inactive will be
            // closed.
            brokerConnectionEntry.getConnectionTimer().startSessionReaperTimer(getAnalysisEngineController().getComponentName());
        }
        
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "getEndpointConnection",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_acquiring_connection_to_endpoint__FINE",
                  new Object[] { getAnalysisEngineController().getComponentName(), destination,
                    brokerConnectionURL });
        }

        // check the cache first
        if (!brokerConnectionEntry.endpointExists(key)) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINE,
                    CLASS_NAME.getName(),
                    "getEndpointConnection",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_create_new_connection__FINE",
                    new Object[] { getAnalysisEngineController().getComponentName(), destination,
                      brokerConnectionURL });
          }
          
          Endpoint masterEndpoint = null;
          if ( getAnalysisEngineController() instanceof AggregateAnalysisEngineController ) {
            //  Check if the endpoint has previously FAILED. It may have been marked as FAILED
            //  due to a temp queue listener shutdown caused by a Broker failure. In such case,
            //  before sending a message to a remote delegate we need to start a new instance
            //  of a listener which creates a new temp reply queue. 
            if ( !anEndpoint.isReplyEndpoint() ) {  // this just means that we are not sending reply to a client
              //  The masterEndoint has the most current state. The 'anEndpoint' instance passed into
              //  this method is a clone from the master made at the begining of processing. The master endpoint
              //  may have been marked as FAILED after the clone was made
              masterEndpoint = ((AggregateAnalysisEngineController) getAnalysisEngineController()).
                        lookUpEndpoint(anEndpoint.getDelegateKey(),false);
              if ( masterEndpoint != null ) {
                //  Only one thread at a time is allowed here.
                synchronized( masterEndpoint ) {
                  if ( masterEndpoint.getStatus() == Endpoint.FAILED ) {
                    //  Returns InputChannel if the Reply Listener for the delegate has previously failed.
                    //  If the listener hasnt failed the getReplyInputChannel returns null
                    InputChannel iC = getAnalysisEngineController().getReplyInputChannel(anEndpoint.getDelegateKey());
                    if ( iC != null ) { 
                      try {
                        // Create a new Listener, new Temp Queue and associate the listener with the Input Channel
                    	// Also resets endpoint status to OK  
                        iC.createListener(anEndpoint.getDelegateKey(), anEndpoint);
                        iC.removeDelegateFromFailedList(masterEndpoint.getDelegateKey());
                      } catch( Exception exx) { 
                        throw new AsynchAEException(exx);
                      }
                    } else{
                    	throw new AsynchAEException("Aggregate:"+getAnalysisEngineController()+" Has not yet recovered a listener for delegate: "+anEndpoint.getDelegateKey());
                    }
                  } else if ( !masterEndpoint.isFreeCasEndpoint() ) {
                    //  In case this thread blocked while the reply queue listener was created, make sure
                    //  that this endpoint uses the most up-date reply queue destination
                    anEndpoint.setDestination(masterEndpoint.getDestination());
                  }
                }
              }
            }
          }
          
          endpointConnection = new JmsEndpointConnection_impl(brokerConnectionEntry, anEndpoint,
                  getAnalysisEngineController());
          brokerConnectionEntry.addEndpointConnection(key, endpointConnection);
//          long replyQueueInactivityTimeout = getInactivityTimeout(destination, brokerConnectionURL);
//          brokerConnectionEntry.getConnectionTimer().setInactivityTimeout(replyQueueInactivityTimeout);
//          // Start the FixedRate timer which wakes up at regular intervals defined by 'replyQueueInactivityTimeout'.
//          // The purpose is to find inactive jms sessions. All sessions found the be inactive will be
//          // closed.
//          brokerConnectionEntry.getConnectionTimer().startSessionReaperTimer(getAnalysisEngineController().getComponentName());

          // Connection is not in the cache, create a new connection, initialize it and cache it
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                    "getEndpointConnection", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_open_new_connection_to_endpoint__FINE",
                    new Object[] { getDestinationName(anEndpoint), brokerConnectionURL });
          }

          /**
           * Open connection to a broker, create JMS session and MessageProducer
           */
          endpointConnection.open();

          brokerConnectionEntry.getConnectionTimer().setConnectionCreationTimestamp(
                  endpointConnection.connectionCreationTimestamp);

          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINE,
                    CLASS_NAME.getName(),
                    "getEndpointConnection",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_connection_opened_to_endpoint__FINE",
                    new Object[] { getAnalysisEngineController().getComponentName(), destination,
                      brokerConnectionURL });
          }
        } else {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.FINE,
                    CLASS_NAME.getName(),
                    "getEndpointConnection",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_reusing_existing_connection__FINE",
                    new Object[] { getAnalysisEngineController().getComponentName(), destination,
                      brokerConnectionURL });
          }
          // Retrieve connection from the connection cache
          endpointConnection = brokerConnectionEntry.getEndpointConnection(key);
          // check the state of the connection and re-open it if necessary
          if (endpointConnection != null && !endpointConnection.isOpen()) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                      "getEndpointConnection", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_connection_closed_reopening_endpoint__FINE",
                      new Object[] { destination });
            }
            endpointConnection.open();
            if ( endpointConnection.isOpen()) {
                brokerConnectionEntry.getConnectionTimer()
                .setConnectionCreationTimestamp(System.nanoTime());
                if ( getAnalysisEngineController() instanceof AggregateAnalysisEngineController &&
                		anEndpoint.getDelegateKey() != null ) {
                	Endpoint masterEndpoint = 
                		((AggregateAnalysisEngineController) getAnalysisEngineController()).lookUpEndpoint(
                				anEndpoint.getDelegateKey(), false);
                	masterEndpoint.setStatus(Endpoint.OK);
                }
            } 
          }
        }

    
    
    } catch (InterruptedException e) {
      } finally {
	    connectionSemaphore.release();	  
      }
    
    //System.out.println("+++++++++++++++++++++ ConnectionMap Size:"+connectionMap.size());
    return endpointConnection;
  }

  private void logRequest(String key, Endpoint anEndpoint ) {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
              "sendRequest", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "key", new Object[] { getAnalysisEngineController().getComponentName(), anEndpoint.getDelegateKey() });
    }

  }
  private Delegate startGetMetaTimerAndGetDelegate( Endpoint anEndpoint ) {
    Delegate delegate = null;
    if (anEndpoint.getDestination() != null) {
      String replyQueueName = ((ActiveMQDestination) anEndpoint.getDestination())
              .getPhysicalName().replaceAll(":", "_");
      if (getAnalysisEngineController() instanceof AggregateAnalysisEngineController) {
        String delegateKey = ((AggregateAnalysisEngineController) getAnalysisEngineController())
                .lookUpDelegateKey(anEndpoint.getEndpoint());
        ServiceInfo serviceInfo = ((AggregateAnalysisEngineController) getAnalysisEngineController())
                .getDelegateServiceInfo(delegateKey);
        if (serviceInfo != null) {
          serviceInfo.setReplyQueueName(replyQueueName);
          serviceInfo.setServiceKey(delegateKey);
        }
        delegate = lookupDelegate(delegateKey);
        if (delegate.getGetMetaTimeout() > 0) {
          delegate.startGetMetaRequestTimer();
        }
      }
    } 
    return delegate;
  }
  /**
   * Sends request message to a delegate.
   * 
   * @param aCommand
   *          - the type of request [Process|GetMeta]
   * @param anEndpoint
   *          - the destination where the delegate receives messages
   * 
   * @throws AsynchAEException
   */
  public void sendRequest(int aCommand, String aCasReferenceId, Endpoint anEndpoint) 
  throws AsynchAEException {
    Delegate delegate = null;
    try {
      JmsEndpointConnection_impl endpointConnection = getEndpointConnection(anEndpoint);

      Message tm = endpointConnection.produceTextMessage("");
      tm.setIntProperty(AsynchAEMessage.Payload, AsynchAEMessage.None);
      switch(aCommand) {
        case AsynchAEMessage.CollectionProcessComplete:
          logRequest("UIMAEE_send_cpc_req__FINE", anEndpoint);
        break;
        case AsynchAEMessage.ReleaseCAS:
          tm.setStringProperty(AsynchAEMessage.CasReference, aCasReferenceId);
          logRequest("UIMAJMS_releasecas_request__endpoint__FINEST", anEndpoint);
        break;
        case AsynchAEMessage.GetMeta:
          delegate = startGetMetaTimerAndGetDelegate(anEndpoint);
          logRequest("UIMAEE_service_sending_getmeta_request__FINE", anEndpoint);
        break;
        case AsynchAEMessage.Stop:
          tm.setStringProperty(AsynchAEMessage.CasReference, aCasReferenceId);
          logRequest("UIMAEE_service_sending_stop_request__FINE", anEndpoint);
        break;
        
        case AsynchAEMessage.Process:
          logRequest("UIMAEE_service_sending_process_request__FINE", anEndpoint);
          serializeCasAndSend(getAnalysisEngineController().
                  getInProcessCache().
                    getCacheEntryForCAS(aCasReferenceId), anEndpoint);
          return;  /// <<<<< RETURN - Done here >>>>
          
        
      };

      populateHeaderWithRequestContext(tm, anEndpoint, aCommand);

      // For remotes add a special property to the message. This property
      // will be echoed back by the service. This property enables matching
      // the reply with the right endpoint object managed by the aggregate.
       tm.setStringProperty(AsynchAEMessage.EndpointServer, anEndpoint.getServerURI());

      if (endpointConnection.send(tm, 0, true) != true) {
        throw new ServiceNotFoundException();
      }

    } catch (AsynchAEException e) {
      throw e;
    } catch (Exception e) {
      if (delegate != null && aCommand == AsynchAEMessage.GetMeta) {
        delegate.cancelDelegateGetMetaTimer();
      }
      // Handle the error
      ErrorContext errorContext = new ErrorContext();
      errorContext.add(AsynchAEMessage.Command, aCommand);
      errorContext.add(AsynchAEMessage.Endpoint, anEndpoint);
      getAnalysisEngineController().getErrorHandlerChain().handle(e, errorContext,
              getAnalysisEngineController());
    }
  }
  
  private void serializeCasAndSend(CacheEntry entry, Endpoint anEndpoint) throws Exception {
    if (anEndpoint.getSerialFormat() == SerialFormat.XMI) {
      String serializedCAS = getSerializedCasAndReleaseIt(false, entry.getCasReferenceId(), anEndpoint,
              anEndpoint.isRetryEnabled());
      if (UIMAFramework.getLogger().isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "sendRequest",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_sending_serialized_cas__FINEST",
                new Object[] { getAnalysisEngineController().getComponentName(),
                    anEndpoint.getDelegateKey(), entry.getCasReferenceId(), serializedCAS });
      }
      // Send process request to remote delegate and start timeout timer
      sendCasToRemoteEndpoint(true, serializedCAS, entry, anEndpoint, true);
    } else {
      byte[] serializedCAS = getBinaryCasAndReleaseIt(false, entry.getCasReferenceId(), anEndpoint,
              anEndpoint.isRetryEnabled());
      if (UIMAFramework.getLogger().isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINEST,
                CLASS_NAME.getName(),
                "sendRequest",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_sending_binary_cas__FINEST",
                new Object[] { getAnalysisEngineController().getComponentName(),
                    anEndpoint.getDelegateKey(), entry.getCasReferenceId(), serializedCAS });
      }
      
      // Send process request to remote delegate and start timeout timer
      sendCasToRemoteEndpoint(true, serializedCAS, entry, anEndpoint, true);

    }

  }


  public void sendReply(CacheEntry entry, Endpoint anEndpoint) throws AsynchAEException {
    try {
      anEndpoint.setReplyEndpoint(true);
      if (anEndpoint.isRemote()) {
        if (anEndpoint.getSerialFormat() == SerialFormat.XMI) {
          // Serializes CAS and releases it back to CAS Pool
          String serializedCAS = getSerializedCas(true, entry.getCasReferenceId(), anEndpoint,
                  anEndpoint.isRetryEnabled());
          sendCasToRemoteEndpoint(false, serializedCAS, entry, anEndpoint, false);
        } else {
          byte[] binaryCas = getBinaryCas(true, entry.getCasReferenceId(), anEndpoint, anEndpoint
                  .isRetryEnabled());
          if (binaryCas == null) {
            return;
          }
          sendCasToRemoteEndpoint(false, binaryCas, entry, anEndpoint, false);
        }

      } else {
        // Not supported
      }
    } catch (ServiceShutdownException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    } catch (AsynchAEException e) {
      throw e;
    }

    catch (Exception e) {
      throw new AsynchAEException(e);
    }

  }
  public void sendReply(int aCommand, Endpoint anEndpoint, String aCasReferenceId, boolean notifyOnJmsException)
  throws AsynchAEException {
	    try {
	        if (aborting) {
	          return;
	        }
	        anEndpoint.setReplyEndpoint(true);
	        JmsEndpointConnection_impl endpointConnection = getEndpointConnection(anEndpoint);

	        TextMessage tm = endpointConnection.produceTextMessage("");
	        tm.setIntProperty(AsynchAEMessage.Payload, AsynchAEMessage.None);
	        populateHeaderWithResponseContext(tm, anEndpoint, aCommand);
	        if ( aCasReferenceId != null ) {
		        tm.setStringProperty(AsynchAEMessage.CasReference, aCasReferenceId);
	        }

	        // If this service is a Cas Multiplier add to the message a FreeCasQueue.
	        // The client may need send Stop request to that queue.
	        if (aCommand == AsynchAEMessage.ServiceInfo
	                && getAnalysisEngineController().isCasMultiplier() && freeCASTempQueue != null) {
	          // Attach a temp queue to the outgoing message. This a queue where
	          // Free CAS notifications need to be sent from the client
	          tm.setJMSReplyTo(freeCASTempQueue);
	        }
	        //	Check if there was a failure while sending a message
	        if ( !endpointConnection.send(tm, 0, false, notifyOnJmsException) && notifyOnJmsException ) {
	        	throw new JMSException("JMS Send Failed. Check UIMA Log For Details.");
	        }
	        addIdleTime(tm);
	        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
	          UIMAFramework.getLogger(CLASS_NAME).logrb(
	                  Level.FINE,
	                  CLASS_NAME.getName(),
	                  "sendReply",
	                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
	                  "UIMAJMS_cpc_reply_sent__FINE",
	                  new Object[] { getAnalysisEngineController().getComponentName(),
	                      anEndpoint.getEndpoint() });
	        }
	      } catch (JMSException e) {
	        if ( notifyOnJmsException ) {
	        	throw new AsynchAEException(e);
	        }
	        // Unable to establish connection to the endpoint. Log it and continue
	        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
	          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
	                  "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
	                  "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

	          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "sendReply",
	                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAEE_exception__WARNING",
	                 e);
	        }
	      }

	      catch (ServiceShutdownException e) {
	        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
	          
	          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
	                  "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
	                  "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

	          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
	                  "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
	                  "UIMAJMS_exception__WARNING", e);
	        }
	      }

	      catch (AsynchAEException e) {
	        throw e;
	      } catch (Exception e) {
	        throw new AsynchAEException(e);
	      }

  }

  /**
   * Sends JMS Reply Message to a given endpoint. The reply message contains given Throwable (with
   * full stack)
   * 
   * @param t
   *          - Throwable to include in the reply message
   * @param anEndpoint
   *          - an endpoint to receive the reply message
   * @param aCasReferenceId
   *          - a unique CAS reference id
   * 
   * @throws AsynchAEException
   */
  public void sendReply(Throwable t, String aCasReferenceId, String aParentCasReferenceId,
          Endpoint anEndpoint, int aCommand) throws AsynchAEException {
    anEndpoint.setReplyEndpoint(true);
    try {
      Throwable wrapper = null;
      if (!(t instanceof UimaEEServiceException)) {
        // Strip off AsyncAEException and replace with UimaEEServiceException
        if (t instanceof AsynchAEException && t.getCause() != null) {
          wrapper = new UimaEEServiceException(t.getCause());
        } else {
          wrapper = new UimaEEServiceException(t);
        }
      }
      if (aborting) {
        return;
      }
      anEndpoint.setReplyEndpoint(true);
      JmsEndpointConnection_impl endpointConnection = getEndpointConnection(anEndpoint);
      // Create Message that will contain serialized Exception with stack
      ObjectMessage om = endpointConnection.produceObjectMessage();
      //  Now try to catch non-serializable exception. The Throwable passed into this method may
      //  not be serializable. Catch the exception, and create a wrapper containing stringified  
      //  stack trace.
      try {
          //  serialize the Throwable
          if (wrapper == null) {
            om.setObject(t);
          } else {
            om.setObject(wrapper);
          }
      } catch( RuntimeException e) {
          //  Check if we failed due to non-serializable object in the Throwable
          if ( e.getCause() != null && e.getCause() instanceof NotSerializableException ) {
            //  stringify the stack trace
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            wrapper = new UimaEEServiceException(sw.toString());
            //  serialize the new wrapper
            om.setObject(wrapper);
          } else {
            throw e;  // rethrow
          }
        } catch( Exception e) {
          throw e; // rethrow
      }
        // Add common header properties
      populateHeaderWithResponseContext(om, anEndpoint, aCommand); // AsynchAEMessage.Process);

      om.setIntProperty(AsynchAEMessage.Payload, AsynchAEMessage.Exception);
      if (aCasReferenceId != null) {
        om.setStringProperty(AsynchAEMessage.CasReference, aCasReferenceId);
        if (aParentCasReferenceId != null) {
          om.setStringProperty(AsynchAEMessage.InputCasReference, aParentCasReferenceId);
        }
      }

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "sendReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_sending_exception__FINE",
                new Object[] { getAnalysisEngineController().getName(), anEndpoint.getEndpoint() });
      }
      // Dispatch Message to destination
      endpointConnection.send(om, 0, false);
      addIdleTime(om);
    } catch (JMSException e) {
      // Unable to establish connection to the endpoint. Logit and continue
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "sendReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_unable_to_connect__INFO",
                new Object[] { getAnalysisEngineController().getName(), anEndpoint.getEndpoint() });
      }
    } catch (ServiceShutdownException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    } catch (AsynchAEException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "sendReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_unable_to_connect__INFO",
                new Object[] { getAnalysisEngineController().getName(), anEndpoint.getEndpoint() });
      }
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  /**
   * 
   * @param aProcessingResourceMetadata
   * @param anEndpoint
   * @param serialize
   * @throws AsynchAEException
   */
  public void sendReply(ProcessingResourceMetaData aProcessingResourceMetadata,
          Endpoint anEndpoint, boolean serialize) throws AsynchAEException {
    if (aborting) {
      return;
    }
    long msgSize = 0;

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
    	
    	
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "sendReply",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_fetching_endpoint_dispatcher__INFO",
                    new Object[] {analysisEngineController.getComponentName(), anEndpoint.getDestination()});
      }
      anEndpoint.setReplyEndpoint(true);
      // Initialize JMS connection to given endpoint
      JmsEndpointConnection_impl endpointConnection = getEndpointConnection(anEndpoint);
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "sendReply",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_fetched_endpoint_dispatcher__INFO",
                  new Object[] {analysisEngineController.getComponentName(), anEndpoint.getDestination()});
    }

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "sendReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_produce_txt_msg__FINE",
                new Object[] {});
      }
      TextMessage tm = endpointConnection.produceTextMessage("");

      // Collocated Aggregate components dont send metadata just empty reply
      // Such aggregate has merged its typesystem already since it shares
      // CasManager with its parent
      if (serialize) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_serializing_meta__FINE", new Object[] {});
        }
        // Serialize metadata
        aProcessingResourceMetadata.toXML(bos);
        tm.setText(bos.toString());
        msgSize = bos.toString().length();
      }

      tm.setIntProperty(AsynchAEMessage.Payload, AsynchAEMessage.Metadata);
      // This service supports Binary Serialization
      // change comment on next 2 lines to install compressed serialization as the capability
      tm.setIntProperty(AsynchAEMessage.SERIALIZATION, AsynchAEMessage.BINARY_SERIALIZATION);
//      tm.setIntProperty(AsynchAEMessage.SERIALIZATION, AsynchAEMessage.BINARY_COMPRESSED_FILTERED_SERIALIZATION);

      populateHeaderWithResponseContext(tm, anEndpoint, AsynchAEMessage.GetMeta);
      if (freeCASTempQueue != null) {
        // Attach a temp queue to the outgoing message. This a queue where
        // Free CAS notifications need to be sent from the client
        tm.setJMSReplyTo(freeCASTempQueue);
      }

      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(), "sendReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_metadata_reply__endpoint__FINEST",
                new Object[] { serviceInputEndpoint, anEndpoint.getEndpoint() });
      }
      endpointConnection.send(tm, msgSize, false);
    } catch (JMSException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
      // Unable to establish connection to the endpoint. Log it and continue
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "sendReply",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_unable_to_connect__INFO",
                new Object[] { getAnalysisEngineController().getName(), anEndpoint.getEndpoint() });
      }
    }

    catch (ServiceShutdownException e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
      throw new AsynchAEException(e);
    } finally {
      try {
        bos.close();
      } catch (Exception e) {
      }
    }
  }

  private byte[] getBinaryCas(boolean isReply, String aCasReferenceId, Endpoint anEndpoint,
          boolean cacheSerializedCas) throws Exception {
    CAS cas = null;
    try {
      byte[] serializedCAS = null;
      // Using Cas reference Id retrieve CAS from the shared Cash
      cas = getAnalysisEngineController().getInProcessCache().getCasByReference(aCasReferenceId);
      ServicePerformance casStats = getAnalysisEngineController().getCasStatistics(aCasReferenceId);
      CacheEntry entry = getAnalysisEngineController().getInProcessCache().getCacheEntryForCAS(
              aCasReferenceId);
      long t1 = getAnalysisEngineController().getCpuTime();
      // Serialize CAS for remote Delegates
      SerialFormat serializerType = anEndpoint.getSerialFormat();
      if (cas == null || entry == null) {
        return null;
      }
      //  Fetch dedicated Serializer associated with this thread
      UimaSerializer serializer = SerializerCache.lookupSerializerByThreadId();

      if (serializerType == SerialFormat.BINARY || serializerType == SerialFormat.COMPRESSED_FILTERED) {
        
        if (entry.acceptsDeltaCas() && isReply) {
          if (entry.getMarker() != null && entry.getMarker().isValid()) {
            if (serializerType == SerialFormat.COMPRESSED_FILTERED) {
              serializedCAS = serializer.serializeCasToBinary6(cas, entry.getMarker(), entry.getCompress6ReuseInfo());  
            } else {
              serializedCAS = serializer.serializeCasToBinary(cas, entry.getMarker());
            }
            entry.setSentDeltaCas(true);
          } else {
            if (serializerType == SerialFormat.COMPRESSED_FILTERED) {
              serializedCAS = serializer.serializeCasToBinary6(cas); 
            } else {
              serializedCAS = serializer.serializeCasToBinary(cas);
            }
            entry.setSentDeltaCas(false);
          }
        } else {
          // either is a reply to a caller not accepting delta, or
          //        is not a reply
          if (serializerType == SerialFormat.COMPRESSED_FILTERED) {
            if (isReply) {
              serializedCAS = serializer.serializeCasToBinary6(cas);  // never called? 
            } else {
              serializedCAS = serializer.serializeCasToBinary6(cas, entry, anEndpoint.getTypeSystemImpl());
            }
          } else {
            serializedCAS = serializer.serializeCasToBinary(cas);
          }
          entry.setSentDeltaCas(false);
          if (isReply) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "getBinaryCas",
                        JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_serialize_cas__FINEST",
                        new Object[] { aCasReferenceId, "FULL Cas serialized and sent." });
            }
          }
        }
        // create a fresh marker
        if (entry.getMarker() != null && !entry.getMarker().isValid()) {
          entry.setMarker(cas.createMarker());
        }

      } else {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.INFO,
                  CLASS_NAME.getName(),
                  "getBinaryCas",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_serializer__WARNING",
                  new Object[] { getAnalysisEngineController().getName(), serializerType,
                      anEndpoint.getEndpoint() });
        }
        throw new UimaEEServiceException("Invalid Serializer:" + serializerType + " For Endpoint:"
                + anEndpoint.getEndpoint());
      }
      long timeToSerializeCas = getAnalysisEngineController().getCpuTime() - t1;

      getAnalysisEngineController().incrementSerializationTime(timeToSerializeCas);

      entry.incrementTimeToSerializeCAS(timeToSerializeCas);
      casStats.incrementCasSerializationTime(timeToSerializeCas);
      getAnalysisEngineController().getServicePerformance().incrementCasSerializationTime(
              timeToSerializeCas);
      return serializedCAS;
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }

  }

  private String getSerializedCas(boolean isReply, String aCasReferenceId, Endpoint anEndpoint,
          boolean cacheSerializedCas) throws Exception {
    CAS cas = null;
    try {
      String serializedCAS = null;
      // Using Cas reference Id retrieve CAS from the shared Cash
      cas = getAnalysisEngineController().getInProcessCache().getCasByReference(aCasReferenceId);
      ServicePerformance casStats = getAnalysisEngineController().getCasStatistics(aCasReferenceId);
      if (cas == null) {
        serializedCAS = getAnalysisEngineController().getInProcessCache().getSerializedCAS(
                aCasReferenceId);
      } else {
        CacheEntry entry = getAnalysisEngineController().getInProcessCache().getCacheEntryForCAS(
                aCasReferenceId);
        long t1 = getAnalysisEngineController().getCpuTime();
        // Serialize CAS for remote Delegates
        SerialFormat serializer = anEndpoint.getSerialFormat();
        if (serializer == null) {
          serializer = SerialFormat.XMI;
        }
        serializedCAS = serializeCAS(isReply, cas, aCasReferenceId, serializer);
        long timeToSerializeCas = getAnalysisEngineController().getCpuTime() - t1;
        getAnalysisEngineController().incrementSerializationTime(timeToSerializeCas);

        entry.incrementTimeToSerializeCAS(timeToSerializeCas);
        casStats.incrementCasSerializationTime(timeToSerializeCas);
        getAnalysisEngineController().getServicePerformance().incrementCasSerializationTime(
                timeToSerializeCas);
        if (cacheSerializedCas) {
          getAnalysisEngineController().getInProcessCache().saveSerializedCAS(aCasReferenceId,
                  serializedCAS);
        }
      }
      return serializedCAS;
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }
  }

  private byte[] getBinaryCasAndReleaseIt(boolean isReply, String aCasReferenceId,
          Endpoint anEndpoint, boolean cacheSerializedCas) throws Exception {
    try {
      return getBinaryCas(isReply, aCasReferenceId, anEndpoint, cacheSerializedCas);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    } finally {
      if (getAnalysisEngineController() instanceof PrimitiveAnalysisEngineController
              && anEndpoint.isRemote()) {
        getAnalysisEngineController().dropCAS(aCasReferenceId, true);
      }
    }
  }

  private String getSerializedCasAndReleaseIt(boolean isReply, String aCasReferenceId,
          Endpoint anEndpoint, boolean cacheSerializedCas) throws Exception {
    try {
      return getSerializedCas(isReply, aCasReferenceId, anEndpoint, cacheSerializedCas);
    } catch (Exception e) {
      throw new AsynchAEException(e);
    } finally {
      if (getAnalysisEngineController() instanceof PrimitiveAnalysisEngineController
              && anEndpoint.isRemote()) {
        getAnalysisEngineController().dropCAS(aCasReferenceId, true);
      }
    }
  }

  private void populateStats(Message aTextMessage, Endpoint anEndpoint, String aCasReferenceId,
          int anAdminCommand, boolean isRequest) throws Exception {
    if (anEndpoint.isFinal()) {
      aTextMessage.setLongProperty("SENT-TIME", System.nanoTime());
    }

    if (anAdminCommand == AsynchAEMessage.Process) {
      if (isRequest) {
        long departureTime = System.nanoTime();
        getAnalysisEngineController().saveTime(departureTime, aCasReferenceId,
                anEndpoint.getEndpoint());
      } else {
        try {
          if ( getAnalysisEngineController().getInProcessCache().entryExists(aCasReferenceId)) {
            CacheEntry entry = 
                    getAnalysisEngineController().getInProcessCache().getCacheEntryForCAS(aCasReferenceId);
            //  getDelegateMetrics returns an empty list if no metrics are found
            if ( entry.getDelegateMetrics().size() > 0 ) {
              aTextMessage.setStringProperty(AsynchAEMessage.CASPerComponentMetrics, 
                      xstream.toXML(entry.getDelegateMetrics()));
            }
          }
        } catch( Exception ex) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                  "populateStats", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", ex);        
        }
        ServicePerformance casStats = getAnalysisEngineController().getCasStatistics(
                aCasReferenceId);

        aTextMessage.setLongProperty(AsynchAEMessage.TimeToSerializeCAS, casStats
                .getRawCasSerializationTime());
        aTextMessage.setLongProperty(AsynchAEMessage.TimeToDeserializeCAS, casStats
                .getRawCasDeserializationTime());
        aTextMessage.setLongProperty(AsynchAEMessage.TimeInProcessCAS, casStats
                .getRawAnalysisTime());
        aTextMessage.setLongProperty(AsynchAEMessage.TimeWaitingForCAS,
                getAnalysisEngineController().getServicePerformance().getTimeWaitingForCAS());
        long iT = getAnalysisEngineController().getIdleTimeBetweenProcessCalls(
                AsynchAEMessage.Process);
        aTextMessage.setLongProperty(AsynchAEMessage.IdleTime, iT);
        String lookupKey = getAnalysisEngineController().getName();
        long arrivalTime = getAnalysisEngineController().getTime(aCasReferenceId, lookupKey); // serviceInputEndpoint);
        long timeInService = getAnalysisEngineController().getCpuTime() - arrivalTime;
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINEST, CLASS_NAME.getName(),
                  "populateStats", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_timein_service__FINEST",
                  new Object[] { serviceInputEndpoint, (double) timeInService / (double) 1000000 });
        }
      }
    }
  }

  private long getCommandTimeoutValue(Endpoint anEndpoint, int aCommand) {
    switch (aCommand) {
      case AsynchAEMessage.GetMeta:
        return anEndpoint.getMetadataRequestTimeout();
      case AsynchAEMessage.Process:
        return anEndpoint.getProcessRequestTimeout();
    }
    return 0; // no match for the command
  }

  /**
   * Adds Request specific properties to the JMS Header.
   * 
   * @param aMessage
   * @param anEndpoint
   * @param aCommand
   * @throws Exception
   */
  private void populateHeaderWithRequestContext(Message aMessage, Endpoint anEndpoint, int aCommand)
          throws Exception {
    aMessage.setIntProperty(AsynchAEMessage.MessageType, AsynchAEMessage.Request);
    aMessage.setIntProperty(AsynchAEMessage.Command, aCommand);
    // TODO override default based on system property
    aMessage.setBooleanProperty(AsynchAEMessage.AcceptsDeltaCas, true);
    long timeout = getCommandTimeoutValue(anEndpoint, aCommand);
    // If the timeout is defined in the Deployment Descriptor and
    // the service is configured to use time to live (TTL), add
    // JMS message expiration time. The TTL is by default always
    // added to the message. To override this add "-DNoTTL" to the
    // command line.
    if (timeout > 0 && addTimeToLive) {
      Delegate delegate = lookupDelegate(anEndpoint.getDelegateKey());
      long ttl = timeout;
      // How many CASes are in the list of CASes pending reply for this delegate
      int currentOutstandingCasListSize = delegate.getCasPendingReplyListSize();
      if (currentOutstandingCasListSize > 0) {
        // increase the time-to-live
        ttl *= currentOutstandingCasListSize;
      }
      aMessage.setJMSExpiration(ttl);
    }
    if (getAnalysisEngineController() instanceof AggregateAnalysisEngineController) {
      aMessage.setStringProperty(AsynchAEMessage.MessageFrom, controllerInputEndpoint);
      if (anEndpoint.isRemote()) {
        String protocol = serviceProtocolList;
        if (anEndpoint.getServerURI().trim().toLowerCase().startsWith("http")
                || (anEndpoint.getReplyToEndpoint() != null && anEndpoint.getReplyToEndpoint()
                        .trim().length() > 0)) {
          protocol = anEndpoint.getServerURI().trim();
          // protocol = extractURLWithProtocol(serviceProtocolList, "http");

          // get the replyto endpoint name
          String replyTo = anEndpoint.getReplyToEndpoint();
          if (replyTo == null && anEndpoint.getDestination() == null) {
            throw new AsynchAEException(
                    "replyTo endpoint name not specified for HTTP-based endpoint:"
                            + anEndpoint.getEndpoint());
          }
          if (replyTo == null) {
            replyTo = "";
          }
          aMessage.setStringProperty(AsynchAEMessage.MessageFrom, replyTo);

        }
        Object destination;
        
        if ((destination = anEndpoint.getDestination()) != null) {
          aMessage.setJMSReplyTo((Destination) destination);
          aMessage.setStringProperty(UIMAMessage.ServerURI, anEndpoint.getServerURI());
        } else {
          aMessage.setStringProperty(UIMAMessage.ServerURI, protocol);
        }

        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "populateHeaderWithRequestContext",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_sending_new_msg_to_remote_FINE",
                  new Object[] { getAnalysisEngineController().getComponentName(),
                      anEndpoint.getServerURI(), anEndpoint.getEndpoint() });
        }
      } else // collocated
      {
        aMessage.setStringProperty(UIMAMessage.ServerURI, anEndpoint.getServerURI());
      }
    }
  }

  /**
   * Adds Response specific properties to the JMS Header
   * 
   * @param aMessage
   * @param anEndpoint
   * @param aCommand
   * @throws Exception
   */
  private void populateHeaderWithResponseContext(Message aMessage, Endpoint anEndpoint, int aCommand)
          throws Exception {
    aMessage.setIntProperty(AsynchAEMessage.MessageType, AsynchAEMessage.Response);
    aMessage.setIntProperty(AsynchAEMessage.Command, aCommand);
    aMessage.setStringProperty(AsynchAEMessage.MessageFrom, serviceInputEndpoint);

    if (anEndpoint.isRemote()) {
      aMessage.setStringProperty(UIMAMessage.ServerURI, getServerURI());
      if (hostIP != null) {
        aMessage.setStringProperty(AsynchAEMessage.ServerIP, hostIP);
        if ( getAnalysisEngineController() != null ) {
          aMessage.setStringProperty(AsynchAEMessage.UimaASProcessPID, getAnalysisEngineController().getPID()+":"+Thread.currentThread().getId());
        }
      }
      if (anEndpoint.getEndpointServer() != null) {
        aMessage.setStringProperty(AsynchAEMessage.EndpointServer, anEndpoint.getEndpointServer());
      }
    } else {
      aMessage.setStringProperty(UIMAMessage.ServerURI, anEndpoint.getServerURI());
    }
  }

  public AnalysisEngineController getAnalysisEngineController() {
    return analysisEngineController;
  }

  public void setController(AnalysisEngineController analysisEngineController) {
    this.analysisEngineController = analysisEngineController;
    controllerLatch.countDown();
  }

  public String getControllerInputEndpoint() {
    return controllerInputEndpoint;
  }

  public void setControllerInputEndpoint(String controllerInputEndpoint) {
    this.controllerInputEndpoint = controllerInputEndpoint;
  }

  private void dispatch(Message aMessage, Endpoint anEndpoint, CacheEntry entry, boolean isRequest,
          JmsEndpointConnection_impl endpointConnection, long msgSize) throws Exception {
	  
	  if ( anEndpoint == null ) { 
		  throw new UimaEEServiceException("UIMA AS service:"+getAnalysisEngineController().getComponentName()+" unable to dispatch request to a remote delegate. Endpoint is null. This is an invalid state.");
	  }
    // Add stats
    populateStats(aMessage, anEndpoint, entry.getCasReferenceId(), AsynchAEMessage.Process,
            isRequest);
    //  If this is a reply to a client, use the same broker URL that manages this service input queue.
    //  Otherwise this is a request so use a broker specified in the endpoint object.
    String brokerConnectionURL = (anEndpoint.isReplyEndpoint()) ? serverURI : anEndpoint.getServerURI();

    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(
              Level.FINE,
              CLASS_NAME.getName(),
              "dispatch",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_sending_new_msg_to_remote_FINE",
              new Object[] { getAnalysisEngineController().getName(),
                brokerConnectionURL, endpointConnection.getEndpoint() });
    }
    // By default start a timer associated with a connection to the endpoint. Once a connection is
    // established with an
    // endpoint it is cached and reused for subsequent messaging. If the connection is not used
    // within a given interval
    // the timer silently expires and closes the connection. This mechanism is similar to what Web
    // Server does when
    // managing sessions. In case when we want the remote delegate to respond to a temporary queue,
    // which is implied
    // by anEndpoint.getDestination != null, we dont start the timer.
    boolean startConnectionTimer = isRequest ? false : true; // connection time is for replies
    // ----------------------------------------------------
    // Send Request Messsage to the Endpoint
    // ----------------------------------------------------
    // Add the CAS to the delegate's list of CASes pending reply. Do the add before
    // the send to eliminate a race condition where the reply is received (on different
    // thread) *before* the CAS is added to the list.
    if (isRequest) {
      anEndpoint.setWaitingForResponse(true);
      // Add CAS to the list of CASes pending reply
      addCasToOutstandingList(entry, isRequest, anEndpoint.getDelegateKey());
    } else {
      addIdleTime(aMessage);
    }
    // If the send fails it returns false.
    if (endpointConnection.send(aMessage, msgSize, startConnectionTimer) == false) {
      // Failure on sending a request requires cleanup that includes stopping a listener
      // on the delegate that we were unable to send a message to. The delegate state is
      // set to FAILED. If there are retries or more CASes to send to this delegate the
      // connection will be retried. The error handler called from the send() above already
      // removed the CAS from outstanding list.
      if (isRequest && anEndpoint.getDelegateKey() != null) {
    	  Endpoint master= ((AggregateAnalysisEngineController) getAnalysisEngineController()).lookUpEndpoint(anEndpoint.getDelegateKey(), false);
    	  master.setStatus(Endpoint.FAILED);
    	  
    	  String key = getLookupKey(anEndpoint);
    	    if (connectionMap.containsKey(brokerConnectionURL)) {
         	  // First get a Map containing destinations managed by a broker provided by the client
    	      BrokerConnectionEntry brokerConnectionEntry = (BrokerConnectionEntry) connectionMap.get(brokerConnectionURL);
    	      // check if the broker connection entry contains an endpoint that we failed on while
    	      // sending a message. If it exits, remove the endpoint to allow recovery to work on 
    	      // a subsequent message. 
    	      if ( brokerConnectionEntry != null ) {
        	      if (brokerConnectionEntry.endpointExists(key)) {
          	    	brokerConnectionEntry.removeEndpoint(key);
          	      }
    	      }
    	    } 
        // Spin recovery thread to handle send error. After the recovery thread
        // is started the current (process) thread goes back to a thread pool in
        // ThreadPoolExecutor. The recovery thread can than stop the listener and the
        // ThreadPoolExecutor since all threads are back in the pool. Any retries will
        // be done in the recovery thread.
        RecoveryThread recoveryThread = new RecoveryThread(this, anEndpoint, entry, isRequest,
                getAnalysisEngineController());
        Thread t = new Thread(Thread.currentThread().getThreadGroup().getParent(), recoveryThread);
        t.start();
      } else {
    	  try {
        	  CasStateEntry casStateEntry = getAnalysisEngineController().
				getLocalCache().lookupEntry(entry.getCasReferenceId());
        	  casStateEntry.setDeliveryToClientFailed();   // Mark the CAS, so that later we know that the delivery to client failed
        	  if ( anEndpoint != null ) {
				  // Add the reply destination (temp queue) to a dead client map
            	  Object clientDestination = anEndpoint.getDestination();
        		  if ( clientDestination != null && clientDestination instanceof TemporaryQueue ) {
        			  if ( !getAnalysisEngineController().
                	  		getDeadClientMap().containsKey(clientDestination.toString())) {
                    	  getAnalysisEngineController().
                    	  		getDeadClientMap().
                    	  			put(clientDestination.toString(),clientDestination.toString());
        			  }
        		  }
        	  }
        	  
    	  } catch( Exception e ) {
    		  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
    	                "dispatch", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
    	                "UIMAJMS_exception__WARNING", e);
    	  }
    	  
    	  
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.INFO,
                  CLASS_NAME.getName(),
                  "dispatch",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_send_reply_failed__INFO",
                  new Object[] { getAnalysisEngineController().getComponentName(),
                      brokerConnectionURL, endpointConnection.getEndpoint() });
        }
      }
    }
  }

  private void sendCasToRemoteEndpoint(boolean isRequest, Object aSerializedCAS, CacheEntry entry,
          Endpoint anEndpoint, boolean startTimer) throws AsynchAEException,
          ServiceShutdownException {
    CasStateEntry casStateEntry = null;
    long msgSize = 0;
    try {
      if (aborting) {
        return;
      }
      //  If this is a reply to a client, use the same broker URL that manages this service input queue.
      //  Otherwise this is a request so use a broker specified in the endpoint object.
      String brokerConnectionURL = (anEndpoint.isReplyEndpoint()) ? serverURI : anEndpoint.getServerURI();

      casStateEntry = getAnalysisEngineController().getLocalCache().lookupEntry(
              entry.getCasReferenceId());
      if (casStateEntry == null) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.WARNING,
                CLASS_NAME.getName(),
                "sendCasToRemoteDelegate",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_unable_to_send_reply__WARNING",
                new Object[] { getAnalysisEngineController().getComponentName(),
                  anEndpoint.getDestination(), brokerConnectionURL, 
                  entry.getInputCasReferenceId() == null ? "" : entry.getInputCasReferenceId(), 
                          entry.getCasReferenceId(), 0, 
                          new Exception("Unable to lookup entry in Local Cache for a given Cas Id")  });
        return;
      }

      // Get the connection object for a given endpoint
      JmsEndpointConnection_impl endpointConnection = getEndpointConnection(anEndpoint);

      if (!endpointConnection.isOpen()) {
        if (!isRequest) {
          return;
        }
      }
      Message tm = null;
      try {
        if (anEndpoint.getSerialFormat() == SerialFormat.XMI) {
          tm = endpointConnection.produceTextMessage((String)aSerializedCAS);
          if (aSerializedCAS != null) {
            msgSize = ((String)aSerializedCAS).length();
          }
          tm.setIntProperty(AsynchAEMessage.Payload, AsynchAEMessage.XMIPayload);
        } else {
          // Create empty JMS Bytes Message
          tm = endpointConnection.produceByteMessage((byte[])aSerializedCAS);
          if (aSerializedCAS != null) {
            msgSize = ((byte[])aSerializedCAS).length;
          }
          tm.setIntProperty(AsynchAEMessage.Payload, AsynchAEMessage.BinaryPayload);
        }
      } catch (AsynchAEException ex) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.WARNING,
                  CLASS_NAME.getName(),
                  "sendCasToRemoteDelegate",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_unable_to_send_reply__WARNING",
                  new Object[] { getAnalysisEngineController().getComponentName(),
                  	anEndpoint.getDestination(), brokerConnectionURL, entry.getInputCasReferenceId() == null ? "" : entry.getInputCasReferenceId(), entry.getCasReferenceId(), 0, ex  });
        return;
      }
      // Add Cas Reference Id to the outgoing JMS Header
      tm.setStringProperty(AsynchAEMessage.CasReference, entry.getCasReferenceId());
      // Add common properties to the JMS Header
      if (isRequest == true) {
        populateHeaderWithRequestContext(tm, anEndpoint, AsynchAEMessage.Process);
      } else {
        populateHeaderWithResponseContext(tm, anEndpoint, AsynchAEMessage.Process);   
        tm.setBooleanProperty(AsynchAEMessage.SentDeltaCas, entry.sentDeltaCas());
      }
      // The following is true when the analytic is a CAS Multiplier
      if (casStateEntry.isSubordinate() && !isRequest) {
        // Override MessageType set in the populateHeaderWithContext above.
        // Make the reply message look like a request. This message will contain a new CAS
        // produced by the CAS Multiplier. The client will treat this CAS
        // differently from the input CAS.
        tm.setIntProperty(AsynchAEMessage.MessageType, AsynchAEMessage.Request);

        isRequest = true;
        // Save the id of the parent CAS
        tm.setStringProperty(AsynchAEMessage.InputCasReference, getTopParentCasReferenceId(entry
                .getCasReferenceId()));
        // Add a sequence number assigned to this CAS by the controller
        tm.setLongProperty(AsynchAEMessage.CasSequence, entry.getCasSequence());
        // If this is a Cas Multiplier, add a reference to a special queue where
        // the client sends Free Cas Notifications
        if (freeCASTempQueue != null) {
          // Attach a temp queue to the outgoing message. This is a queue where
          // Free CAS notifications need to be sent from the client
          tm.setJMSReplyTo(freeCASTempQueue);
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "sendCasToRemoteEndpoint",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_send_cas_to_collocated_service_detail__FINE",
                  new Object[] { getAnalysisEngineController().getComponentName(), "Remote",
                      anEndpoint.getEndpoint(), entry.getCasReferenceId(),
                      entry.getInputCasReferenceId(), entry.getInputCasReferenceId() });
        }
      }
      dispatch(tm, anEndpoint, entry, isRequest, endpointConnection, msgSize);

    } catch (JMSException e) {
      // Unable to establish connection to the endpoint. Logit and continue
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "sendCasToRemoteDelegate", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_unable_to_connect__INFO",
                new Object[] { getAnalysisEngineController().getName(), anEndpoint.getEndpoint() });
      }
    } catch (ServiceShutdownException e) {
      throw e;
    } catch (AsynchAEException e) {
      throw e;
    } catch (Exception e) {
      throw new AsynchAEException(e);
    }

  }

  private Delegate lookupDelegate(String aDelegateKey) {
    if (getAnalysisEngineController() instanceof AggregateAnalysisEngineController) {
      Delegate delegate = ((AggregateAnalysisEngineController) getAnalysisEngineController())
              .lookupDelegate(aDelegateKey);
      return delegate;
    }
    return null;
  }

  private void addCasToOutstandingList(CacheEntry entry, boolean isRequest, String aDelegateKey) {
    Delegate delegate = null;
    if (isRequest && (delegate = lookupDelegate(aDelegateKey)) != null) {
      delegate.addCasToOutstandingList(entry.getCasReferenceId(), entry.getCas().hashCode(), false);  // false=dont start timer thread per CAS
    }
  }

  private void removeCasFromOutstandingList(CacheEntry entry, boolean isRequest, String aDelegateKey) {
    Delegate delegate = null;
    if (isRequest && (delegate = lookupDelegate(aDelegateKey)) != null) {
      delegate.removeCasFromOutstandingList(entry.getCasReferenceId());
    }
  }

  private String getTopParentCasReferenceId(String casReferenceId) throws Exception {
    if (!getAnalysisEngineController().getLocalCache().containsKey(casReferenceId)) {
      return null;
    }
    CasStateEntry casStateEntry = getAnalysisEngineController().getLocalCache().lookupEntry(
            casReferenceId);

    if (casStateEntry.isSubordinate()) {
      // Recurse until the top CAS reference Id is found
      return getTopParentCasReferenceId(casStateEntry.getInputCasReferenceId());
    }
    // Return the top ancestor CAS id
    return casStateEntry.getCasReferenceId();
  }

  private void addIdleTime(Message aMessage) {
    long t = System.nanoTime();
    getAnalysisEngineController().saveReplyTime(t, "");
  }
  public void stop() {
	    stop(Channel.CloseAllChannels, true);
}

  public void stop(boolean shutdownNow) {
	    stop(Channel.CloseAllChannels, shutdownNow);
  }

  public void stop(int channelsToClose, boolean shutdownNow) {
    aborting = true;
    try {
      // Fetch iterator over all Broker Connections. This service may be connected
      // to many brokers. Each broker connection may handle multiple sessions to
      // different reply queues
      Iterator it = connectionMap.keySet().iterator();
      JmsEndpointConnection_impl endpointConnection = null;
      // iterate over connections
      while (it.hasNext()) {
        // The key is the broker URL
        String key = (String) it.next();
        // Fetch a connection object for a given URL
        Object value = connectionMap.get(key);

        if (value instanceof BrokerConnectionEntry) {
          BrokerConnectionEntry brokerConnectionEntry = (BrokerConnectionEntry) value;
          // A connection object may have many endpoint objects. There is a separate
          // endpoint object per reply queue.
          Iterator replyEndpointIterator = brokerConnectionEntry.endpointMap.keySet().iterator();
          // Iterate over endpoints, each representing a reply queue
          while (replyEndpointIterator.hasNext()) {
            // Get endpoint object for a reply queue. The abort() call below
            // just closes a session and a producer. The JMS Connection is closed
            // outside of this while-loop when we clean up all the sessions.
            endpointConnection = brokerConnectionEntry.endpointMap
                    .get(replyEndpointIterator.next());
            // Close the session and the producer
            endpointConnection.abort();
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(
                      Level.INFO,
                      CLASS_NAME.getName(),
                      "stop",
                      JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_forced_endpoint_close__INFO",
                      new Object[] { getAnalysisEngineController().getName(),
                          endpointConnection.getEndpoint(), endpointConnection.getServerUri() });
            }
          }
          // Cancel any pending timers and finally close the JMS Connection to the
          // broker
          if (brokerConnectionEntry != null) {
            if (brokerConnectionEntry.getConnectionTimer() != null) {
              brokerConnectionEntry.getConnectionTimer().cancelTimer();
            }
            if (brokerConnectionEntry.getConnection() != null) {
              try {
                brokerConnectionEntry.getConnection().close();
              } catch (Exception ex) { /* ignore, we are stopping */
              }
            }
          }
        }
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "stop",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_output_channel_aborted__INFO",
                new Object[] { getAnalysisEngineController().getName() });
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "sendReply", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", getAnalysisEngineController().getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "stop", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    }
  }


  public void cancelTimers() {
    if (connectionMap.size() > 0) {
      Iterator<String> it = connectionMap.keySet().iterator();
      while (it.hasNext()) {
        String key = it.next();
        BrokerConnectionEntry ce = (BrokerConnectionEntry) connectionMap.get(key);
        if (ce != null && ce.getConnectionTimer() != null) {
          ce.getConnectionTimer().cancelTimer();
        }
      }
    }
  }

  public static class BrokerConnectionEntry {
    private String brokerURL;

    private Connection connection;

    private ConnectionTimer connectionTimer;

    Map<Object, JmsEndpointConnection_impl> endpointMap = new ConcurrentHashMap<Object, JmsEndpointConnection_impl>();

    public String getBrokerURL() {
      return brokerURL;
    }

    public void setConnectionTimer(ConnectionTimer aConnectionTimer) {
      connectionTimer = aConnectionTimer;
    }

    public ConnectionTimer getConnectionTimer() {
      return connectionTimer;
    }

    public void setBrokerURL(String brokerURL) {
      this.brokerURL = brokerURL;
    }

    public Connection getConnection() {
      return connection;
    }

    public void setConnection(Connection connection) {
      this.connection = connection;
    }

    public void addEndpointConnection(Object key, JmsEndpointConnection_impl endpointConnection) {
      endpointMap.put(key, endpointConnection);
//      System.out.println("----------- Endpoint Map for Endpoint:"+key+" Has:"+endpointMap.size()+" Entries");
    }

    public JmsEndpointConnection_impl getEndpointConnection(Object key) {
      return endpointMap.get(key);
    }

    public boolean endpointExists(Object key) {
      return endpointMap.containsKey(key);
    }

    public void removeEndpoint(Object key) {
      endpointMap.remove(key);
    }
  }

  protected class ConnectionTimer {
    private final Class CLASS_NAME = ConnectionTimer.class;

    private long inactivityTimeout;

    private AnalysisEngineController controller;

    private BrokerConnectionEntry brokerDestinations;

    private long connectionCreationTimestamp;

    private String componentName = "";

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    private boolean doLog = true;
    
    public ConnectionTimer(BrokerConnectionEntry aBrokerDestinations) {
      brokerDestinations = aBrokerDestinations;
    }

    public synchronized void setInactivityTimeout(long anInactivityTimeout) {
      inactivityTimeout = anInactivityTimeout;
    }

    public void setAnalysisEngineController(AnalysisEngineController aController) {
      controller = aController;
      if (controller != null) {
        componentName = controller.getComponentName();
      }
    }

    public void setConnectionCreationTimestamp(long aConnectionCreationTimestamp) {
      connectionCreationTimestamp = aConnectionCreationTimestamp;
    }

    public void stopSessionReaperTimer() {
      if ( scheduler != null ) {
    	  scheduler.shutdownNow();
      }
    }
    /**
     * Schedules regular cleanup of JMS sessions. A session is cleaned up (closed) if it has not
     * been used in an interval defined by value in inactivityTimeout.
     * 
     * @param aComponentName
     */
    public synchronized void startSessionReaperTimer( String aComponentName) {
        //	Fire the runnable at fixed intervals equal to inactivityTimeout value
        scheduler.scheduleWithFixedDelay(new Runnable(){
            public void run() {
              //  System.out.println("SessionReaper Thread Woke Up After:"+inactivityTimeout*60*1000+" Millis");
                long inactivityThreshold = inactivityTimeout*60*1000;  // normalize into millis
                if ( doLog && UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                  doLog = false;
                  UIMAFramework.getLogger(CLASS_NAME).logrb(
                          Level.INFO,
                          CLASS_NAME.getName(),
                          "startSessionReaperTimer.run",
                          JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAJMS_inactivity_timer_expired__INFO",
                          new Object[] { Thread.currentThread().getId(), componentName,
                              inactivityTimeout, brokerDestinations.getBrokerURL() });
                }
                try {
                    connectionSemaphore.acquire();
                    if (brokerDestinations.getConnection() != null
                            && !((ActiveMQConnection) brokerDestinations.getConnection()).isClosed()) {
                      try {
                      	Iterator<Entry<Object, JmsEndpointConnection_impl>> it = 
                      			brokerDestinations.endpointMap.entrySet().iterator();
                      	//	loop through all endpoint wrappers (JmsEndpointConnection_impl) and
                      	//  close sessions which have not been used for a defined (inactivity_timeout)
                      	//  amount of time. 
                      	while( it.hasNext() ) {
                      		Entry<Object, JmsEndpointConnection_impl> value = it.next();
                      		long lastDispatchTime = value.getValue().lastDispatchTimestamp.get();
//                			System.out.println("-------- lastDispatchTime:"+lastDispatchTime+" Delta:"+(System.currentTimeMillis() - lastDispatchTime)+" InactivityThreshold:"+inactivityThreshold);
                      		if ( lastDispatchTime > 0 && (System.currentTimeMillis() - lastDispatchTime) >= inactivityThreshold ) {
                      			value.getValue().close();  // close the jms session
                      			it.remove();
//                    			System.out.println("-------- Closing Session for Destination:"+value.getValue().delegateEndpoint.getDestination());

//                    			System.out.println("-------- Closing Session for Destination:"+value.getValue().delegateEndpoint.getDestination());
//                                UIMAFramework.getLogger(CLASS_NAME).logrb(
//                                        Level.INFO,
//                                        CLASS_NAME.getName(),
//                                        "startTimer",
//                                        JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
//                                        "UIMAJMS_removed_expired_session__INFO",
//                                        new Object[] { Thread.currentThread().getId(), componentName,
//                                            inactivityTimeout, value.getValue().delegateEndpoint.getDestination(), brokerDestinations.getBrokerURL()  });

                    		}
                      	}
                      } catch (Exception e) {
                      	e.printStackTrace();
                      } finally {
                        try {
                      	  if ( brokerDestinations.endpointMap.isEmpty() ) {
                          	  brokerDestinations.getConnection().stop();
                                brokerDestinations.getConnection().close();
                                brokerDestinations.setConnection(null);
                                brokerDestinations.endpointMap.clear();
                                connectionMap.remove(brokerDestinations);
                                stopSessionReaperTimer();

                                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
                                    UIMAFramework.getLogger(CLASS_NAME).logrb(
                                            Level.FINE,
                                            CLASS_NAME.getName(),
                                            "startTimer",
                                            JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                                            "UIMAJMS_closing_broker_connection__FINE",
                                            new Object[] { Thread.currentThread().getId(), componentName,
                                                brokerDestinations.getBrokerURL(),inactivityTimeout  });
                                  }
                      	  }
                        } catch( Exception e) {
                        	UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                                    "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                                    "UIMAJMS_exception__WARNING", e);
                        }
                      }
                    }
                  } catch (Exception e) {
                	  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                              "sendReply", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                              "UIMAJMS_exception__WARNING", e);
                  } finally {
                	  connectionSemaphore.release();
                  }

            }

        }, inactivityTimeout * 60, inactivityTimeout * 60,TimeUnit.SECONDS);
      }
    private void cancelTimer() {
      if ( scheduler != null ) {
        scheduler.shutdownNow();
      }
    }

    public synchronized void stopTimer() {
      cancelTimer();
    }
  }

  private static class RecoveryThread implements Runnable {
    Endpoint endpoint;

    CacheEntry entry;

    boolean isRequest;

    AnalysisEngineController controller;

    JmsOutputChannel outputChannel;

    public RecoveryThread(JmsOutputChannel channel, Endpoint anEndpoint, CacheEntry anEntry,
            boolean isRequest, AnalysisEngineController aController) {
      endpoint = anEndpoint;
      entry = anEntry;
      controller = aController;
      this.isRequest = isRequest;
      outputChannel = channel;
    }

    public void run() {
      Delegate delegate = outputChannel.lookupDelegate(endpoint.getDelegateKey());
      // Removes the failed CAS from the list of CASes pending reply. This also
      // cancels the timer if this CAS was the oldest pending CAS, and if there
      // are other CASes pending a fresh timer is started.
      outputChannel.removeCasFromOutstandingList(entry, isRequest, endpoint.getDelegateKey());
      if (delegate != null) {
        // Mark this delegate as Failed
        delegate.getEndpoint().setStatus(Endpoint.FAILED);
        // Destroy listener associated with a reply queue for this delegate
        InputChannel ic = controller.getInputChannel(delegate.getEndpoint().getDestination()
                .toString());
        if (ic != null && delegate != null && delegate.getEndpoint() != null) {
          ic.destroyListener(delegate.getEndpoint().getDestination().toString(), endpoint
                  .getDelegateKey());
        }
        // Setup error context and handle failure in the error handler
        ErrorContext errorContext = new ErrorContext();
        errorContext.add(AsynchAEMessage.Command, AsynchAEMessage.Process);
        errorContext.add(AsynchAEMessage.CasReference, entry.getCasReferenceId());
        errorContext.add(AsynchAEMessage.Endpoint, endpoint);
        errorContext.handleSilently(true); // dont dump exception to the log
        // Failure on send treat as timeout
        delegate.handleError(new MessageTimeoutException(), errorContext);
      }

    }
  }
}
