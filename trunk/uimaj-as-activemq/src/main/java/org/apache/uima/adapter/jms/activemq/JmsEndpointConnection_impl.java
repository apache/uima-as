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
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ConnectionFailedException;
import org.apache.activemq.advisory.ConsumerEvent;
import org.apache.activemq.advisory.ConsumerListener;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.InputChannel;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.BaseAnalysisEngineController;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.AsynchAEException;
import org.apache.uima.aae.error.DelegateConnectionLostException;
import org.apache.uima.aae.error.InvalidMessageException;
import org.apache.uima.aae.error.ServiceShutdownException;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.activemq.JmsOutputChannel.BrokerConnectionEntry;
import org.apache.uima.cas.CAS;
import org.apache.uima.util.Level;


public class JmsEndpointConnection_impl implements ConsumerListener {
  private static final Class CLASS_NAME = JmsEndpointConnection_impl.class;

  // timestamp containing time when the last message was dispatched from this dispatcher to 
  // jms destination. This is updated every time a message is dispatched to a queue. 
  // At fixed intervals a cleanup thread wakes up and checks for unused dispatchers by
  // comparing value in lastDispatchTimestamp to a max allowed which by default is 5 minutes.
  protected AtomicLong lastDispatchTimestamp = new AtomicLong(0);

  protected Destination destination;

  protected Session producerSession;

  private MessageProducer producer;

  private BrokerConnectionEntry brokerDestinations;

  private String serverUri;

  private String endpoint;

  private String endpointName;

  protected Endpoint delegateEndpoint;

  private volatile boolean retryEnabled;

  private AnalysisEngineController controller = null;

  private volatile boolean connectionAborted = false;

  protected static long connectionCreationTimestamp = 0L;

  private Object semaphore = new Object();

  protected boolean isReplyEndpoint;

  private volatile boolean failed = false;

  private Object lock = new Object();

  private final String componentName;

  // Create semaphore to control creation of a JMS connection.
  // This semaphore is shared by all instances of this class
  private static Semaphore connectionSemaphore = new Semaphore(1);
  
  public JmsEndpointConnection_impl(BrokerConnectionEntry aBrokerDestinationMap,
          Endpoint anEndpoint, AnalysisEngineController aController) {
    brokerDestinations = aBrokerDestinationMap;
    if ( anEndpoint.isFreeCasEndpoint() && anEndpoint.isCasMultiplier() && anEndpoint.isReplyEndpoint()) {
      serverUri = anEndpoint.getServerURI();
    } else {
      //  If this is a reply to a client, use the same broker URL that manages this service input queue.
      //  Otherwise this is a request so use a broker specified in the endpoint object.
      serverUri = (anEndpoint.isReplyEndpoint()) ? 
              ((JmsOutputChannel) aController.getOutputChannel()).getServerURI() : anEndpoint.getServerURI();
    }
    isReplyEndpoint = anEndpoint.isReplyEndpoint();
    controller = aController;

    if ((anEndpoint.getCommand() == AsynchAEMessage.Stop || isReplyEndpoint)
            && anEndpoint.getDestination() != null
            && anEndpoint.getDestination() instanceof ActiveMQDestination) {
      endpoint = ((ActiveMQDestination) anEndpoint.getDestination()).getPhysicalName();
    } else {
      endpoint = anEndpoint.getEndpoint();
    }
    anEndpoint.remove();
    componentName = controller.getComponentName();
    delegateEndpoint = anEndpoint;
  }

  public boolean isRetryEnabled() {
    return retryEnabled;
  }

  public void setRetryEnabled(boolean retryEnabled) {
    this.retryEnabled = retryEnabled;
  }

  public boolean isOpen() {
	synchronized (lock) {
	    if (failed || producerSession == null || connectionClosedOrFailed(brokerDestinations)) {
	        return false;
	      }
	      return ((ActiveMQSession) producerSession).isRunning();
	}
  }

  protected static boolean connectionClosedOrFailed(BrokerConnectionEntry aBrokerDestinationMap) {
    Connection connection = aBrokerDestinationMap.getConnection();
    if (connection == null
            || ((ActiveMQConnection) connection).isClosed()
            || ((ActiveMQConnection) connection).isClosing()
            || ((ActiveMQConnection) connection).isTransportFailed()) {
      return true;
    }
    return false;
  }

  private void openChannel() throws AsynchAEException, ServiceShutdownException {
    openChannel(getServerUri(), componentName, endpoint, controller);
  }

  private void openChannel(String brokerUri, String aComponentName,
          String anEndpointName, AnalysisEngineController aController) throws AsynchAEException,
          ServiceShutdownException {
	  synchronized (lock) {
		    try {

		        // If replying to http request, reply to a queue managed by this service broker using tcp
		        // protocol
		        if (isReplyEndpoint && brokerUri.startsWith("http")) {
		          brokerUri = ((JmsOutputChannel) aController.getOutputChannel()).getServerURI();

		          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
		            UIMAFramework.getLogger(CLASS_NAME).logrb(
		                    Level.FINE,
		                    CLASS_NAME.getName(),
		                    "open",
		                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                    "UIMAJMS_override_connection_to_endpoint__FINE",
		                    new Object[] {  aComponentName, getEndpoint(),
		                      ((JmsOutputChannel) aController.getOutputChannel()).getServerURI() });
		          }
		        } else if ( !brokerUri.startsWith("http") && !brokerUri.startsWith("failover")){
		              brokerUri += "?wireFormat.maxInactivityDuration=0";
		        }

		        if (!isOpen()) {
		          Connection conn = null;
		          //  Check connection status and create a new one (if necessary) as an atomic operation
		          try {
		            connectionSemaphore.acquire();
		            
		            if (connectionClosedOrFailed(brokerDestinations)) {
		              // Create one shared connection per unique brokerURL.
		              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
		                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
		                        "openChannel", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                        "UIMAJMS_activemq_open__FINE",
		                        new Object[] { aController.getComponentName(), anEndpointName, brokerUri });
		              }
		              if ( brokerDestinations.getConnection() != null ) {
		                try {
		                  //  Close the connection to avoid leaks in the broker
		                  brokerDestinations.getConnection().close();
		                } catch( Exception e) {
		                  //  Ignore exceptions on a close of a bad connection
		                }
		              }
		              //System.out.println("---------- Opening New Broker Connection ---------------"+brokerUri);
		              ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUri);
		              //  Create shared jms connection to a broker
		              conn = factory.createConnection();
		              
		              factory.setDispatchAsync(true);
		              factory.setUseAsyncSend(true);
		              factory.setCopyMessageOnSend(false);
		              //  Cache the connection. There should only be one connection in the jvm
		              //  per unique broker url. 
		              brokerDestinations.setConnection(conn);
		              // Close and invalidate all sessions previously created from the old connection
		              Iterator<Map.Entry<Object, JmsEndpointConnection_impl>> it = brokerDestinations.endpointMap
		                      .entrySet().iterator();
		              while (it.hasNext()) {
		                Map.Entry<Object, JmsEndpointConnection_impl> entry = it.next();
		                if (entry.getValue().producerSession != null) {
		                  // Close session
		                  entry.getValue().producerSession.close();
		                  // Since we created a new connection invalidate session that
		                  // have been created with the old connection
		                  entry.getValue().producerSession = null;
		                }
		              }
		            }
		          } catch( Exception exc) {
		            throw exc; // rethrow
		          } finally {
		            connectionSemaphore.release();
		          }
		          
		          connectionCreationTimestamp = System.nanoTime();
		          failed = false;
		        } else {
		        	System.out.println("...... Reusing Existing Broker Connetion");
		        }
		        Connection conn = brokerDestinations.getConnection();
		        if (failed) {
		          // Unable to create a connection
		          return;
		        }

		        producerSession = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
	              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
	                      "openChannel", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
	                      "UIMAJMS_session_open__INFO",
	                      new Object[] { aComponentName, anEndpointName, brokerUri });

		        if ((delegateEndpoint.getCommand() == AsynchAEMessage.Stop || isReplyEndpoint)
		                && delegateEndpoint.getDestination() != null) {
		          producer = producerSession.createProducer(null);
		          if (aController != null) {
		            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
		              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
		                      "openChannel", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                      "UIMAJMS_temp_conn_starting__FINE",
		                      new Object[] { aComponentName, anEndpointName, brokerUri });
		            }
		          }
		        } else {
		          destination = producerSession.createQueue(getEndpoint());
		          producer = producerSession.createProducer(destination);
		          if (controller != null) {
		            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
		              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
		                      "openChannel", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                      "UIMAJMS_conn_starting__FINE",
		                      new Object[] { aComponentName, anEndpointName, brokerUri });
		            }
		          }
		        }
		        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		        // Since the connection is shared, start it only once
		        if (!((ActiveMQConnection) brokerDestinations.getConnection()).isStarted()) {
		          brokerDestinations.getConnection().start();
		        }
		        if (controller != null) {
		          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
		            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
		                    "openChannel", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                    "UIMAJMS_conn_started__FINE", new Object[] { endpoint, brokerUri });
		            if (controller.getInputChannel() != null) {
		              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
		                      "openChannel", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                      "UIMAJMS_connection_open_to_endpoint__FINE",
		                      new Object[] { aComponentName, getEndpoint(), brokerUri });
		            }
		          }
		        }
		        failed = false;
		      } catch (Exception e) {
		        boolean rethrow = true;
		        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
		          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
		                  "openChannel", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
		                  "UIMAEE_service_exception_WARNING", controller.getComponentName());
		          
		          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
		                  "openChannel", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                  "UIMAJMS_exception__WARNING", e);
		        }

		        if (e instanceof JMSException) {
		          rethrow = handleJmsException((JMSException) e);

		        }
		        if (rethrow) {
		          throw new AsynchAEException(e);
		        }
		      }
		  
	  }
  }

  public synchronized void open() throws AsynchAEException, ServiceShutdownException {
    open(delegateEndpoint.getEndpoint(), serverUri);
  }

  public synchronized void open( String anEndpointName, String brokerUri) throws AsynchAEException,
          ServiceShutdownException {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "open",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_open__FINE",
              new Object[] { anEndpointName, brokerUri });
    }
    if (!connectionAborted) {
      openChannel();
    }

  }

  public synchronized void abort() {
    connectionAborted = true;
    brokerDestinations.getConnectionTimer().stopTimer();
    try {
      this.close();
    } catch (Exception e) {
    }
  }

	public void close() throws Exception {
		synchronized (lock) {
			if (producer != null) {
				try {
					producer.close();
				} catch (Exception e) {
					// Ignore we are shutting down
				}
			}
			if (producerSession != null) {
				try {
					producerSession.close();
				} catch (Exception e) {
					// Ignore we are shutting down
				}
				producerSession = null;
			}
			if (destination != null) {
				destination = null;
			}
		}
	}

  protected String getEndpoint() {
    return endpoint;
  }

  protected void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }
  protected void setDelegateEndpoint(Endpoint delegateEndpoint) {
    this.delegateEndpoint = delegateEndpoint;
  }

  protected synchronized String getServerUri() {
    return serverUri;
  }

  protected synchronized void setServerUri(String serverUri) {
    this.serverUri = serverUri;
  }

  public TextMessage produceTextMessage(String aTextMessage) throws AsynchAEException {
	synchronized( lock ) {
		  if ( producerSession == null ) {
		      throw new AsynchAEException("Controller:"+controller.getComponentName()+" Unable to create JMS Message. Producer Session Not Initialized (Null)");
		    }
		    try {
		       if (aTextMessage == null) {
		          return producerSession.createTextMessage("");
		       } else {
		          return producerSession.createTextMessage(aTextMessage);
		       }
		     } catch (javax.jms.IllegalStateException e) {
		        try {
		          open();
		        } catch (ServiceShutdownException ex) {
		          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
		            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
		                    "produceTextMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
		                    "UIMAEE_service_exception_WARNING", controller.getComponentName());
		            
		            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
		                    "produceTextMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
		                    "UIMAJMS_exception__WARNING", ex);
		          }
		        } catch (AsynchAEException ex) {
		          throw ex;
		        }
		      } catch (Exception e) {
		        throw new AsynchAEException(e);
		    }
		    throw new AsynchAEException(new InvalidMessageException("Unable to produce Message Object"));
	}
  }

  public BytesMessage produceByteMessage(byte[] aSerializedCAS) throws AsynchAEException {
    synchronized( lock ) {
        if ( producerSession == null ) {
            throw new AsynchAEException("Controller:"+controller.getComponentName()+" Unable to create JMS Message. Producer Session Not Initialized (Null)");
          }
          int retryCount = 1;
          while (retryCount > 0) {
            try {
              retryCount--;
              BytesMessage bm = producerSession.createBytesMessage();
              bm.writeBytes(aSerializedCAS);

              return bm;

            } catch (javax.jms.IllegalStateException e) {
              try {
                open();
              } catch (ServiceShutdownException ex) {
                if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                          "produceByteMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_service_exception_WARNING", controller.getComponentName());
                  
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                          "produceByteMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAJMS_exception__WARNING", ex);
                }
              }

            } catch (Exception e) {
              throw new AsynchAEException(e);
            }
          }
          throw new AsynchAEException(
                  new InvalidMessageException("Unable to produce BytesMessage Object"));
    }
  }

  public ObjectMessage produceObjectMessage() throws AsynchAEException {
    synchronized( lock ) {
  	  if ( producerSession == null ) {
  	      throw new AsynchAEException("Controller:"+controller.getComponentName()+" Unable to create JMS Message. Producer Session Not Initialized (Null)");
  	    }
  	    try {
  	      if (!((ActiveMQSession) producerSession).isRunning()) {
  	        open();
  	      }
  	      return producerSession.createObjectMessage();
  	    } catch (Exception e) {
  	      throw new AsynchAEException(e);
  	    }
    }
  }

  private boolean delayCasDelivery(int msgType, Message aMessage, int command) throws Exception {

    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "recoverSession",
              JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_open_connection_to_endpoint__FINE",
              new Object[] { getEndpoint() });
    }
    openChannel();
    // The connection has been successful. Now check if we need to create a new listener
    // and a temp queue to receive replies. A new listener will be created only if the
    // endpoint for the delegate is marked as FAILED. This will be the case if the listener
    // on the reply queue for the endpoint has failed.
    String endpointName = delegateEndpoint.getEndpoint();
    synchronized (lock) {
      if (controller instanceof AggregateAnalysisEngineController) {
        // Using the queue name lookup the delegate key
        String key = ((AggregateAnalysisEngineController) controller)
                .lookUpDelegateKey(endpointName);
        if (key != null && destination != null && !isReplyEndpoint) {
          // For Process Requests check the state of the delegate that is to receive
          // the CAS. If the delegate state = TIMEOUT_STATE, push the CAS id onto
          // delegate's list of delayed CASes. The state of the delegate was
          // changed to TIMEOUT when a previous CAS timed out.
          if (msgType != AsynchAEMessage.Request && command == AsynchAEMessage.Process) {
            String casReferenceId = aMessage.getStringProperty(AsynchAEMessage.CasReference);
            if (casReferenceId != null ) {
              CAS cas = ((AggregateAnalysisEngineController) controller).getInProcessCache().getCasByReference(casReferenceId);
              if ( cas != null && ((AggregateAnalysisEngineController) controller)
                            .delayCasIfDelegateInTimedOutState(casReferenceId, endpointName, cas.hashCode())) {
                      return true;
               }
            }
          }
        }
      }
    }
    return false;

  }
  public boolean send(final Message aMessage, long msgSize, boolean startTimer) {
	 return send( aMessage, msgSize, startTimer, false);
  }

  public boolean send(final Message aMessage, long msgSize, boolean startTimer, boolean failOnJMSException) {
    String destinationName = "";
    String target = "Delegate";
    int msgType = 0;
    int command = 0;
    try {
      msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
      command = aMessage.getIntProperty(AsynchAEMessage.Command);
      boolean newCAS = false;
      if ( aMessage.propertyExists(AsynchAEMessage.CasSequence) &&
              aMessage.getLongProperty(AsynchAEMessage.CasSequence) > 0 ) {
        newCAS = true;
      }
      
      if ( msgType == AsynchAEMessage.Response || (msgType == AsynchAEMessage.Request && newCAS) ) {
        target = "Client";
      }
      Endpoint masterEndpoint = null;
      if ( delegateEndpoint != null && delegateEndpoint.getDelegateKey() != null ) {
        masterEndpoint = ((AggregateAnalysisEngineController) controller).lookUpEndpoint(
                delegateEndpoint.getDelegateKey(), false);
        // Endpoint is marked as FAILED by the aggregate when it detects broker connection
        // failure. In such an event the aggregate stops the listener on the delegate
        // reply queue.
        if ( msgType == AsynchAEMessage.Request && command == AsynchAEMessage.Process &&
             masterEndpoint != null && masterEndpoint.getStatus() == Endpoint.FAILED) {
          HashMap<Object, Object> map = new HashMap<Object, Object>();
          Delegate delegate = ((AggregateAnalysisEngineController) controller).lookupDelegate(delegateEndpoint.getDelegateKey());
          //  Cancel Delegate timer before entering Error Handler
          if ( delegate != null ) {
            delegate.cancelDelegateTimer();
          }
          //  Handle the Connection error in the ProcessErrorHandler
          map.put(AsynchAEMessage.Command, AsynchAEMessage.Process);
          map.put(AsynchAEMessage.CasReference, aMessage.getStringProperty(AsynchAEMessage.CasReference));
          map.put(AsynchAEMessage.Endpoint, masterEndpoint);
          Exception e = new DelegateConnectionLostException("Controller:"+controller.getComponentName()+" Lost Connection to "+target+ ":"+masterEndpoint.getDelegateKey());
          //  Handle error in ProcessErrorHandler
          ((BaseAnalysisEngineController)controller).handleError(map, e);
          return true; // return true as if this was successful send 
        }
      }

      if ( !isOpen() ) {
        if (delayCasDelivery(msgType, aMessage, command)) {
          // Return true as if the CAS was sent
          return true;
        }
      }
      // Stop messages and replies are sent to the endpoint provided in the destination object
      if ((command == AsynchAEMessage.Stop || command == AsynchAEMessage.ReleaseCAS || isReplyEndpoint)
              && delegateEndpoint.getDestination() != null) {
        destinationName = ((ActiveMQDestination) delegateEndpoint.getDestination())
                .getPhysicalName();
        if (UIMAFramework.getLogger().isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "send",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_sending_msg_to_endpoint__FINE",
                  new Object[] { destinationName });
        }
        logMessageSize(aMessage, msgSize, destinationName);
        synchronized (producer) {
          producer.send((Destination) delegateEndpoint.getDestination(), aMessage);
        }
      } else {
        destinationName = ((ActiveMQQueue) producer.getDestination()).getPhysicalName();
        if (UIMAFramework.getLogger().isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(), "send",
                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_sending_msg_to_endpoint__FINE",
                  new Object[] { destinationName });
        }
        logMessageSize(aMessage, msgSize, destinationName);
        synchronized (producer) {
          producer.send(aMessage);
        }

      }
      // Starts a timer on a broker connection. Every time a new message
      // is sent to a destination managed by the broker the timer is
      // restarted. The main purpose of the timer is to close connections
      // that are not used.
      if (startTimer) {
//        brokerDestinations.getConnectionTimer().startTimer(connectionCreationTimestamp,
//                delegateEndpoint);
      }
      // record the time when this dispatches sent a message. This time will be used
      // to find inactive sessions.
	  lastDispatchTimestamp.set(System.currentTimeMillis());
	  
	  if ( msgType == AsynchAEMessage.Response && command == AsynchAEMessage.GetMeta ) {
	      if (UIMAFramework.getLogger().isLoggable(Level.INFO)) {
	          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "send",
	                  JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_dispatched_getmeta_reply__INFO",
	                  new Object[] { controller.getComponentName(), destinationName });
          }
	  }
      
	  
	  
	  
      // Succeeded sending the CAS
      return true;
    } catch (Exception e) {
    	
    	 // if a client terminates with an outstanding request, the service will not
        // be able to deliver a reply. Just log the fact that the reply queue is
        // no longer available.
      if ( e instanceof InvalidDestinationException && "Client".equals(target) ) {
          if ( delegateEndpoint != null ) {
            endpointName = ((ActiveMQDestination) delegateEndpoint.getDestination())
            		.getPhysicalName();
          }
    	  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "send", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_destination__INFO",
                  new Object[] { controller.getComponentName(),endpointName });
    	  if ( command == AsynchAEMessage.ServiceInfo ) {
    		  return false;
    	  }
    	  if ( (msgType == AsynchAEMessage.Response || msgType == AsynchAEMessage.Request ) &&
    			  command == AsynchAEMessage.Process ) {
    		  String casReferenceId="";
    		  try {
    		     casReferenceId = aMessage.getStringProperty(AsynchAEMessage.CasReference);
    		  } catch( Exception exx ) {
    		        String key = "";
    		        String endpointName = "";
    		        if ( delegateEndpoint != null ) {
    		          delegateEndpoint.getDelegateKey();
    		          endpointName = ((ActiveMQDestination) delegateEndpoint.getDestination())
    		          .getPhysicalName();
    		        }
    	          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
    	                  "send", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
    	                  "UIMAEE_service_delivery_exception__WARNING",new Object[] { controller.getComponentName(), key, endpointName});
    		  }
   		      CasStateEntry casStateEntry = controller.getLocalCache().lookupEntry(casReferenceId);
   		      // Mark the CAS as failed so that the CAS is released and cache cleaned up
    		  casStateEntry.setDeliveryToClientFailed();
    	  }
    	  return true;  // expect the client can go away at any time. Not an error
      }    	
    	
    	
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        
        String key = "";
        String endpointName = "";
        if ( delegateEndpoint != null ) {
          delegateEndpoint.getDelegateKey();
          endpointName = ((ActiveMQDestination) delegateEndpoint.getDestination())
          .getPhysicalName();
        }
        if ( "Client".equals(target) ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "send", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_delivery_to_client_exception__WARNING",
                  new Object[] { controller.getComponentName(),endpointName });
        } else {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "send", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_delivery_exception__WARNING",new Object[] { controller.getComponentName(), key, endpointName});
        }

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "send", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
      // If the controller has been stopped no need to send messages
      if (controller.isStopped()) {
        return false;
       } else {
        if (e instanceof JMSException) {
          handleJmsException((JMSException) e);
          //	whoever called this method is interested in knowing that there was JMS Exception
          if ( failOnJMSException ) {
        	  return false;
          }
        } else {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "send", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_service_exception_WARNING", controller.getComponentName());

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "send",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_exception__WARNING",
                    e);
          }
        }

      }
    }
    //brokerDestinations.getConnectionTimer().stopTimer();
    // Failed here
    return false;
  }

  private void logMessageSize(Message aMessage, long msgSize, String destinationName) {
    if (UIMAFramework.getLogger().isLoggable(Level.FINE)) {
      boolean isReply = false;
      if (isReplyEndpoint) {
        isReply = true;
      }
      String type = "Text";
      if (aMessage instanceof BytesMessage) {
        type = "Binary";
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "logMessageSize",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_msg_size__FINE",
                new Object[] { componentName, isReply == true ? "Reply" : "Request", "Binary",
                    destinationName, msgSize });
      } else if (aMessage instanceof TextMessage) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(
                Level.FINE,
                CLASS_NAME.getName(),
                "logMessageSize",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_msg_size__FINE",
                new Object[] { componentName, isReply == true ? "Reply" : "Request", "XMI",
                    destinationName, msgSize });
      }
    }
  }

  /**
   * This method is called during recovery of failed connection. It is only called if the endpoint
   * associated with a given delegate is marked as FAILED. It is marked that way when a listener
   * attached to the reply queue fails. This method creates a new listener and a new temp queue.
   * 
   * @param delegateKey
   * @throws Exception
   */
  private void createListener(String delegateKey) throws Exception {
    if (controller instanceof AggregateAnalysisEngineController) {
      // Fetch an InputChannel that handles messages for a given delegate
      InputChannel iC = controller.getReplyInputChannel(delegateKey);
      // Create a new Listener, new Temp Queue and associate the listener with the Input Channel
      iC.createListener(delegateKey, null);
    }
  }

  private synchronized boolean handleJmsException(JMSException ex) {
    if (!failed) {
      failed = true;
    }
    try {
      // Check if the exception is due to deleted queue. ActiveMQ does not identify
      // this condition in the cause, so we need to parse the exception message and
      // compare against "Cannot publish to a deleted Destination" text. If match is
      // found, extract the name of the deleted queue from the exception and log it.
      if (ex.getMessage() != null
              && ex.getMessage().startsWith("Cannot publish to a deleted Destination")) {
        String destName = endpointName;
        int startPos = ex.getMessage().indexOf(':');
        if (startPos > 0) {
          destName = ex.getMessage().substring(startPos);
        }
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "handleJmsException", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_send_failed_deleted_queue_INFO",
                  new Object[] { componentName, destName, serverUri });
        }
        return false;

      }
      if (ex instanceof ConnectionFailedException && isReplyEndpoint) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "handleJmsException", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_connection_failure__INFO",
                new Object[] { componentName, serverUri, delegateEndpoint.getDestination() });
        return false;

      } else {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handleJmsException", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", controller.getComponentName());

          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handleJmsException", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", ex);
        }
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "handleJmsException", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", controller.getComponentName());

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(), "handleJmsException",
                JmsConstants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_exception__WARNING",
                e);
      }
    }

    return true;
  }

  public void onConsumerEvent(ConsumerEvent arg0) {
    if (controller != null) {
      controller.handleDelegateLifeCycleEvent(getEndpoint(), arg0.getConsumerCount());
    }
  }

  protected synchronized void finalize() throws Throwable {
//    brokerDestinations.getConnectionTimer().stopTimer();
  }

}
