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

package org.apache.uima.adapter.jms.client;

import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.client.UimaASProcessStatus;
import org.apache.uima.aae.client.UimaASProcessStatusImpl;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.message.UimaMessageValidator;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngineCommon_impl.ClientRequest;
import org.apache.uima.adapter.jms.client.BaseUIMAAsynchronousEngineCommon_impl.SharedConnection;
import org.apache.uima.adapter.jms.message.PendingMessage;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.jms.error.handler.BrokerConnectionException;
import org.apache.uima.util.Level;
import org.apache.uima.util.impl.ProcessTrace_impl;

/**
 * Creates a worker thread for sending messages. This is an abstract implementation that provides a
 * thread with run logic. The concrete implementation of the Worker Thread must extend this class.
 * The application threads share a special in-memory queue with this worker thread. The application
 * threads add jms messages to the pendingMessageList queue and the worker thread consumes them. The
 * worker thread terminates when the uima ee client calls doStop() method.
 */
public abstract class BaseMessageSender implements Runnable, MessageSender {
  private static final Class CLASS_NAME = BaseMessageSender.class;

  // A reference to a shared queue where application threads enqueue messages
  // to be sent
  protected BlockingQueue<PendingMessage> messageQueue = new LinkedBlockingQueue<PendingMessage>();

  // Global flag controlling lifecycle of this thread. It will be set to true
  // when the
  // uima ee engine calls doStop()
  protected volatile boolean done;

  // A reference to the uima ee client engine
  protected BaseUIMAAsynchronousEngineCommon_impl engine;

  // Global flag to indicate failure of the worker thread
  protected volatile boolean workerThreadFailed;

  // If the worker thread fails, store the reason for the failure
  protected Exception exception;

  // These are required methods to be implemented by a concrete implementation

  // Returns instance of JMS MessageProducer
  public abstract MessageProducer getMessageProducer();

  // Provides implementation-specific implementation logic. It is expected
  // that this
  // message creates an instance of JMS MessageProducer
  protected abstract void initializeProducer() throws Exception;

  // Releases resources
  protected abstract void cleanup() throws Exception;

  // Returns the name of the destination
  protected abstract String getDestinationEndpoint() throws Exception;

  public abstract void setConnection(Connection connection);
  
  private MessageProducer producer = null;
  

  public BaseMessageSender(BaseUIMAAsynchronousEngineCommon_impl anEngine) {
    messageQueue = anEngine.pendingMessageQueue;
    engine = anEngine;
    try {
      // Acquire a shared lock. Release it in the run() method once we initialize
      // the producer.
      engine.producerSemaphore.acquire();
    } catch (InterruptedException e) {
    }
  }

  /**
   * Stops the worker thread
   */
  public void doStop() {
    done = true;
    // Create an empty message to deliver to the queue that is blocking
    PendingMessage emptyMessage = new PendingMessage(0);
    messageQueue.add(emptyMessage);
  }

  /**
   * Return the Exception that caused the failure in this worker thread
   * 
   * @return - Exception
   */
  public Exception getReasonForFailure() {
    return exception;
  }

  /**
   * The uima ee client should call this method to check if there was a failure. The method returns
   * true if there was a failure or false otherwise. If true, the uima ee client can call
   * getReasonForFailure() to get the reason for failure
   */
  public boolean failed() {
    return workerThreadFailed;
  }
  /**
   * This method determines if a given request message should be rejected or not. Rejected in a sense
   * that it will not be sent to the destination due to the fact the broker connection is down.
   */
  private boolean reject(PendingMessage pm ) {
    return reject(pm, new BrokerConnectionException("Unable To Deliver Message To Destination. Connection To Broker "+engine.getBrokerURI()+" Has Been Lost"));
  }
  
  /**
   * This method determines if a given request message should be rejected or not. Rejected in a sense
   * that it will not be sent to the destination due to the fact the broker connection is down.
   */
  private boolean reject( PendingMessage pm, Exception e ) {
    boolean rejectRequest = false;
    //  If the connection to a broker was lost, notify the client
    //  and reject the request unless this is getMeta Ping request.
    SharedConnection sharedConnection = engine.lookupConnection(engine.getBrokerURI());
    if ( sharedConnection != null && !sharedConnection.isConnectionValid() ) {
      String messageKind = "";
      if (pm.getMessageType() == AsynchAEMessage.GetMeta ) {
        messageKind = "GetMeta";
      } else if (pm.getMessageType() == AsynchAEMessage.Process ) {
        messageKind = "Process";
      } else if (pm.getMessageType() == AsynchAEMessage.CollectionProcessComplete ) {
        messageKind = "CollectionProcessComplete";
      }
      rejectRequest = true;
      try {
        //  Is this is a Process request
        if (pm.getMessageType() == AsynchAEMessage.Process) {
          //  fetch the cache entry for this CAS
          ClientRequest cacheEntry = (ClientRequest) engine.getCache().get(
                  pm.get(AsynchAEMessage.CasReference));
          if ( cacheEntry != null ) {
            //  We are rejecting any Process requests until connection to broker
            //  is recovered
            cacheEntry.setProcessException();
            //  if the request was via synchronous API dont notify listeners
            //  instead the code will throw exception to the client
            boolean notifyListener = (cacheEntry.isSynchronousInvocation() == false);
            //  handle rejected request
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, getClass().getName(),
                      "reject", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_client_rejected_process_request_broker_down__INFO", new Object[] { messageKind });
            }
            //  Sets the state of a remote delegate to handle subsequent requests
            engine.serviceDelegate.setState(Delegate.TIMEOUT_STATE);
            //  handle exception but dont rethrow the exception
            engine.handleException(e, cacheEntry.getCasReferenceId(), null, cacheEntry, notifyListener, false);
          }
        } else {
          //  Dont handle GetMeta Ping. Let it flow through. The Ping will be done once the connection is recovered
          if ( !engine.serviceDelegate.isAwaitingPingReply()) {
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, getClass().getName(),
                      "reject", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_client_rejected_nonprocess_request_broker_down__INFO", new Object[] { messageKind });
            }
            engine.handleNonProcessException(e);
          } else if ( pm.getMessageType() == AsynchAEMessage.GetMeta ){
            rejectRequest = false;   // Don't reject GetMeta Ping 
          }
        }
      } catch( Exception ex ) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                  "reject", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_exception__WARNING", ex);
        }
      }
    }
    return rejectRequest;
  }
  /**
   * Initializes jms message producer and starts the main thread. This thread waits for messages
   * enqueued by application threads. The application thread adds a jms message to the
   * pendingMessageList 'queue' and signals this worker that there is a new message. The worker
   * thread removes the message and sends it out to a given destination. All messages should be
   * fully initialized The worker thread does check the message nor adds anything new to the
   * message. It just sends it out.
   */
  public void run() {
    String destination = null;
    //  by default, add time to live to each message
    boolean addTimeToLive = true;
    // Check the environment for existence of NoTTL tag. If present,
    // the deployer of the service wants to disable message expiration.
    if (System.getProperty("NoTTL") != null) {
      addTimeToLive = false;
    }

    // Create and initialize the producer.
    try {
      initializeProducer();
      destination = getDestinationEndpoint();
      if (destination == null) {
        throw new InvalidDestinationException("Unable to determine the destination");
      }
    } catch (Exception e) {
      workerThreadFailed = true;
      exception = e;
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                "run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_exception__WARNING", e);
      }
      return;

    } finally {
      engine.producerSemaphore.release();
    }

    engine.onProducerInitialized();


    // Wait for messages from application threads. The uima ee client engine
    // will call doStop() which sets the global flag 'done' to true.
    PendingMessage pm = null;
    ClientRequest cacheEntry = null;
    boolean doCallback = false;
    while (!done) {
      // Remove the oldest message from the shared 'queue'
      // // Wait for a new message
      try {
        pm = messageQueue.take();
      } catch (InterruptedException e) {
      }
      if (done) {
        break; // done in this loop
      }
      //  Check if the request should be rejected. If the connection to the broker is invalid and the request
      //  is not GetMeta Ping, reject the request after the connection is made. The reject() method created
      //  an exception and notified client of the fact that the request could not continue. The ping is a 
      //  special case that we dont reject even though the broker connection has been lost. It is allow to
      //  fall through and will be sent as soon as the connection is recovered.
      boolean rejectRequest = reject(pm);
      if ( !engine.running) {
    	  break;
      }
      //  blocks until the connection is re-established with a broker
      engine.recoverSharedConnectionIfClosed();
      //  get the producer initialized from a valid connection
      producer = getMessageProducer();
      //  Check if the request should be rejected. It would be the case if the connection was invalid and
      //  subsequently recovered. If it was invalid, we went through error handling and the request is stale.
      if ( !rejectRequest && engine.running) {
          if ( engine.serviceDelegate.isAwaitingPingReply() &&
                pm.getMessageType() == AsynchAEMessage.GetMeta ){
            if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, getClass().getName(),
                      "run", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_client_dispatching_getmeta_ping__INFO", new Object[] { });
            }
           }
           try {
             // Request JMS Message from the concrete implementation
             Message message = null;
             // Determine if this a CAS Process Request
             boolean casProcessRequest = isProcessRequest(pm);
             // Only Process request can be serialized as binary
             if (casProcessRequest && (engine.getSerialFormat() != SerialFormat.XMI)) {
               message = createBytesMessage();
             } else {
               message = createTextMessage();
             }

             initializeMessage(pm, message);
             if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
               UIMAFramework.getLogger(CLASS_NAME).logrb(
                       Level.FINE,
                       CLASS_NAME.getName(),
                       "run",
                       JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                       "UIMAJMS_sending_msg_to_endpoint__FINE",
                       new Object[] {
                           UimaMessageValidator.decodeIntToString(AsynchAEMessage.Command, message
                                   .getIntProperty(AsynchAEMessage.Command)),
                           UimaMessageValidator.decodeIntToString(AsynchAEMessage.MessageType, message
                                   .getIntProperty(AsynchAEMessage.MessageType)), destination });
             }
             if (casProcessRequest) {
               cacheEntry = (ClientRequest) engine.getCache().get(
                       pm.get(AsynchAEMessage.CasReference));
               if (cacheEntry != null) {
                   CAS cas = cacheEntry.getCAS();

            	   // Use Process Timeout value for the time-to-live property in the
                 // outgoing JMS message. When this time is exceeded
                 // while the message sits in a queue, the JMS Server will remove it from
                 // the queue. What happens with the expired message depends on the
                 // configuration. Most JMS Providers create a special dead-letter queue
                 // where all expired messages are placed. NOTE: In ActiveMQ expired msgs in the DLQ
                 // are not auto evicted yet and accumulate taking up memory.
                 long timeoutValue = cacheEntry.getProcessTimeout();

                 if (timeoutValue > 0 && addTimeToLive ) {
                   // Set high time to live value
                   message.setJMSExpiration(10 * timeoutValue);
                 }
                 if (pm.getMessageType() == AsynchAEMessage.Process) {
                   cacheEntry.setCASDepartureTime(System.nanoTime());
                 }
                 cacheEntry.setCASDepartureTime(System.nanoTime());
         
                 doCallback = true;
                 
             } else {
                 if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                     UIMAFramework.getLogger(CLASS_NAME).logrb(
                             Level.WARNING,
                             CLASS_NAME.getName(),
                             "run",
                             JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                             "UIMAJMS_failed_cache_lookup__WARNING",
                             new Object[] {
                            	 pm.get(AsynchAEMessage.CasReference),
                                 UimaMessageValidator.decodeIntToString(AsynchAEMessage.Command, message
                                         .getIntProperty(AsynchAEMessage.Command)),
                                 UimaMessageValidator.decodeIntToString(AsynchAEMessage.MessageType, message
                                         .getIntProperty(AsynchAEMessage.MessageType)), destination });
                   }
                }
            	 
             }
             // start timers
             if( casProcessRequest ) { 
            	 CAS cas = cacheEntry.getCAS();
               // Add the cas to a list of CASes pending reply. Also start the timer if necessary
               engine.serviceDelegate.addCasToOutstandingList(cacheEntry.getCasReferenceId(), cas.hashCode(), engine.timerPerCAS); // true=timer per cas
               if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
            	   UIMAFramework.getLogger(CLASS_NAME).logrb(Level.FINE, CLASS_NAME.getName(),
                        "sendCAS", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_cas_added_to_pending_FINE", new Object[] { cacheEntry.getCasReferenceId(), String.valueOf(cas.hashCode()), engine.serviceDelegate.toString()});
               }

             
             } else if ( pm.getMessageType() == AsynchAEMessage.GetMeta &&
                     engine.serviceDelegate.getGetMetaTimeout() > 0 ) {
               // timer for PING has been started in sendCAS()
               if ( !engine.serviceDelegate.isAwaitingPingReply()) {
                 engine.serviceDelegate.startGetMetaRequestTimer();
               } 
             } 
             //  Dispatch asynchronous request to Uima AS service
             producer.send(message);
             if ( doCallback ) {
               UimaASProcessStatus status = new UimaASProcessStatusImpl(new ProcessTrace_impl(),cacheEntry.getCAS(),
                       cacheEntry.getCasReferenceId());
               // Notify engine before sending a message
               if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
                   UIMAFramework.getLogger(CLASS_NAME).logrb(
                           Level.FINE,
                           CLASS_NAME.getName(),
                           "run",
                           JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                           "UIMAJMS_calling_onBeforeMessageSend__FINE",
                           new Object[] {
                             pm.get(AsynchAEMessage.CasReference),
                             String.valueOf(cacheEntry.getCAS().hashCode())
                           });
                 }  
               // Note the callback is a misnomer. The callback is made *after* the send now
               // Application receiving this callback can consider the CAS as delivere to a queue
               engine.onBeforeMessageSend(status);
             
             
             }
           } catch (Exception e) {
             if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
               UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
                       "run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                       "UIMAEE_exception__WARNING", e);
             }
             reject(pm,e);
           }

      }
    }
    try {
      cleanup();
    } catch (Exception e) {
      handleException(e, destination);
    }
  }

  private void initializeMessage(PendingMessage aPm, Message anOutgoingMessage) throws Exception {
    // Populate message properties based on outgoing message type
    switch (aPm.getMessageType()) {
      case AsynchAEMessage.GetMeta:
        engine.setMetaRequestMessage(anOutgoingMessage);
        break;

      case AsynchAEMessage.Process:
        String casReferenceId = (String) aPm.get(AsynchAEMessage.CasReference);
        if (engine.getSerialFormat() == SerialFormat.XMI) {
          String serializedCAS = (String) aPm.get(AsynchAEMessage.CAS);
          engine.setCASMessage(casReferenceId, serializedCAS, anOutgoingMessage);
        } else {
          byte[] serializedCAS = (byte[]) aPm.get(AsynchAEMessage.CAS);
          engine.setCASMessage(casReferenceId, serializedCAS, anOutgoingMessage);

        }
        // Message Expiration for Process is added in the main run() loop
        // right before the message is dispatched to the AS Service
        break;

      case AsynchAEMessage.CollectionProcessComplete:
        engine.setCPCMessage(anOutgoingMessage);
        break;
    }
  }

  private boolean isProcessRequest(PendingMessage pm) {
    return pm.getMessageType() == AsynchAEMessage.Process;
  }

  private void handleException(Exception e, String aDestination) {
    workerThreadFailed = true;
    exception = e;
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, getClass().getName(),
              "handleException", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_exception__WARNING", e);
    }
    engine.recoverSharedConnectionIfClosed();
    // Notify the engine that there was an exception.
    engine.onException(e, aDestination);

  }

  /**
   * @override
   */
  public MessageProducer getMessageProducer(Destination destination) throws Exception {
    return null;
  }

}
