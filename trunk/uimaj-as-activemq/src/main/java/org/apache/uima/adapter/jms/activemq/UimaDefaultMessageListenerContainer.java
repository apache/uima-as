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

import java.lang.reflect.Method;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.InputChannel;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaAsThreadFactory;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.BaseAnalysisEngineController.ServiceState;
import org.apache.uima.aae.controller.Endpoint;
import org.apache.uima.aae.controller.PrimitiveAnalysisEngineController;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.error.ErrorHandler;
import org.apache.uima.aae.error.Threshold;
import org.apache.uima.aae.error.handler.GetMetaErrorHandler;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jms.JmsException;
import org.springframework.jms.listener.AbstractJmsListeningContainer;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.support.JmsUtils;
import org.springframework.jms.support.destination.DestinationResolver;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class UimaDefaultMessageListenerContainer extends DefaultMessageListenerContainer implements
        ExceptionListener {
  private static final Class CLASS_NAME = UimaDefaultMessageListenerContainer.class;

  private String destinationName = "";

  private Endpoint endpoint;

  private volatile boolean freeCasQueueListener;

  private AnalysisEngineController controller;

  private volatile boolean failed = false;

  private Object mux = new Object();

  private final UimaDefaultMessageListenerContainer __listenerRef;

  private TaskExecutor taskExecutor = null;

  private ConnectionFactory connectionFactory = null;

  private Object mux2 = new Object();

  private ThreadGroup threadGroup = null;

  private ThreadFactory tf = null;

  // stores number of consumer threads
  private int cc = 0;

  // stores message listener plugged in by Spring
  private Object ml = null;

  // A new listener will be injected between
  // spring and JmsInputChannel Pojo Listener. This
  // listener purpose is to increment number of children for
  // an input CAS.
  private ConcurrentMessageListener concurrentListener = null;

  private volatile boolean awaitingShutdown = false;
  
  //  When set to true, this flag prevents spring from using refreshUntilSuccessful
  //  logic which attempts to recover the connection. This flag is set to true during the
  //  service shutdown
  public static volatile boolean terminating;

  private ThreadPoolExecutor threadPoolExecutor = null;
  
  private boolean pluginThreadPool;
  
  private CountDownLatch latchToCountNumberOfTerminatedThreads;
  
  public UimaDefaultMessageListenerContainer() {
    super();
    // reset global static. This only effects unit testing as services are deployed 
    // in the same process.
    terminating = false;
    UIMAFramework.getLogger(CLASS_NAME).setLevel(Level.WARNING);
    __listenerRef = this;
    setRecoveryInterval(30000);  // increase connection recovery to 30 sec
    setAcceptMessagesWhileStopping(true);
    setExceptionListener(this);
    threadGroup = new ThreadGroup("ListenerThreadGroup_"
            + Thread.currentThread().getThreadGroup().getName());
  }

  public UimaDefaultMessageListenerContainer(boolean freeCasQueueListener) {
    this();
    this.freeCasQueueListener = freeCasQueueListener;
  }
  /**
   * Overriden Spring's method that tries to recover from lost connection. We dont 
   * want to recover when the service is stopping.
   */
  protected void refreshConnectionUntilSuccessful() {
    boolean doLogFailureMsg = true;
    try {
    	// Only one listener thread should enter to recover lost connection.
    	// Seems like spring recovery api is not reentrant. If multiple listeners
    	// are allowed to attempt recovery, some of them are closed. This is based
    	// on observing jconsole attached to uima-as service with multiple listeners
    	// on an endpoint.  
    	synchronized(UimaDefaultMessageListenerContainer.class ) {
            ActiveMQConnection con = (ActiveMQConnection)super.getSharedConnection();
        	if ( con != null && con.isStarted() && !con.isTransportFailed() ) {
        		return;
        	}
    	    while (isRunning() && !terminating ) {
    		      try {
    		        if (sharedConnectionEnabled()) {
    		          refreshSharedConnection();
    		        }
    		        else {
    		          Connection tcon = createConnection();
    		          JmsUtils.closeConnection(tcon);
    		        }
    		        logger.info("Successfully refreshed JMS Connection");
    		        break;
    		      }
    		      catch (Exception ex) {
    		        if ( doLogFailureMsg ) {
    		          StringBuilder msg = new StringBuilder();
    		          msg.append("Could not refresh JMS Connection for destination '");
    		          msg.append(getDestinationDescription()).append("' - silently retrying in ");
    		          msg.append(5).append(" ms. Cause: ");
    		          msg.append(ex instanceof JMSException ? JmsUtils.buildExceptionMessage((JMSException) ex) : ex.getMessage());
    		          logger.warn(msg);
    		          doLogFailureMsg = false;
    		        }
    		      }
    		      sleepInbetweenRecoveryAttempts();
    		    }	    
    	}
    } catch( IllegalStateException e ) {
    }
  }
  protected void recoverAfterListenerSetupFailure() {
	  if ( !terminating ) {
		  super.recoverAfterListenerSetupFailure();
	  }
  }

  public void setTerminating() {
    terminating = true;
  }
  public void setController(AnalysisEngineController aController) {
    controller = aController;
  }

  /**
   * 
   * @param t - Throwable object to use when determining whether or not to disable 
   *            a UIMA-AS listener
   * @return - true if the listener should be disabled, false otherwise
   */
  private boolean disableListener(Throwable t) {
    if (t.toString().indexOf("SharedConnectionNotInitializedException") > 0
            || (t instanceof JMSException && t.getCause() != null && t.getCause() instanceof ConnectException))
      return true;
    return false;
  }

  /**
   * Stops this Listener
   */
  private void handleListenerFailure() {
    // If shutdown already, nothing to do
    if (awaitingShutdown) {
      return;
    }
    try {
      if (controller instanceof AggregateAnalysisEngineController) {
        String delegateKey = ((AggregateAnalysisEngineController) controller)
                .lookUpDelegateKey(endpoint.getEndpoint());
        InputChannel iC = null;
        String queueName = null;
        if (endpoint.getDestination() != null) {
          queueName = endpoint.getDestination().toString();
        } else {
          queueName = endpoint.getEndpoint();
        }
        iC = ((AggregateAnalysisEngineController) controller).getInputChannel(queueName);
        if (iC != null) {
          iC.destroyListener(queueName, delegateKey);
        } else {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
               UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                        "handleTempQueueFailure", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_unable_to_lookup_input_channel__INFO", queueName);
          }
        }
      }
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( controller != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "handleListenerFailure", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", controller.getComponentName());
        }
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "handleListenerFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    }
  }

  /**
   * Handles failure on a temp queue
   * 
   * @param t
   */
  private void handleTempQueueFailure(Throwable t) {
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      if ( controller != null ) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "handleTempQueueFailure", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", controller.getComponentName());
      }

      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
              "handleTempQueueFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_jms_listener_failed_WARNING",
              new Object[] { getDestination(), getBrokerUrl(), t });
    }
    // Check if the failure is due to the failed connection. Spring (and ActiveMQ) dont seem to
    // provide
    // the cause. Just the top level IllegalStateException with a text message. This is what we need
    // to
    // check for.
    ActiveMQConnection conn = null;
    try {
      conn = (ActiveMQConnection)getSharedConnection();
    } catch( Exception exx ) { // shared connection  may not exist yet if a broker is not up
    }
    if ( (conn != null && conn.isTransportFailed() ) || 
            t instanceof javax.jms.IllegalStateException
            && t.getMessage().equals("The Consumer is closed")) {
      if (controller != null && controller instanceof AggregateAnalysisEngineController) {
				//	If endpoint not set, this is a temp reply queue listener.
        if ( endpoint == null ) {
          destroy();
          return;
        }
  
      String delegateKey = ((AggregateAnalysisEngineController) controller)
                .lookUpDelegateKey(endpoint.getEndpoint());
        try {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(
                    Level.INFO,
                    this.getClass().getName(),
                    "handleTempQueueFailure",
                    JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_stopping_listener_INFO",
                    new Object[] { controller.getComponentName(), endpoint.getDestination(),
                        delegateKey });
          }
          // Stop current listener
          handleListenerFailure();
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
            if ( controller != null ) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, this.getClass().getName(),
                      "handleTempQueueFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAJMS_stopped_listener_INFO",
                      new Object[] { controller.getComponentName(), endpoint.getDestination() });
            }
          }
        } catch (Exception e) {
          if ( controller != null ) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "handleTempQueueFailure", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_service_exception_WARNING", controller.getComponentName());
          }

          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "handleTempQueueFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_exception__WARNING", e);
          }
        }
      }
    } else if (disableListener(t)) {
      handleQueueFailure(t);
    }
  }

  private ErrorHandler fetchGetMetaErrorHandler() {
    ErrorHandler handler = null;
    Iterator it = controller.getErrorHandlerChain().iterator();
    // Find the error handler for GetMeta in the Error Handler List provided in the
    // deployment descriptor
    while (it.hasNext()) {
      handler = (ErrorHandler) it.next();
      if (handler instanceof GetMetaErrorHandler) {
        return handler;
      }
    }
    return null;
  }

  /**
   * Handles failures on non-temp queues
   * 
   * @param t
   */
  private void handleQueueFailure(Throwable t) {
    final String endpointName = (getDestination() == null) ? ""
            : ((ActiveMQDestination) getDestination()).getPhysicalName();
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
              "handleQueueFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_jms_listener_failed_WARNING",
              new Object[] { endpointName, getBrokerUrl(), t });
    }
    boolean terminate = true;
    // Check if the failure is severe enough to disable this listener. Whether or not this listener
    // is actully
    // disabled depends on the action associated with GetMeta Error Handler. If GetMeta Error
    // Handler is
    // configured to terminate the service on failure, this listener will be terminated and the
    // entire service
    // will be stopped.
    if (disableListener(t)) {
      endpoint.setReplyDestinationFailed();
      // If this is a listener attached to the Aggregate Controller, use GetMeta Error
      // Thresholds defined to determine what to do next after failure. Either terminate
      // the service or disable the delegate with which this listener is associated with
      if (controller != null && controller instanceof AggregateAnalysisEngineController) {
        ErrorHandler handler = fetchGetMetaErrorHandler();
        // Fetch a Map containing thresholds for GetMeta for each delegate.
        Map thresholds = handler.getEndpointThresholdMap();
        // Lookup delegate's key using delegate's endpoint name
        String delegateKey = ((AggregateAnalysisEngineController) controller)
                .lookUpDelegateKey(endpoint.getEndpoint());
        // If the delegate has a threshold defined on GetMeta apply Action defined
        if (delegateKey != null && thresholds.containsKey(delegateKey)) {
          // Fetch the Threshold object containing error configuration
          Threshold threshold = (Threshold) thresholds.get(delegateKey);
          // Check if the delegate needs to be disabled
          if (threshold.getAction().equalsIgnoreCase(ErrorHandler.DISABLE)) {
            // The disable delegate method takes a list of delegates
            List list = new ArrayList();
            // Add the delegate to disable to the list
            list.add(delegateKey);
            try {
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
                UIMAFramework.getLogger(CLASS_NAME)
                        .logrb(
                                Level.INFO,
                                this.getClass().getName(),
                                "handleQueueFailure",
                                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                                "UIMAEE_disabled_delegate_bad_broker__INFO",
                                new Object[] { controller.getComponentName(), delegateKey,
                                    getBrokerUrl() });
              }
              // Remove the delegate from the routing table.
              ((AggregateAnalysisEngineController) controller).disableDelegates(list);
              terminate = false; // just disable the delegate and continue
            } catch (Exception e) {
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                if ( controller != null ) {
                  UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                          "handleQueueFailure", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                          "UIMAEE_service_exception_WARNING", controller.getComponentName());
                }

                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                        "handleQueueFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_exception__WARNING", e);
              }
              terminate = true;
            }
          }
        }
      }
    }
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                "handleQueueFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_closing_channel__WARNING",
                new Object[] { getBrokerUrl(), endpoint.getEndpoint(),  });
      }
    
    
    setRecoveryInterval(0);

    // Spin a shutdown thread to terminate listener.
    new Thread() {
      public void run() {
        try {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                    "handleQueueFailure.run", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_disable_listener__WARNING",
                    new Object[] { endpointName, getBrokerUrl() });
          }
          shutdown();
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            if ( controller != null ) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                      "handleQueueFailure.run", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_service_exception_WARNING", controller.getComponentName());
            }

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "handleQueueFailure.run", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_exception__WARNING", e);
          }
        }
      }
    }.start();

    if (terminate) {
      terminate(t);
    }

  }

  /**
   * This method is called by Spring when a listener fails
   */
  protected void handleListenerSetupFailure(Throwable t, boolean alreadyHandled) {
    // If shutdown already, nothing to do
	    // If controller is stopping no need to recover the connection
    if (awaitingShutdown || terminating || (controller != null && controller.isStopped()) ) {
      return;
    }
    if ( controller != null ) {
      controller.changeState(ServiceState.FAILED);
    }
    //	check if endpoint object has been initialized. If it is not 
    //  initialized, most likely the broker is not available and we
    //	go into a silent re-connect retry. 
    if (endpoint == null ) {
      super.handleListenerSetupFailure(t, true);
      String controllerId = "";
      if (controller != null) {
        controllerId = "Uima AS Service:" + controller.getComponentName();
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                "handleListenerSetupFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_listener_connection_failure__WARNING",
                new Object[] { controllerId, getBrokerUrl() });
      }
 	  // Use Spring to retry connection until successful. This call is
	  // blocking this thread.
      refreshConnectionUntilSuccessful();
      if ( controller != null ) {
        controller.changeState(ServiceState.RUNNING);
      }
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                "handleListenerSetupFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_listener_connection_recovered__WARNING",
                new Object[] { controllerId, getBrokerUrl() });
      }
      return;
    }
    // Connection failure that occurs AFTER the service initialized.
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      if ( controller != null ) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "handleListenerSetupFailure", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", controller.getComponentName());
      }

      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
              "handleListenerSetupFailure", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_exception__WARNING", t);
    }
    synchronized (mux) {
      if (!failed) {
        // Check if this listener is attached to a temp queue. If so, this is a listener
        // on a reply queue. Handle temp queue listener failure differently than an
        // input queue listener.
        if (endpoint.isTempReplyDestination()) {
          handleTempQueueFailure(t);
        } else {
          // Handle non-temp queue failure
          handleQueueFailure(t);
        }
      }
      failed = true;
    }
  }

  private void terminate(Throwable t) {
    // ****************************************
    // terminate the service
    // ****************************************
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
              "terminate", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_terminate_service_dueto_bad_broker__WARNING",
              new Object[] { controller.getComponentName(), getBrokerUrl() });
    }
    controller.notifyListenersWithInitializationStatus(new ResourceInitializationException(t));
    if (!controller.isStopped() && !controller.isAwaitingCacheCallbackNotification()) {
      controller.stop();
    }
  }

  protected void handleListenerException(Throwable t) {
    // Already shutdown, nothing to do
    if (awaitingShutdown) {
      return;
    }
    String endpointName = (getDestination() == null) ? ""
            : ((ActiveMQDestination) getDestination()).getPhysicalName();

    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
              "handleListenerException", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_jms_listener_failed_WARNING",
              new Object[] { endpointName, getBrokerUrl(), t });
    }
    super.handleListenerException(t);
  }

  private void allPropertiesSet() {
    super.afterPropertiesSet();
  }

  private void injectConnectionFactory() {
    while (connectionFactory == null) {
      try {
        Thread.sleep(50);
      } catch (Exception e) {
      }
    }
    super.setConnectionFactory(connectionFactory);
  }

  private void injectTaskExecutor() {
    super.setTaskExecutor(taskExecutor);
  }

  private boolean isGetMetaListener() {
    return getMessageSelector() != null
            && __listenerRef.getMessageSelector().equals("Command=2001");
  }

  private boolean isActiveMQDestination() {
    return getDestination() != null && getDestination() instanceof ActiveMQDestination;
  }

  public void initializeContainer() {
    try {

      injectConnectionFactory();
      super.initialize();
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( controller != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "initializeContainer", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", controller.getComponentName());
        }

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "initializeContainer", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    }
  }

  /**
   * Intercept Spring call to increment number of consumer threads. If the value > 1, don't
   * propagate to Spring. A new listener will be injected and it will use provided number of
   * consumer threads.
   **/
  public void setConcurrentConsumers(int concurrentConsumers) {
    cc = concurrentConsumers;
    if (this.freeCasQueueListener) {
      super.setConcurrentConsumers(concurrentConsumers);
    }
  }

  /**
   * Intercept Spring call to inject application Pojo listener. Don't propagate the listener up to
   * Spring just yet. If more than one consumer thread is used, a different listener will be
   * injected.
   **/
  public void setMessageListener(Object messageListener) {
    ml = messageListener;
    if (this.freeCasQueueListener) {
      super.setMessageListener(messageListener);
    }
  }
  public void afterPropertiesSet() {
    afterPropertiesSet(true);
  }
  /**
   * Called by Spring and some Uima AS components when all properties have been set. This method
   * spins a thread in which the listener is initialized.
   */
  public void afterPropertiesSet(final boolean propagate) {
    if (endpoint != null) {
			// Override the prefetch size. The dd2spring always sets this to 1 which 
			// may effect the throughput of a service. Change the prefetch size to
			// number of consumer threads defined in DD.
      if ( cc > 1 && endpoint.isTempReplyDestination() && connectionFactory instanceof ActiveMQConnectionFactory ) {
        ((ActiveMQConnectionFactory)connectionFactory).getPrefetchPolicy().setQueuePrefetch(cc);
      }
      
      // Endpoint has been plugged in from spring xml. This means this is a listener
      // for a reply queue. We need to rewire things a bit. First make Spring use
      // one thread to make sure we receive messages in order. To fix a race condition
      // where a parent CAS is processed first instead of its last child, we need to
      // assure that we get the child first. We need to update the counter of the
      // parent CAS to reflect that there is another child. In the race condition that
      // was observed, the parent was being processed first in one thread. The parent
      // reached the final step and subsequently was dropped. Subsequent to that, a
      // child CAS processed on another thread begun executing and failed since a look
      // on its parent resulted in CAS Not Found In Cache Exception.
      // Make sure Spring uses one thread
      super.setConcurrentConsumers(1);
      if (cc > 1) {
        try {
          String prefix = endpoint.getDelegateKey()+" Reply Thread";
          concurrentListener = new ConcurrentMessageListener(cc, ml, getDestinationName(), threadGroup,prefix);
          super.setMessageListener(concurrentListener);
        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            if ( controller != null ) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                      "afterPropertiesSet", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                      "UIMAEE_service_exception_WARNING", controller.getComponentName());
            }

            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "afterPropertiesSet", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_exception__WARNING", e);
          }
          return;
        }
      } else {
        pluginThreadPool = true;
      }
    } else {
      super.setConcurrentConsumers(cc);
      pluginThreadPool = true;
    }
    Thread t = new Thread(threadGroup, new Runnable() {
      public void run() {
        Destination destination = __listenerRef.getDestination();
        try {
          // Wait until the connection factory is injected by Spring
          while (connectionFactory == null) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException ex) {
            }
          }
          System.setProperty("BrokerURI", ((ActiveMQConnectionFactory) connectionFactory)
                  .getBrokerURL());
          boolean done = false;
          // Wait for controller to be injected by Uima AS
          if (isActiveMQDestination() && !isGetMetaListener()
                  && !((ActiveMQDestination) destination).isTemporary()) {
            // Add self to InputChannel
            connectWithInputChannel();
            // Wait for InputChannel to plug in a controller
            done = true;
            while (controller == null)
              try {
                Thread.sleep(50);
              } catch (InterruptedException ex) {
              }
            ;
          }
          // Plug in connection Factory to Spring's Listener
          __listenerRef.injectConnectionFactory();
          
          if ( pluginThreadPool ) {
            setUimaASThreadPoolExecutor(cc);
          }
          
          // Initialize the TaskExecutor. This call injects a custom Thread Pool into the
          // TaskExecutor provided in the spring xml. The custom thread pool initializes
          // an instance of AE in a dedicated thread
          if ( getMessageSelector() != null && !isGetMetaListener()) {
            initializeTaskExecutor(cc);
          }
          if ( threadPoolExecutor == null ) {
              // Plug in TaskExecutor to Spring's Listener
              __listenerRef.injectTaskExecutor();
          }
          if ( propagate ) {
            // Notify Spring Listener that all properties are ready
            __listenerRef.allPropertiesSet();
          }
          if (isActiveMQDestination() && destination != null) {
            destinationName = ((ActiveMQDestination) destination).getPhysicalName();
          }
          if (!done) {
            connectWithInputChannel();
            done = true;
          }
          if (concurrentListener != null) {
            concurrentListener.setAnalysisEngineController(controller);
          }
          // Save number of concurrent consumers on the temp reply queue in case we need to
          // recreate a new listener on a new temp queue created during recovery
          if (endpoint != null && controller instanceof AggregateAnalysisEngineController) {
            Delegate delegate = ((AggregateAnalysisEngineController) controller)
                    .lookupDelegate(endpoint.getDelegateKey());
            if (delegate != null) {
              delegate.getEndpoint().setConcurrentReplyConsumers(cc);
            }
          }
          //  Show ready message on the console only if this listener is *not* listening
          //  on an input queue. Input queue listeners are not started until the service
          //  is fully initialized
          if (__listenerRef.getMessageListener() == null && getDestination() != null) {
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, this.getClass().getName(),
                    "afterPropertiesSet", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_listener_ready__INFO",
                    new Object[] {controller.getComponentName(),  getBrokerUrl(), getDestination() });
          } 

        } catch (Exception e) {
         
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                  "afterPropertiesSet", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_jms_listener_failed_WARNING",
                  new Object[] { destination, getBrokerUrl(), e });
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "afterPropertiesSet", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
        }
      }
    });
    t.start();
  }

  /**
   * Inject instance of this listener into the InputChannel
   * 
   * @throws Exception
   */
  private void connectWithInputChannel() throws Exception {
    Object pojoListener = getPojoListener();

    if (pojoListener instanceof JmsInputChannel) {
      // Wait until InputChannel has a valid controller. The controller will be plug in
      // by Spring on a different thread
      while ((((JmsInputChannel) pojoListener).getController()) == null) {
        try {
          Thread.currentThread().sleep(50);
        } catch (Exception e) {
        }
      }
      ((JmsInputChannel) pojoListener).setListenerContainer(__listenerRef);
    } else if (pojoListener instanceof ModifiableListener) {
      ((ModifiableListener) pojoListener).setListener(__listenerRef);
    }
  }

  public String getDestinationName() {

    return destinationName;
  }

  public String getEndpointName() {
    if (getDestination() != null) {
      return ((ActiveMQDestination) getDestination()).getPhysicalName();
    }
    return null;
  }

  public String getBrokerUrl() {
    return ((ActiveMQConnectionFactory) connectionFactory).getBrokerURL();
  }

  /*
   * Overrides specified Connection Factory. Need to append maxInactivityDuration=0 to the broker
   * URL. The Connection Factory is immutable thus we need to intercept the one provided in the
   * deployment descriptor and create a new one with rewritten Broker URL. We will inject the
   * prefetch policy to the new CF based on what is found in the CF in the deployment descriptor.
   */

  public void setConnectionFactory(ConnectionFactory aConnectionFactory) {
    connectionFactory = aConnectionFactory;
    super.setConnectionFactory(connectionFactory);
  }

  public void setDestinationResolver(DestinationResolver resolver) {
    ((TempDestinationResolver) resolver).setListener(this);
    super.setDestinationResolver(resolver);
  }
  /**
   * Closes shares connection to a broker
  **/
  public void closeConnection() throws Exception {
    try {
      setRecoveryInterval(0);
      setAutoStartup(false);
      if ( getSharedConnection() != null ) {
        ActiveMQConnection amqc = (ActiveMQConnection)getSharedConnection();
        if (amqc != null && amqc.isStarted()
                && !amqc.isClosed()
                && !amqc.isClosing()
                && !amqc.isTransportFailed()) {
          getSharedConnection().close();
        }
      }
    } catch( AbstractJmsListeningContainer.SharedConnectionNotInitializedException e) {
      //  Ignore this. This is thrown from Spring's getSharedConnection() 
    } catch (Exception e) {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
        if ( controller != null ) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                  "closeConnection", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAEE_service_exception_WARNING", controller.getComponentName());
        }

        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                "closeConnection", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    }
  }

  public void setDestination(Destination aDestination) {
    super.setDestination(aDestination);
    if (endpoint != null) {
      endpoint.setDestination(aDestination);
      //  Get the prefetch size. If > 1, it has been previously overriden. The override is done in
      // the code since dd2spring alwys sets the prefetch on a reply queue to 1. This may slow down
      // a throughput of a service.
      int prefetchSize = ((ActiveMQConnectionFactory)connectionFactory).getPrefetchPolicy().getQueuePrefetch();
      if (aDestination instanceof TemporaryQueue ) {
        //	Only log if prefetch on temp queue has been earlier overriden. The dd2spring
        //  always sets prefetch on a temp queue to 1. The fact that the prefetch > 1 means
        //	that an override must have taken place. Just log the value of a prefetch.
        if ( prefetchSize > 1 && UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
           UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                    "setDestination", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_replyq_prefetch_override__INFO", new Object[] {aDestination,prefetchSize
           });
        }
        endpoint.setTempReplyDestination(true);
        Object pojoListener = getPojoListener();
        if (pojoListener != null && pojoListener instanceof InputChannel) {
          ((JmsInputChannel) pojoListener).setListenerContainer(this);
        }
      }
      endpoint.setServerURI(getBrokerUrl());
    }
  }

  private Object getPojoListener() {
    Object pojoListener = null;
    if (ml != null) {
      pojoListener = ml;
    } else if (getMessageListener() != null) {
      pojoListener = getMessageListener();
    }
    return pojoListener;
  }

  public Destination getListenerEndpoint() {
    return getDestination();
  }

  public void onException(JMSException arg0) {
    if (awaitingShutdown) {
      return;
    }
    String endpointName = (getDestination() == null) ? ""
            : ((ActiveMQDestination) getDestination()).getPhysicalName();

    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
      if ( controller != null ) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "onException", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAEE_service_exception_WARNING", controller.getComponentName());
      }

      UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
              "onException", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAJMS_jms_listener_failed_WARNING",
              new Object[] { endpointName, getBrokerUrl(), arg0 });
    }

    if ( getDestination() != null && ((ActiveMQDestination)getDestination()).isTemporary() ) {
      handleTempQueueFailure(arg0);
    }
  
  }

  public void setTargetEndpoint(Endpoint anEndpoint) {
    endpoint = anEndpoint;
  }

  public boolean isFreeCasQueueListener() {
    return freeCasQueueListener;
  }

  protected void setModifiedTaskExecutor(TaskExecutor taskExecutor) {
    super.setTaskExecutor(taskExecutor);
  }

  /**
   * Delegate shutdown to the super class
   */
  public void doDestroy() {
    super.destroy();
  }
  public void setMessageSelector( String messageSelector) {
    super.setMessageSelector(messageSelector);
    //  turn off auto startup. Selectors are only used on input queues. We dont
    //  want listeners on this queue to start now. Once the service initializes 
    //  we will start listeners on input queue.
    this.setAutoStartup(false);
  }
  public void shutdownTaskExecutor(ThreadPoolExecutor tpe, boolean stopImmediate) throws InterruptedException {
    if ( stopImmediate ) {
  	  tpe.purge();
      tpe.shutdownNow();
    } else {
      tpe.shutdown();
    }
  }
  public void destroy() {
	  destroy(true); 
  }
  /**
   * Spins a shutdown thread and stops Sprint and ActiveMQ threads.
   * 
   */
  public void destroy(final boolean stopImmediate) {
	  
    if (awaitingShutdown) {
      return;
    }
    // Spin a thread that will shutdown all taskExecutors and wait for their threads to stop.
    // A separate thread is necessary since we cant stop a threadPoolExecutor if one of its
    // threads is busy stopping the executor. This leads to a hang.
    Thread threadGroupDestroyer = new Thread(threadGroup.getParent().getParent(),
            "threadGroupDestroyer") {
      public void run() {
        try {
          if ( !__listenerRef.awaitingShutdown ) {
        	    awaitingShutdown = true;
              if (taskExecutor != null && taskExecutor instanceof ThreadPoolTaskExecutor) {
              	//	Modify task executor to terminate idle threads. While the thread terminates
              	//  it calls destroy() method on the pinned instance of AE
                
                //  java 5 ThreadPoolExecutor doesnt implement allowCoreThreadTimeout Method. 
                //  Use alternate mechanism to passivate threads in the pool.
                try {
                  Method m = ((ThreadPoolTaskExecutor) taskExecutor).
                    getThreadPoolExecutor().getClass().getMethod("allowCoreThreadTimeOut", boolean.class);
                  m.invoke(((ThreadPoolTaskExecutor) taskExecutor).getThreadPoolExecutor(), true);
                } catch ( NoSuchMethodException e) {
                  ((ThreadPoolTaskExecutor) taskExecutor).getThreadPoolExecutor().setCorePoolSize(0);
                }
               // ((ThreadPoolTaskExecutor) taskExecutor).getThreadPoolExecutor().allowCoreThreadTimeOut(true);
                ((ThreadPoolTaskExecutor) taskExecutor).getThreadPoolExecutor().setKeepAliveTime(1000, TimeUnit.MILLISECONDS);
              	((ThreadPoolTaskExecutor) taskExecutor).setWaitForTasksToCompleteOnShutdown(true);
              	((ThreadPoolTaskExecutor) taskExecutor).shutdown();
              } else if (concurrentListener != null) {
                  shutdownTaskExecutor(concurrentListener.getTaskExecutor(), stopImmediate);
                  concurrentListener.stop();
              } else if ( threadPoolExecutor != null ) {
            	  shutdownTaskExecutor(threadPoolExecutor, true);
              }
        	}
          // Close Connection to the broker
          String controllerName = (__listenerRef.controller == null) ? "" :__listenerRef.controller.getComponentName();
          __listenerRef.getSharedConnection().close();
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                       "destroy.run()", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                       "UIMAJMS_listener_shutdown__INFO", new Object[] {controllerName,__listenerRef.getMessageSelector(),__listenerRef.getBrokerUrl()});
         }
         // __listenerRef.shutdown();
         if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
              UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                       "destroy.run()", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                       "UIMAJMS_listener_jms_connection_closed__INFO", new Object[] {controllerName,__listenerRef.getMessageSelector()});
         }
        } catch (Exception e) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                  "destroy", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", e);
        }

        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
          threadGroup.getParent().list();
        }
        try {
          synchronized (threadGroup) {
            if (!threadGroup.isDestroyed()) {
              threadGroup.destroy();
            }
          }
        } catch (Exception e) {
        } // Ignore
      }
    };
    threadGroupDestroyer.start();
	  //	Wait for process threads to finish. Each thread
	  // will count down the latch on exit. When all thread
	  // finish we can continue. Otherwise we block on the latch
    try {
      if ( latchToCountNumberOfTerminatedThreads != null && cc > 1) {
        latchToCountNumberOfTerminatedThreads.await();
      }
    } catch( Exception ex) {
       UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, this.getClass().getName(),
                  "destroy", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_exception__WARNING", ex);
    }
    
  }
  
  private void setUimaASThreadPoolExecutor(int consumentCount) throws Exception{
    super.setMessageListener(ml);
    // create task executor with custom thread pool for:
    // 1) GetMeta request processing
    // 2) ReleaseCAS request
    if ( taskExecutor == null ) {
      UimaAsThreadFactory tf = new UimaAsThreadFactory(threadGroup);
      tf.setDaemon(false);
      if ( isFreeCasQueueListener()) {
        tf.setThreadNamePrefix(controller.getComponentName()+" - FreeCASRequest Thread");
      } else if ( isGetMetaListener()  ) {
        tf.setThreadNamePrefix(super.getBeanName()+" - Thread");
      } else if ( getDestination() != null && getMessageSelector() != null ) {
        tf.setThreadNamePrefix(controller.getComponentName() + " Process Thread");
      } else if ( endpoint != null && endpoint.isTempReplyDestination() ) {
        tf.setThreadNamePrefix(super.getBeanName()+" - Thread");
      } else { 
        throw new Exception("Unknown Context Detected in setUimaASThreadPoolExecutor()");
      }
      ExecutorService es = Executors.newFixedThreadPool(consumentCount,tf);
      if ( es instanceof ThreadPoolExecutor ) {
          threadPoolExecutor = (ThreadPoolExecutor)es;
          super.setTaskExecutor(es);
      }
    } else {
        UimaAsThreadFactory tf = new UimaAsThreadFactory(threadGroup);
        tf.setDaemon(false);
        if ( isFreeCasQueueListener()) {
          tf.setThreadNamePrefix(controller.getComponentName()+" - FreeCASRequest Thread");
        } else if ( isGetMetaListener()  ) {
          tf.setThreadNamePrefix(super.getBeanName()+" - Thread");
        } else if ( getDestination() != null && getMessageSelector() != null ) {
          tf.setThreadNamePrefix(controller.getComponentName() + " Process Thread");
        } else if ( endpoint != null && endpoint.isTempReplyDestination() ) {
          tf.setThreadNamePrefix(super.getBeanName()+" - Thread");
        } else { 
          throw new Exception("Unknown Context Detected in setUimaASThreadPoolExecutor()");
        }
        
    }
  }

  
  /**
   * Called by Spring to inject TaskExecutor
   */
  public void setTaskExecutor(TaskExecutor aTaskExecutor) {
    taskExecutor = aTaskExecutor;
  }

  public TaskExecutor getTaskExecutor() {
	return taskExecutor;
  }
  
  /**
   * This method initializes ThreadPoolExecutor with a custom ThreadPool. Each thread produced by
   * the ThreadPool is used to first initialize an instance of the AE before the thread is added to
   * the pool. From this point on, a thread used to initialize the AE will also be used to call this
   * AE's process() method.
   * 
   * @throws Exception
   */
  private void initializeTaskExecutor(int consumers) throws Exception {
    // TaskExecutor is only used with primitives
    if (controller instanceof PrimitiveAnalysisEngineController) {
      // in case the taskExecutor is not plugged in yet, wait until one
      // becomes available. The TaskExecutor is plugged in by Spring
      synchronized (mux2) {
        while (taskExecutor == null) {
          mux2.wait(20);
        }
      }
      latchToCountNumberOfTerminatedThreads = new CountDownLatch(consumers);
      // Create a Custom Thread Factory. Provide it with an instance of
      // PrimitiveController so that every thread can call it to initialize
      // the next available instance of a AE.
      tf = new UimaAsThreadFactory(threadGroup, (PrimitiveAnalysisEngineController) controller, latchToCountNumberOfTerminatedThreads);
      ((UimaAsThreadFactory)tf).setDaemon(true);
      // This ThreadExecutor will use custom thread factory instead of defult one
      ((ThreadPoolTaskExecutor) taskExecutor).setThreadFactory(tf);
      // Initialize the thread pool
      ((ThreadPoolTaskExecutor) taskExecutor).initialize();
      // Make sure all threads are started. This forces each thread to call
      // PrimitiveController to initialize the next instance of AE
      ((ThreadPoolTaskExecutor) taskExecutor).getThreadPoolExecutor().prestartAllCoreThreads();
      //  Change the state of a collocated service
      if ( !controller.isTopLevelComponent() ) {
        controller.changeState(ServiceState.RUNNING);
      }
    }
    
    if ( threadPoolExecutor != null ) {
    	threadPoolExecutor.prestartAllCoreThreads();
    }
  }
  public void delegateStop() {
    super.stop();
  }
  public void stop() throws JmsException {
    destroy();
  }
}
