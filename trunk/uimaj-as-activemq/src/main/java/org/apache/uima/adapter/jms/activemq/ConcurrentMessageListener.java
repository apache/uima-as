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

import java.io.InvalidClassException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.UimaAsThreadFactory;
import org.apache.uima.aae.UimaBlockingExecutor;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController;
import org.apache.uima.aae.controller.AggregateAnalysisEngineController_impl;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.LocalCache.CasStateEntry;
import org.apache.uima.aae.delegate.Delegate;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.adapter.jms.JmsConstants;
import org.apache.uima.util.Level;
import org.springframework.jms.listener.SessionAwareMessageListener;

/**
 * Message listener injected at runtime into Aggregate to handle a race condition when multiple
 * threads simultaneously process messages from a Cas Multiplier. It is only used to process
 * messages from a Cas Multiplier and only if the reply queue has more than one consumer thread
 * configured in a deployment descriptor. The listener creates a pool of threads equal to the number
 * of concurrent consumers defined in the DD for the listener on the reply queue. Once the message
 * is handled in onMessage(), it is than delegated for processing to one of the available threads
 * from the pool.
 * 
 * This listener guarantees processing order. It receives messages from Spring in a single thread
 * and if it finds a child CAS in the message, it increments the parent (input) CAS child count and
 * delegates processing to the InputChannel instance.
 * 
 * The race condition: The Cas Multiplier sends the last child and the parent almost at the same
 * time. Both are received by the aggregate and are processed in different threads, if a scaleout is
 * used on the reply queue. One thread may start processing the input CAS while the other thread
 * (with the last child) is not yet allowed to run. The first thread takes the input CAS all the way
 * to the final step and since at this time, the input CAS has no children ( the thread processing
 * the last child has not updated the child count yet), it will be prematurely released. When the
 * thread with the last child is allowed to run, it finds that the parent no longer exists in the
 * cache.
 * 
 * 
 */
public class ConcurrentMessageListener implements SessionAwareMessageListener {
  private static final Class CLASS_NAME = ConcurrentMessageListener.class;

  private SessionAwareMessageListener delegateListener;

  private int concurrentThreadCount = 0;

  private AnalysisEngineController controller;

  private ThreadPoolExecutor executor = null;

  private UimaBlockingExecutor blockingExecutor;
  
  private LinkedBlockingQueue<Runnable> workQueue;

  private CountDownLatch controllerLatch = new CountDownLatch(1);

  /**
   * Creates a listener with a given number of process threads. This listener is injected between
   * Spring and JmsInputChannel to enable orderly processing of CASes. This listener is only used on
   * reply queues that have scale out attribute in DD greater than 1. Its main job is to increment
   * number of child CASes for a given input CAS. It does so in a single thread, and once it
   * completes the update this listener submits the CAS for further processing up to the
   * JmsInputChannel. The CAS is submitted to a queue where the executor assigns a free thread to
   * process the CAS.
   * 
   * @param concurrentThreads
   *          - number of threads to use to process CASes
   * @param delegateListener
   *          - JmsInputChannel instance to delegate CAS to
   * @throws InvalidClassException
   */
  public ConcurrentMessageListener(int concurrentThreads, Object delegateListener, String destination, ThreadGroup threadGroup, String threadPrefix)
          throws InvalidClassException {
    if (!(delegateListener instanceof SessionAwareMessageListener)) {
      throw new InvalidClassException("Invalid Delegate Listener. Expected Object of Type:"
              + SessionAwareMessageListener.class + " Received:" + delegateListener.getClass());
    }
    concurrentThreadCount = concurrentThreads;
    this.delegateListener = (SessionAwareMessageListener) delegateListener;
    if (concurrentThreads > 1) {
      //  created an unbounded queue. The throttling is controlled by the
      //  semaphore in the UimaBlockingExecutor initialized below
      workQueue = new LinkedBlockingQueue<Runnable>();
      executor = new ThreadPoolExecutor(concurrentThreads, concurrentThreads, Long.MAX_VALUE,
              TimeUnit.NANOSECONDS, workQueue);
      UimaAsThreadFactory tf = new UimaAsThreadFactory(threadGroup);
      tf.setDaemon(true);
      tf.setThreadNamePrefix(threadPrefix);
      executor.setThreadFactory(tf);
      
      executor.prestartAllCoreThreads();
      if ( destination != null ) {
        blockingExecutor = new UimaBlockingExecutor(executor, concurrentThreads, destination);
      } else {
        blockingExecutor = new UimaBlockingExecutor(executor, concurrentThreads);
      }
    }
  }
  public ThreadPoolExecutor getTaskExecutor() {
    return executor;
  }
  public void stop() {
    blockingExecutor.stop();
  }

  public void setAnalysisEngineController(AnalysisEngineController controller) {
    this.controller = controller;
    controllerLatch.countDown();
  }

  private boolean isMessageFromCasMultiplier(final Message message) throws JMSException {
    return message.propertyExists(AsynchAEMessage.CasSequence);
  }

  /**
   * Intercept a message to increment a child count of the input CAS. This method is always called
   * in a single thread, guaranteeing order of processing. The child CAS will always come here
   * first. Once the count is updated, or this CAS is not an child, the message will be delegated to
   * one of the threads in the pool that will eventually call InputChannel object where the actual
   * processing of the message begins.
   * 
   */
  public void onMessage(final Message message, final Session session) throws JMSException {
    try {
      // Wait until the controller is plugged in
      controllerLatch.await();
    } catch (InterruptedException e) {
    }
    if (isMessageFromCasMultiplier(message)) {
      // Check if the message came from a Cas Multiplier and it contains a new Process Request
      int command = message.getIntProperty(AsynchAEMessage.Command);
      int messageType = message.getIntProperty(AsynchAEMessage.MessageType);
      // Intercept Cas Process Request from a Cas Multiplier
      if (command == AsynchAEMessage.Process && messageType == AsynchAEMessage.Request) {
        String msgFrom = (String) message.getStringProperty(AsynchAEMessage.MessageFrom);
        if (msgFrom != null && controller instanceof AggregateAnalysisEngineController) {
          String delegateKey = ((AggregateAnalysisEngineController) controller)
                  .lookUpDelegateKey(msgFrom);
          if (delegateKey != null) {
            Delegate delegate = ((AggregateAnalysisEngineController) controller)
                    .lookupDelegate(delegateKey);
            delegate.setConcurrentConsumersOnReplyQueue();
          }
        }
        try {
          String parentCasReferenceId = message
                  .getStringProperty(AsynchAEMessage.InputCasReference);
          // Fetch parent CAS entry from the local cache
          CasStateEntry parentEntry = controller.getLocalCache().lookupEntry(parentCasReferenceId);
          // increment number of child CASes this parent has in play
          parentEntry.incrementSubordinateCasInPlayCount();
          //  increment a counter that counts number of child CASes that have no
          //  flow object yet. The flow object is created for each child CAS from
          //  the parent flow object. The method below will actually acquire a 
          //  permit from a binary semaphore to force the parent to block until
          //  the last of its children acquires its Flow object.
          parentEntry.incrementOutstandingFlowCounter();

        } catch (Exception e) {
          if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
            
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "onMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAEE_service_exception_WARNING", controller.getComponentName());
            
            UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                    "onMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                    "UIMAJMS_exception__WARNING", e);
          }
        }
      }

    }
    if (concurrentThreadCount > 1) {
      // Delegate meesage to the JmsInputChannel
      try {
        blockingExecutor.submitTask(new Runnable() {
          public void run() {
            try {
              delegateListener.onMessage(message, session);
            } catch (Exception e) {
              if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.WARNING)) {
                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                        "onMessage", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAEE_service_exception_WARNING", controller.getComponentName());

                UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                        "onMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                        "UIMAJMS_exception__WARNING", e);
              }
            }
          }
        });
      } catch( InterruptedException e) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.WARNING, CLASS_NAME.getName(),
                "onMessage", JmsConstants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_exception__WARNING", e);
      }
    } else {
      // Just handoff the message to the InputChannel object
      delegateListener.onMessage(message, session);
    }
  }
}
