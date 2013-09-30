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

package org.apache.uima.aae;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.uima.UIMAFramework;
import org.apache.uima.util.Level;

/**
 * A wrapper around ThreadPoolExecutor that blocks a thread if number of executing threads exceeds
 * provided maximum number of permits. The implementation uses a semaphore that is initialized with
 * a max number of permits. Each thread grabs a permit and executes. If all permits are exhausted, a
 * thread blocks on a semaphore until a permit is available.
 * 
 */
public class UimaBlockingExecutor {

  private static final Class CLASS_NAME = UimaBlockingExecutor.class;

  private final ThreadPoolExecutor executor;

  private final Semaphore semaphore;

  private volatile boolean stopping = false;

  private String destination = null;

  public UimaBlockingExecutor(ThreadPoolExecutor executor, int permits) {
    this(executor, permits, null);
  }

  public UimaBlockingExecutor(ThreadPoolExecutor executor, int permits, String destination) {
    this.executor = executor;
    this.destination = destination;
    this.semaphore = new Semaphore(permits);
  }

  public boolean isReady() {
    if (executor.isShutdown() || executor.isTerminating() || executor.isShutdown()) {
      return false;
    }
    return true;
  }

  public void stop() {
    stopping = true;
    semaphore.release(); // in case we are blocking on acquire
    executor.purge();
    executor.shutdownNow();
  }

  public void submitTask(final Runnable task) throws InterruptedException,
          RejectedExecutionException {
    // Check if we are stopping. No need to continue if this is true
    if (stopping) {
      return;
    }
    SimpleDateFormat timeFormatter = new SimpleDateFormat("H:mm:ss:SSS");
    if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINEST)) {
      String extraInfo = ""; // changed if destination != null
      if (destination != null) {
        extraInfo = " Executor Handling Messages from Destination:" + destination;
      }
      UIMAFramework.getLogger(CLASS_NAME).logrb(
              Level.FINEST,
              CLASS_NAME.getName(),
              "submitTask",
              UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
              "UIMAEE_dump_threadpool_semaphore_info__FINEST",
              new Object[] { timeFormatter.format(new Date()), Thread.currentThread().getId(),
                  extraInfo, semaphore.availablePermits() });
    }
    // Get a permit. If one is not available BLOCK!
    semaphore.acquire();
    // Check if we are stopping. We may have waited for awhile above.
    // No need to continue if this is true
    if (stopping) {
      return;
    }
    try {
      executor.execute(new Runnable() {
        public void run() {
          try {
            task.run();
          } finally {
            semaphore.release();
          }
        }
      });
    } catch (RejectedExecutionException e) {
      // This should really never happen since we control number of executing threads
      // with a semaphore
      semaphore.release();
      throw e;
    } catch (Exception e) {
      semaphore.release();
      e.printStackTrace();
    }
  }
}
