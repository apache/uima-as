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

package org.apache.uima.aae.deployment;

public interface ProcessCasErrors 
{
    final static int        KIND_THRESHOLD_COUNT    = 1;
    final static int        KIND_THRESHOLD_WINDOW   = 2;
    final static int        KIND_THRESHOLD_ACTION   = 3;
    final static int        KIND_TIMEOUT            = 4;
    final static int        KIND_MAX_RETRIES        = 5;
    final static int        KIND_CONTINUE_ON_RETRY  = 6;
    
    // String name used in the table view
    final static String NAME_THRESHOLD_COUNT    = "Threshold Count";
    final static String NAME_THRESHOLD_WINDOW   = "Threshold Window";
    final static String NAME_THRESHOLD_ACTION   = "Threshold Action";
    final static String NAME_DELEGATE_THRESHOLD_COUNT    = "Delegate Threshold Count";
    final static String NAME_DELEGATE_THRESHOLD_WINDOW   = "Delegate Threshold Window";
    final static String NAME_DELEGATE_THRESHOLD_ACTION   = "Delegate Threshold Action";
    final static String NAME_MAX_RETRIES          = "CAS Max Retries";
    final static String NAME_TIMEOUT              = "CAS Timeout (in millisec)";    
    final static String NAME_CONTINUE_ON_FAILURE  = "CAS Continue On Failure";


    public ProcessCasErrors clone(AsyncAEErrorConfiguration parent);
    
    public AsyncAEErrorConfiguration getParent();

    public void setValueById (int id, Object value);

    /**
     * @return the continueOnRetryFailure
     */
    public boolean isContinueOnRetryFailure();

    /**
     * @param continueOnRetryFailure the continueOnRetryFailure to set
     */
    public void setContinueOnRetryFailure(boolean continueOnRetryFailure);

    /**
     * @return the maxRetries
     */
    public int getMaxRetries();

    /**
     * @param maxRetries the maxRetries to set
     */
    public void setMaxRetries(int maxRetries);

    /**
     * @return the thresholdAction
     */
    public String getThresholdAction();

    /**
     * @param thresholdAction the thresholdAction to set
     */
    public void setThresholdAction(String thresholdAction);

    /**
     * @return the thresholdCount
     */
    public int getThresholdCount();

    /**
     * @param thresholdCount the thresholdCount to set
     */
    public void setThresholdCount(int thresholdCount);

    /**
     * @return the thresholdWindow
     */
    public int getThresholdWindow();

    /**
     * @param thresholdWindow the thresholdWindow to set
     */
    public void setThresholdWindow(int thresholdWindow);

    /**
     * @return the timeout
     */
    public int getTimeout();

    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(int timeout);

}