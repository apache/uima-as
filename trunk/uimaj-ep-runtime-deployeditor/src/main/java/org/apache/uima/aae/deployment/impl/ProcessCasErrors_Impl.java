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

/**
 * 
 * Project UIMA Tooling
 * 
 * 
 * creation date: Aug 11, 2007, 10:29:51 PM
 * source:  GetMetadataErrors.java
 */
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

package org.apache.uima.aae.deployment.impl;

import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.ProcessCasErrors;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;


/**
 * 
 *
 */
public class ProcessCasErrors_Impl extends MetaDataObject_impl
                    implements AEDeploymentConstants, ProcessCasErrors
{
    private static final long serialVersionUID = 789496854750231933L;
    
    protected AsyncAEErrorConfiguration parent;

    protected int           maxRetries = DEFAULT_MAX_RETRIES;
    protected int           timeout = DEFAULT_PROCESSCASERROR_TIMEOUT;
    protected boolean       continueOnRetryFailure = DEFAULT_CONTINUE_ON_RETRY_FAILURE;

    protected int           thresholdCount = DEFAULT_THRESHOLD_COUNT;
    protected int           thresholdWindow = DEFAULT_THRESHOLD_WINDOW;
    protected String        thresholdAction = DEFAULT_THRESHOLD_ACTION;

    /*************************************************************************/

    public ProcessCasErrors_Impl(AsyncAEErrorConfiguration parent) {
        this.parent = parent;
    }
    
    public ProcessCasErrors clone(AsyncAEErrorConfiguration parent) 
    {
        ProcessCasErrors clone = new ProcessCasErrors_Impl(parent);
        clone.setThresholdCount(getThresholdCount());
        clone.setThresholdWindow(getThresholdWindow());
        clone.setThresholdAction(getThresholdAction());
        clone.setMaxRetries(getMaxRetries());
        clone.setTimeout(getTimeout());
        clone.setContinueOnRetryFailure(isContinueOnRetryFailure());
        
        return clone;
    }
    
    
    /*************************************************************************/

    public AsyncAEErrorConfiguration getParent() {
        return parent;
    }

    @Override
    protected XmlizationInfo getXmlizationInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    public void setValueById (int id, Object value) {
        if (id == KIND_TIMEOUT) {
            timeout = ((Integer) value).intValue();
        } else if (id == KIND_MAX_RETRIES) {
            maxRetries = ((Integer) value).intValue();
        } else if (id == KIND_THRESHOLD_COUNT) {
            thresholdCount = ((Integer) value).intValue();
        } else if (id == KIND_THRESHOLD_WINDOW) {
            thresholdWindow = ((Integer) value).intValue();
        } else if (id == KIND_THRESHOLD_ACTION) {
            thresholdAction = (String) value;
        } else if (id == KIND_CONTINUE_ON_RETRY) {
            continueOnRetryFailure = Boolean.valueOf((String)value);
        }
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#isContinueOnRetryFailure()
     */
    public boolean isContinueOnRetryFailure() {
        return continueOnRetryFailure;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#setContinueOnRetryFailure(boolean)
     */
    public void setContinueOnRetryFailure(boolean continueOnRetryFailure) {
        this.continueOnRetryFailure = continueOnRetryFailure;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#getMaxRetries()
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#setMaxRetries(int)
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#getThresholdAction()
     */
    public String getThresholdAction() {
        return thresholdAction;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#setThresholdAction(java.lang.String)
     */
    public void setThresholdAction(String thresholdAction) {
        this.thresholdAction = thresholdAction;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#getThresholdCount()
     */
    public int getThresholdCount() {
        return thresholdCount;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#setThresholdCount(int)
     */
    public void setThresholdCount(int thresholdCount) {
        this.thresholdCount = thresholdCount;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#getThresholdWindow()
     */
    public int getThresholdWindow() {
        return thresholdWindow;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#setThresholdWindow(int)
     */
    public void setThresholdWindow(int thresholdWindow) {
        this.thresholdWindow = thresholdWindow;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#getTimeout()
     */
    public int getTimeout() {
        return timeout;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.ProcessCasErrors#setTimeout(int)
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    
    /*************************************************************************/

}
