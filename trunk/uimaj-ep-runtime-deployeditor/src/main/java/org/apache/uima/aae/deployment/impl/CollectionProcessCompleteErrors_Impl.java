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
import org.apache.uima.aae.deployment.CollectionProcessCompleteErrors;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;


/**
 * 
 *
 */
public class CollectionProcessCompleteErrors_Impl extends MetaDataObject_impl
                    implements AEDeploymentConstants, CollectionProcessCompleteErrors
{
    private static final long serialVersionUID = 789434854750231933L;
    
    protected AsyncAEErrorConfiguration parent;

    protected int           timeout = DEFAULT_COLLPROCESSCOMPLETEERROR_TIMEOUT;
    protected String        additionalErrorAction = DEFAULT_ADDITIONAL_ERROR_ACTION;

    /*************************************************************************/

    public CollectionProcessCompleteErrors_Impl(AsyncAEErrorConfiguration parent) {
        this.parent = parent;
    }
    
    public CollectionProcessCompleteErrors clone(AsyncAEErrorConfiguration parent) 
    {
        CollectionProcessCompleteErrors clone = new CollectionProcessCompleteErrors_Impl(parent);
        clone.setTimeout(getTimeout());
        clone.setAdditionalErrorAction(getAdditionalErrorAction());
    
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
        } else if (id == KIND_ADDITIONA_ERROR_ACTION) {
            additionalErrorAction = (String) value;
        }
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.CollectionProcessCompleteErrors#getAdditionalErrorAction()
     */
    public String getAdditionalErrorAction() {
        return additionalErrorAction;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.CollectionProcessCompleteErrors#setAdditionalErrorAction(java.lang.String)
     */
    public void setAdditionalErrorAction(String additionalErrorAction) {
        this.additionalErrorAction = additionalErrorAction;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.CollectionProcessCompleteErrors#getTimeout()
     */
    public int getTimeout() {
        return timeout;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.CollectionProcessCompleteErrors#setTimeout(int)
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    
    /*************************************************************************/

}
