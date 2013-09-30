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
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.aae.deployment.AsyncAEErrorConfiguration;
import org.apache.uima.aae.deployment.GetMetadataErrors;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;
import org.apache.uima.tools.debug.util.Trace;


/**
 * 
 *
 */
public class GetMetadataErrors_Impl extends MetaDataObject_impl
                    implements AEDeploymentConstants, GetMetadataErrors
{
    private static final long serialVersionUID = 789496854750231918L;
    
    protected AsyncAEErrorConfiguration parent;
    
    protected int           maxRetries = DEFAULT_MAX_RETRIES;
    protected int           timeout = DEFAULT_GETMETADATA_TIMEOUT;
    protected String        errorAction = DEFAULT_ERROR_ACTION;

    /*************************************************************************/

    public GetMetadataErrors_Impl(AsyncAEErrorConfiguration parent) {
        this.parent = parent;
    }
    
    public GetMetadataErrors clone(AsyncAEErrorConfiguration parent) 
    {
        GetMetadataErrors clone = new GetMetadataErrors_Impl(parent);
        clone.setErrorAction(getErrorAction());
        clone.setMaxRetries(getMaxRetries());
        clone.setTimeout(getTimeout());
        
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
        } else if (id == KIND_ERRORACTION) {
            errorAction = (String) value;
        }
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.GetMetadataErrors#getErrorAction()
     */
    public String getErrorAction() {
        return errorAction;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.GetMetadataErrors#setErrorAction(java.lang.String)
     */
    public void setErrorAction(String errorAction) {
        this.errorAction = errorAction;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.GetMetadataErrors#getMaxRetries()
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.GetMetadataErrors#setMaxRetries(int)
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.GetMetadataErrors#getTimeout()
     */
    public int getTimeout() {
        return timeout;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.GetMetadataErrors#setTimeout(int)
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    
    /*************************************************************************/

}
