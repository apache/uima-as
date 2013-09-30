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
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;


public class InputQueue_Impl extends MetaDataObject_impl 
                        implements AEDeploymentConstants, InputQueue
{
    private static final long serialVersionUID = -3427240441303143531L;
    
    protected String        endPoint = "";
    protected String        brokerURL = "";
    
    /*************************************************************************/

    public InputQueue_Impl() {
    }
    
    /*************************************************************************/

    @Override
    protected XmlizationInfo getXmlizationInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.InputQueue#getBrokerURL()
     */
    public String getBrokerURL() {
        return brokerURL;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.InputQueue#setBrokerURL(java.lang.String)
     */
    public void setBrokerURL(String brokerURL) {
        this.brokerURL = brokerURL;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.InputQueue#getEndPoint()
     */
    public String getEndPoint() {
        return endPoint;
    }

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.impl.InputQueue#setEndPoint(java.lang.String)
     */
    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

}
