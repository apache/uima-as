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
 * creation date: Aug 28, 2007, 10:40:13 AM
 * source:  DeploymentMetaData_Impl.java
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

import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;


/**
 *  Common abstract class for AEDeploymentMetaData_Impl and RemoteAEDeploymentMetaData_Impl
 *
 */
public abstract class DeploymentMetaData_Impl extends MetaDataObject_impl {

    protected AEDeploymentMetaData  parent;
    protected String key;

    /* (non-Javadoc)
     * @see com.ibm.uima.aae.deployment.AEDeploymentMetaData#getParent()
     */
    public AEDeploymentMetaData getParent() {
        return parent;
    }

    /**
     * @param parent the parent to set
     */
    public void setParent(AEDeploymentMetaData parent) {
        this.parent = parent;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.uima.aae.deployment.impl.AEDeploymentMetaData#getKey()
     */
    public String getKey() {
      return key;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.uima.aae.deployment.impl.AEDeploymentMetaData#setKey(java.lang.String)
     */
    public void setKey(String key) {
      this.key = key;
    }

}
