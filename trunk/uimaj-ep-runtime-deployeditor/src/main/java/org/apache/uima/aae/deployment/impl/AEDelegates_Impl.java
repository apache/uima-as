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
 * creation date: Aug 11, 2007, 9:50:20 PM
 * source:  AEDelegates_Impl.java
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

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.aae.deployment.AEDeploymentConstants;
import org.apache.uima.aae.deployment.AEDeploymentMetaData;
import org.apache.uima.resource.metadata.MetaDataObject;
import org.apache.uima.resource.metadata.impl.MetaDataObject_impl;
import org.apache.uima.resource.metadata.impl.XmlizationInfo;
import org.apache.uima.util.XMLizable;


/**
 * 
 *
 */
public class AEDelegates_Impl extends MetaDataObject_impl
                    implements AEDeploymentConstants
{
    private static final long serialVersionUID = -1752995234054132490L;

    protected AEDeploymentMetaData  parent;

    // List of AEDeploymentMetaData or RemoteAEDeploymentMetaData
    protected List<XMLizable> list = new ArrayList<XMLizable>();
    protected List<String> delegateKeys = new ArrayList<String>();
    
    /*************************************************************************/

    public AEDelegates_Impl(AEDeploymentMetaData parent) {
        this.parent = parent;
    }
    
    protected XMLizable contains (String key) {
      if (delegateKeys.contains(key)) {
        return list.get(delegateKeys.indexOf(key));       
      } else {
        return null;
      }
    }
    
    public void addDelegate (DeploymentMetaData_Impl delegate) {
        delegate.setParent(parent);
        list.add (delegate);
        delegateKeys.add(delegate.getKey());
    }
    
    public void removeDelegate (DeploymentMetaData_Impl delegate) {
        list.remove (delegate);
        delegateKeys.remove(delegate.getKey());
    }
    
    public int indexOf (DeploymentMetaData_Impl delegate) {
        return list.indexOf(delegate);
    }
       
    public int replaceDelegate (int index, DeploymentMetaData_Impl newDelegate) {
        if (index != -1) {
            list.set(index, newDelegate);
            delegateKeys.set(index, newDelegate.getKey());
        }
        return index;
    }
    
    public int replaceDelegate (DeploymentMetaData_Impl oldDelegate, DeploymentMetaData_Impl newDelegate) {
        int index = list.indexOf(oldDelegate);
        if (index != -1) {
            list.set(index, newDelegate);
            delegateKeys.set(index, newDelegate.getKey());
        }
        return index;
    }
    
    public List<XMLizable> getDelegates () {
        return list;
    }
    
    /*************************************************************************/

    @Override
    protected XmlizationInfo getXmlizationInfo() {
        return new XmlizationInfo(null, null);
    }

    /*************************************************************************/
    
    
    
}
