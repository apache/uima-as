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
package org.apache.uima.resourceSpecifier.factory.impl;

import org.apache.uima.resourceSpecifier.TopDescriptorType;
import org.apache.uima.resourceSpecifier.factory.Import;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.TopDescriptor;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class TopDescriptorImpl.
 */
public class TopDescriptorImpl implements TopDescriptor {
  
  /** The tdt. */
  private TopDescriptorType tdt;
  
  /** The resource import. */
  private Import resourceImport;
  /*
   * !!!!!!!!!!! Modify to support import by name
   */
  /**
   * Instantiates a new top descriptor impl.
   *
   * @param tdt the tdt
   * @param context the context
   */
  public TopDescriptorImpl(TopDescriptorType tdt, ServiceContext context) {
    this.tdt = tdt;
    if ( context.getDescriptor().endsWith("xml")) {
        getImport().setLocation(context.getDescriptor());
    } else {
        getImport().setByName(context.getDescriptor());
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.TopDescriptor#getImport()
   */
  public Import getImport() {
    Assert.notNull(tdt);
    if ( resourceImport == null ) {
      resourceImport = new ImportImpl(tdt.addNewImport());
    }
    return resourceImport;
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.TopDescriptor#setImport(org.apache.uima.resourceSpecifier.factory.Import)
   */
  public void setImport(Import imprt) {
    resourceImport = imprt;
    if ( resourceImport.getLocation() != null && resourceImport.getLocation().endsWith("xml")) {
        getImport().setLocation(imprt.getLocation());
    } else {
        getImport().setByName(imprt.getByName());
    }
  }

}
