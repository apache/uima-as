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

import org.apache.uima.resourceSpecifier.ImportType;
import org.apache.uima.resourceSpecifier.factory.Import;
import org.springframework.util.Assert;

// TODO: Auto-generated Javadoc
/**
 * The Class ImportImpl.
 */
public class ImportImpl implements Import {

  /** The it. */
  private ImportType it;
  
  /**
   * Instantiates a new import impl.
   *
   * @param it the it
   */
  protected ImportImpl(ImportType it ) {
    this.it = it;
  }
  
  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Import#getLocation()
   */
  public String getLocation() {
    Assert.notNull(it);
    return it.getLocation();
  }

  /* (non-Javadoc)
   * @see org.apache.uima.resourceSpecifier.factory.Import#setLocation(java.lang.String)
   */
  public void setLocation(String location) {
    Assert.notNull(it);
    it.setLocation(location);
  }

public String getByName() {
    Assert.notNull(it);
    return it.getName();
}

public void setByName(String name) {
    Assert.notNull(it);
    it.setName(name);
	
}
  
}
