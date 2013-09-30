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

import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.Step;

public class ParallelStep extends Step {
  public ParallelStep(String[] aCasProcessorKeys) {
    mKeys = aCasProcessorKeys;
  }

  /**
   * Gets the key of the Analysis Engine to which the CAS should be routed.
   * 
   * @return an AnalysisEngine key
   */
  public String[] getAnalysisEngineKeys() {
    return mKeys;
  }

  /**
   * Sets the key of the Analysis Engine to which the CAS should be routed. By using this method, a
   * user's Flow implementation can (but is not required to) reuse the same SimpleStep object
   * multiple times.
   * 
   * @return an Analysis Engine key. This must be one of the keys in the FlowController's
   *         {@link FlowControllerContext#getAnalysisEngineMetaDataMap()}.
   */
  public void setAnalysisEngineKeys(String[] aKeys) {
    mKeys = aKeys;
  }

  private String[] mKeys;

}
