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

public interface InputQueue {

    /**
     * @return the brokerURL
     */
    public String getBrokerURL();

    /**
     * @param brokerURL the brokerURL to set
     */
    public void setBrokerURL(String brokerURL);

    /**
     * @return the endPoint
     */
    public String getEndPoint();

    /**
     * @param endPoint the endPoint to set
     */
    public void setEndPoint(String endPoint);

}