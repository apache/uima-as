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
package org.apache.uima.aae.spi.transport;

import org.apache.uima.aae.UimaAsContext;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.error.UimaSpiException;
import org.apache.uima.aae.spi.transport.vm.UimaVmMessage;

public interface UimaTransport {

  /**
   * This method registers a given {@link SpiListener} instance with the SPI. The SPI will call all
   * registered listeners when the initialization is completed, when the start is completed, and
   * when the stop is completed.
   * 
   * @param aListener
   *          - SPI listener to receive events
   */
  public void addSpiListener(SpiListener aListener);

  /**
   * This method is called to start SPI.
   * 
   * @throws UimaSpiException
   *           - any problems while starting the SPI
   */
  public void startIt() throws UimaSpiException;

  /**
   * This method is called to stop SPI.
   * 
   * @throws UimaSpiException
   *           - any problems while stopping the SPI
   */
  public void stopIt() throws UimaSpiException;

  /**
   * Returns SPI Dispatcher
   * 
   * @return - instance of SPI Dispatcher
   * 
   * @throws UimaSpiException
   *           - any problems while fetching Dispatcher
   */
  public UimaMessageDispatcher getUimaMessageDispatcher() throws UimaSpiException;

  public UimaMessageListener getUimaMessageListener() throws UimaSpiException;

  public UimaMessageDispatcher getUimaMessageDispatcher(String aDelegateKey)
          throws UimaSpiException;

  public UimaMessageListener produceUimaMessageListener() throws UimaSpiException;

  public UimaMessageDispatcher produceUimaMessageDispatcher(UimaTransport aTransport)
          throws UimaSpiException;

  public UimaVmMessage produceMessage();

  public UimaVmMessage produceMessage(int aCommand, int aMessageType, String aMessageFrom);

  public void registerWithJMX(AnalysisEngineController aController, String queueKind);
}
