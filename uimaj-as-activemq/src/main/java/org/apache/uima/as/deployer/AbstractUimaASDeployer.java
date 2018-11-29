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
package org.apache.uima.as.deployer;

import java.util.concurrent.CountDownLatch;

import org.apache.uima.aae.UimaASApplicationEvent.EventTrigger;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.controller.ControllerCallbackListener;
import org.apache.uima.resource.ResourceInitializationException;

public abstract class AbstractUimaASDeployer 
implements UimaAsServiceDeployer, ControllerCallbackListener {
	CountDownLatch latch;
	private ResourceInitializationException initException = null;
	protected AbstractUimaASDeployer(CountDownLatch latch) {
		this.latch = latch;
	}

	public void waitUntilInitialized() throws InterruptedException, ResourceInitializationException {
		latch.await();
		if ( initException != null ) {
			throw initException;
		}
	}
	@Override
	public void notifyOnTermination(String aMessage, EventTrigger cause) {
		// TODO Auto-generated method stub

	}

	@Override
	public void notifyOnInitializationFailure(AnalysisEngineController aController, Exception e) {
		// TODO Auto-generated method stub
		System.out.println("------- Controller:"+aController.getName()+" Exception During Initialization - Error:\n");
		notifyOnInitializationFailure(e);
	}

	@Override
	public void notifyOnInitializationSuccess(AnalysisEngineController aController) {
		System.out.println("------- Controller:"+aController.getName()+" Initialized");
		latch.countDown();
	}

	@Override
	public void notifyOnInitializationFailure(Exception e) {
		if ( !(e instanceof ResourceInitializationException) ) {
			initException = new ResourceInitializationException(e);
		} else {
			initException = (ResourceInitializationException) e;
		}
		latch.countDown();
		e.printStackTrace();
		

	}

	@Override
	public void notifyOnInitializationSuccess() {
		// TODO Auto-generated method stub
System.out.println(".................. WHAT?");
	}

	@Override
	public void notifyOnReconnecting(String aMessage) {
		// TODO Auto-generated method stub

	}

	@Override
	public void notifyOnReconnectionSuccess() {
		// TODO Auto-generated method stub

	}
}