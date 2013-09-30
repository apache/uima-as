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

package org.apache.uima.camel;

import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.uima.aae.client.UimaAsynchronousEngine;

/**
 * Represent the UIMA-AS camel driver endpoint.
 */
@SuppressWarnings("rawtypes")
public class UimaAsEndpoint extends DefaultEndpoint {

	private String brokerAddress;
	private String queue;
	private Integer casPoolSize;
	private Integer timeout;
	
	public UimaAsEndpoint(String uri, String brokerAddress, 
    		UimaAsComponent component) {
		super(uri, component);
		
		this.brokerAddress = brokerAddress;
	}
	
	private Integer retrieveOptionIntegerParameter(Map options, String name) {
		Integer value = null;
		if (options.containsKey(name)) {
			String parameterValueString = (String) options.remove(name);
			
    		try {
    			value = Integer.parseInt(parameterValueString);
    		}
    		catch (NumberFormatException e) {
    			System.out.println("Warning " + name + " is invalid, fallback to default!");
    		}
		}
		
		return value;
	}
	
	
    @Override
    public void configureProperties(Map options) {
    	super.configureProperties(options);
    	queue = (String) options.remove("queue");
    	
    	casPoolSize = retrieveOptionIntegerParameter(options, UimaAsynchronousEngine.CasPoolSize);
		if (casPoolSize != null && casPoolSize < 1) {
			System.out.println("Warning " + UimaAsynchronousEngine.CasPoolSize + " must be larger than zero, fallback to default!");
			casPoolSize = null;
		}
		
		timeout = retrieveOptionIntegerParameter(options, UimaAsynchronousEngine.Timeout);
		if (timeout != null && timeout < 0) {
			System.out.println("Warning " + UimaAsynchronousEngine.Timeout + " must be larger than zero, fallback to default!");
			timeout = null;
		}
		
    }
    
    
	public Consumer createConsumer(Processor arg0) throws Exception {
		return null;
	}

	public Producer createProducer() throws Exception {
		return new UimaAsProducer(brokerAddress, queue, casPoolSize, timeout, this);
	}

	public boolean isSingleton() {
		return false;
	}
}
