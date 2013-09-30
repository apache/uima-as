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

package org.apache.uima.aae.deployment;

import org.apache.uima.ResourceSpecifierFactory;
import org.apache.uima.UIMAFramework;
import org.apache.uima.util.XMLParser;

public class UimaParserExtender 
{
	static boolean                      isInitialized_UimaApplicationFramework = false;
	private static XMLParser          uimaXMLParser;

	/*************************************************************************/

	/**
	 * Initialize UIMA XMLParser with new extension tags
	 * 
	 */
	static public void initUimaApplicationFramework ()
	{
		if (isInitialized_UimaApplicationFramework) return;

		isInitialized_UimaApplicationFramework = true;
		uimaXMLParser = org.apache.uima.UIMAFramework.getXMLParser();        
		try {
			//
			// For UIMA-ee Parsing
			//
			uimaXMLParser.addMapping("analysisEngineDeploymentDescription", 
			"org.apache.uima.aae.deployment.impl.AEDeploymentDescription_Impl");
			uimaXMLParser.addMapping("service", 
			"org.apache.uima.aae.deployment.impl.AEService_Impl");
			uimaXMLParser.addMapping("analysisEngine", 
			"org.apache.uima.aae.deployment.impl.AEDeploymentMetaData_Impl");
			uimaXMLParser.addMapping("remoteAnalysisEngine", 
			"org.apache.uima.aae.deployment.impl.RemoteAEDeploymentMetaData_Impl");
			uimaXMLParser.addMapping("asyncAggregateErrorConfiguration", 
			"org.apache.uima.aae.deployment.impl.AsyncAggregateErrorConfiguration_Impl");
			uimaXMLParser.addMapping("asyncPrimitiveErrorConfiguration", 
			"org.apache.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration_Impl");

			// parse("c:/uima/Test/testApp.xml");

			//
			// For object creation
			//
			ResourceSpecifierFactory factory = UIMAFramework.getResourceSpecifierFactory();
			factory.addMapping("org.apache.uima.aae.deployment.AEDeploymentDescription", 
			"org.apache.uima.aae.deployment.impl.AEDeploymentDescription_Impl");
			factory.addMapping("org.apache.uima.aae.deployment.AEDeploymentMetaData", 
			"org.apache.uima.aae.deployment.impl.AEDeploymentMetaData_Impl");
			factory.addMapping("org.apache.uima.aae.deployment.RemoteAEDeploymentMetaData", 
			"org.apache.uima.aae.deployment.impl.RemoteAEDeploymentMetaData_Impl");
			factory.addMapping("org.apache.uima.aae.deployment.AsyncAggregateErrorConfiguration", 
			"org.apache.uima.aae.deployment.impl.AsyncAggregateErrorConfiguration_Impl");
			factory.addMapping("org.apache.uima.aae.deployment.AsyncPrimitiveErrorConfiguration", 
			"org.apache.uima.aae.deployment.impl.AsyncPrimitiveErrorConfiguration_Impl");

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        
	} // initUimaApplicationFramework


}
