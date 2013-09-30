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
package org.apache.uima.resourceSpecifier.factory;

import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resourceSpecifier.AnalysisEngineDeploymentDescriptionDocument;
import org.apache.uima.resourceSpecifier.factory.impl.CollectionProcessCompleteErrorHandlingSettingsImpl;
import org.apache.uima.resourceSpecifier.factory.impl.ColocatedDelegateConfigurationImpl;
import org.apache.uima.resourceSpecifier.factory.impl.DelegateConfigurationImpl;
import org.apache.uima.resourceSpecifier.factory.impl.GetMetaErrorHandlingSettingsImpl;
import org.apache.uima.resourceSpecifier.factory.impl.ProcessErrorHandlingSettingsImpl;
import org.apache.uima.resourceSpecifier.factory.impl.RemoteDelegateConfigurationImpl;
import org.apache.uima.resourceSpecifier.factory.impl.UimaASAggregateDeploymentDescriptorImpl;
import org.apache.uima.resourceSpecifier.factory.impl.UimaASPrimitiveDeploymentDescriptorImpl;

/**
 * Factory class providing static API to generate UIMA AS Deployment Descriptor. Supports
 * creation of both Primitive and Aggregate Deployment Descriptors.
 * <p>
 *  
 */
public final class DeploymentDescriptorFactory {

  /**
   * Prevent instantiation of this class. Only static access allowed.
   */
  private DeploymentDescriptorFactory() {
    //  Forbid instantiation. This class is meant for static access only.
  }

  /**
   * Parses provided UIMA AS deployment descriptor contents (as String) and returns a Java Object representing
   * the descriptor.
   *
   * @param xmlDescriptor - deployment descriptor xml contents as String
   * @return - Java Object representing deployment descriptor
   * @throws ResourceInitializationException the resource initialization exception
   */
  /*
  public static UimaASDeploymentDescriptor createDeploymentDescriptor(String xmlDescriptor) 
  throws ResourceInitializationException {
    try {
      return new UimaASDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument.Factory.parse(xmlDescriptor),new ServiceContextImpl("","","","") );
    } catch( XmlException e ) {
      throw new ResourceInitializationException(e);
    }
  }
  */
  /**
   * Parses provided UIMA AS deployment descriptor xml file and returns a Java Object representing
   * the descriptor.
   *
   * @param xmlFileDescriptor - deployment descriptor file
   * @return - Java Object representing deployment descriptor
   * @throws ResourceInitializationException the resource initialization exception
   */
  /*
  public static UimaASDeploymentDescriptor createDeploymentDescriptor(File xmlFileDescriptor) 
  throws ResourceInitializationException {
    try {
      return new UimaASDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument.Factory.parse(xmlFileDescriptor),new ServiceContextImpl("","","","") );
    } catch( Exception e ) {
      throw new ResourceInitializationException(e);
    }
  }
  */
  /**
   * Parses provided UIMA AS deployment descriptor InputStream and returns a Java Object representing
   * the descriptor.
   *
   * @param descriptorInputStream - deployment descriptor InputStream
   * @return - Java Object representing deployment descriptor
   * @throws ResourceInitializationException the resource initialization exception
   */
  /*
  public static UimaASDeploymentDescriptor createDeploymentDescriptor(InputStream descriptorInputStream) 
  throws ResourceInitializationException {
    try {
      return new UimaASDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument.Factory.parse(descriptorInputStream),new ServiceContextImpl("","","","") );
    } catch( Exception e ) {
      throw new ResourceInitializationException(e);
    }
  }
*/
  /**
   * Creates new Aggregate UIMA-AS DeploymentDescriptor object.
   *
   * @param context - context
   * @param delegateConfigurations - delegate configurations
   * @return UIMA-AS aggregate deployment descriptor
   * @throws ResourceInitializationException the resource initialization exception
   */
  public static UimaASAggregateDeploymentDescriptor createAggregateDeploymentDescriptor(ServiceContext context, DelegateConfiguration ... delegateConfigurations) 
  throws ResourceInitializationException {
    return new UimaASAggregateDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument.Factory.newInstance(), context, delegateConfigurations);
  }
  
  /**
   * Creates new Primitive UIMA-AS DeploymentDescriptor object.
   *
   * @param context - context
   * @return UIMA-AS primitive deployment descriptor
   * @throws ResourceInitializationException the resource initialization exception
   */
  public static UimaASPrimitiveDeploymentDescriptor createPrimitiveDeploymentDescriptor(ServiceContext context) 
  throws ResourceInitializationException {
    return new UimaASPrimitiveDeploymentDescriptorImpl(AnalysisEngineDeploymentDescriptionDocument.Factory.newInstance(),context);
  }
  /**
 * Creates Delegate Configuration for a Primitive delegate.
 *
 * @param key - unique delegate key
 * @param errorHandlingSettings the error handling settings
 * @return the delegate configuration
 */
public static DelegateConfiguration createPrimitiveDelegateConfiguration(String key, ErrorHandlingSettings ...errorHandlingSettings) {
    return new ColocatedDelegateConfigurationImpl(key, new DelegateConfigurationImpl[0], errorHandlingSettings);
  }
  
  
  /**
   * Creates Delegate Configuration for a co-located delegate.
   *
   * @param key - unique delegate key
   * @param delegateConfiguration the delegate configuration
   * @return the colocated delegate configuration
   */
  public static ColocatedDelegateConfiguration createAggregateDelegateConfiguration(String key, DelegateConfiguration ...delegateConfiguration) {
    return new ColocatedDelegateConfigurationImpl(key, delegateConfiguration);
  }
  
  
  /**
 * Creates Delegate Configuration for a remote delegate.
 *
 * @param key - unique delegate key
 * @param brokerURL - broker url
 * @param endpoint - delegate remote endpoint
 * @param serialization - serialization strategy (xmi or binary)
 * @param errorHandlingSettings - error handling settings for this delegate
 * @return remote delegate configuration
 */
public static RemoteDelegateConfiguration createRemoteDelegateConfiguration(String key, String brokerURL, String endpoint, SerializationStrategy serialization, ErrorHandlingSettings ...errorHandlingSettings) {
    return new RemoteDelegateConfigurationImpl(key,brokerURL,endpoint,serialization,errorHandlingSettings);
  }
  
  /**
   * Creates Default Error Handling Settings for GetMeta
   *
   * @return default GetMeta error handling settings
   */
  public static GetMetaErrorHandlingSettings createGetMetaErrorHandlingSettings() {
    return new GetMetaErrorHandlingSettingsImpl();
  }

  /**
   * Creates Error Handling Configuration for GetMeta with provided settings
   *
   * @param retryCount - retry count for GetMeta
   * @param timeout - GetMeta timeout in millis
   * @param action - action to take on error
   * @return the gets the meta error handling settings
   */
  public static GetMetaErrorHandlingSettings createGetMetaErrorHandlingSettings(int retryCount, int timeout, Action action) {
    return new GetMetaErrorHandlingSettingsImpl(retryCount, timeout, action);
  }
  
  /**
   * Creates Default Error Handling Configuration for Process
   *
   * @return default Process error handling settings
   */
  public static ProcessErrorHandlingSettings createProcessErrorHandlingSettings() {
    return new ProcessErrorHandlingSettingsImpl();
  }
  
  /**
   * Creates Error Handling Configuration for Process using provided settings
   *
   * @param retryCount - number of retries
   * @param timeout - Process timeout in millis
   * @param action - action to take on error
   * @param continueOnRetryFailure - continue on retry failure (true/false)
   * @param thresholdCount - threshold limit triggering action to be taken
   * @param thresholdWindow - error threshold window
   * @return the process error handling settings
   */
  public static ProcessErrorHandlingSettings createProcessErrorHandlingSettings(int retryCount, int timeout, Action action, boolean continueOnRetryFailure,int thresholdCount, int thresholdWindow) {
    return new ProcessErrorHandlingSettingsImpl(retryCount,timeout, action,continueOnRetryFailure, thresholdCount, thresholdWindow );
  }
  
  /**
   * Creates Error Handling Configuration for CPC using provided settings
   *
   * @param timeout - CPC timeout
   * @param action - action to take on error
   * @return the collection process complete error handling settings
   */
  public static CollectionProcessCompleteErrorHandlingSettings createCollectionProcessCompleteErrorHandlingSettings(int timeout, Action action) {
    return new CollectionProcessCompleteErrorHandlingSettingsImpl(timeout, action);
  }
  


}
