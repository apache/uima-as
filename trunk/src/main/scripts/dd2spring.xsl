<?xml version='1.0'?>
  <!--
   ***************************************************************
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
   ***************************************************************
   -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:u="http://uima.apache.org/resourceSpecifier"
                xmlns:f="http://uima.apache.org/uimaEE/xslt/functions"
                xmlns:fn="http://www.w3.org/2005/xpath-functions"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:i="http://uima.apache.org/uimaEE/xslt/internal"
                xmlns:x1="org.apache.uima.aae.deploymentDescriptor.XsltImportByName"
                xmlns:x2="org.apache.uima.aae.deploymentDescriptor.XsltGUIDgenerator"
                xmlns:saxon="http://saxon.sf.net/"
                version='2.0'
                exclude-result-prefixes="u f fn xs i x1 x2 saxon">

  <!--  note: XSLT 2.0 required -->
  <!--  Changes
       6/30/2007 support remote primitive cas multipliers
       7/12/2007 support remote brokers for TCP replies based on new XML parm
       7/18/2007 change async defaulting to only default to true if there are delegates
       9/xx/2007 add support for CPP run as separate process for top level, and env vars
      10/01/2007 modify above to need new config param to enable
      10/02/2007 make default action for getMeta if not specified be terminate
      10/2x/2007 Add new attribute to set default CAS Fs Heap Size for the pools
      10/25/2007 Add new listener containers for inputs that enable separate thread for getMeta calls
      11/02/2007 Impl defaulting of casPool, and add initialFsHeapSize to both Java and C++ versions
      11/12/2007 For Reply Qs running with external broker which is the same as the top-level external
                 broker for the service, add temp queue, refer to it using destination Resolver in the
                 listener containers, and plug those listener containers into endpoints of the delegates.
      12/10/2007 support 2 initialFsHeapSize values - one for C++ and one for Java
      12/10/2007 change logic for temp q - do whenever reply q is remote
      12/10/2007 add check for inconsistency - disallow remote delegate specifying 
                 vm://localhost as the brokerURL broker
      11/xx/2007 NOT YET DONE rename pool elements for consistency between cas multiplier and main cas pool
      further updates moved to SVN comments
    --> 

  <!--============================================================-->       
  <!--| Input: Deployment Descriptor                             |-->       
  <!--| Additional inputs:  for each service descriptor, there is|-->       
  <!--|   an associated top-level aggregate descriptor.          |-->       
  <!--|                                                          |-->       
  <!--| Defaulting:  The Deployment descriptor is processed      |-->       
  <!--|   to insert defaults                                     |-->       
  <!--|   During this process, for each service descriptor,      |-->       
  <!--|     the associated top-level aggregate descriptor is     |-->       
  <!--|     read in, and for each delegate,                      |-->       
  <!--|     the import element is read in                        |-->       
  <!--|                                                          |-->             
  <!--| Resolving relative paths                                 |-->             
  <!--|   During pass 1 - putting in defaults, the relative      |-->             
  <!--|   paths are resolved, and any information from the       |-->             
  <!--|   UIMA descriptor is saved in the defaulted tree.        |-->             
  <!--|                                                          |-->             
  <!--|   Relative and absolute paths are supported              |-->             
  <!--|     Paths with back slashes are converted to /           |-->             
  <!--|     Relative paths are concatenated when nesting         |-->             
  <!--|     When processing the resulting Spring XML file,       |-->             
  <!--|     Spring resolves relative to the working directory.   |-->
  <!--|                                                          |-->             
  <!--| To catch errors, elements are verified by looking for    |-->             
  <!--|   for misspelled/misplaced elements and attributes       |-->             
  <!--|                                                          |-->             
  <!--============================================================-->
    
  <xsl:output method="xml" indent="yes"
    doctype-public="-//Spring//DTD BEAN//EN"
    doctype-system="http://www.springframework.org/dtd/spring-beans.dtd" 
    />
 
  <xsl:param name="useRelativePaths"/>
  
  <!--xsl:param name="noTempQueues" select="()"/-->  <!-- not used anymore -->
  
  <xsl:variable name="document-uri" select="document-uri(.)"/>
  
  <xsl:variable name="guid" select="x2:getGUID()"/>
  
  <!-- collect unique instances of queue brokers where we need generate
       a broker factory.
    
       obsolete ->> Jira UIMA-1288 Omit vm://localBroker ones - those use a common factory for this node
       Include remotes if the replyQueue is "remote"
       
    -->
  <xsl:variable name="uniqueInputQueueBrokers">
    <!--xsl:for-each-group group-by="@brokerURL"
      select="$ddd//u:inputQueue
         [ @brokerURL ne 'vm://localBroker' and 
           not(ancestor::u:delegates and not(starts-with(@brokerURL, 'http://')))
         ]">
       <xsl:sequence select="."/>
    </xsl:for-each-group-->
    
    <!-- inputQueue elements occur at the top and within remote delegates
         get the unique set of elements
      The code for the first select used to include this "and" clause
      and 
           (not(ancestor::u:delegates) or
            (../u:replyQueue/@location eq 'remote'))
          -->
    <xsl:for-each-group group-by="@brokerURL"
      select="$ddd//u:inputQueue
         [ @brokerURL ne 'vm://localhost' ]">  
       <!--xsl:message select="('*** ', ..)"/-->
       <xsl:sequence select="."/>
    </xsl:for-each-group>
  </xsl:variable>
  
  <!--xsl:variable name="topLevelInputQueueBroker" 
    select="$ddd/u:analysisEngineDeploymentDescription/u:deployment/u:service/u:inputQueue/@brokerURL"/-->
       
  <!--============================================================-->       
  <!--=  Error Configuration Details are sharable                =-->       
  <!--============================================================--> 
  <xsl:function name="f:ecdKey">
    <xsl:param name="node"/>
    <xsl:sequence select="concat(
      if ($node/@maxRetries)             then concat('_Rtry_',     $node/@maxRetries)             else '',
      if ($node/@continueOnRetryFailure) then concat('_Continue_', $node/@continueOnRetryFailure) else '',
      if ($node/@thresholdCount)         then concat('_TCnt_',     $node/@thresholdCount)         else '',
      if ($node/@thresholdWindow)        then concat('_TWndw_',    $node/@thresholdWindow)        else '',
      if ($node/@thresholdAction)        then concat('_Action_',   $node/@thresholdAction)        else '',
      if ($node/@errorAction)            then concat('_Action_',   $node/@errorAction)            else '',
      if ($node/@additionalErrorAction)  then concat('_Action_',   $node/@additionalErrorAction)  else '')"/>
  </xsl:function>
  
  <xsl:variable name="uniqueErrorConfigDetails">
    <xsl:for-each-group group-by="f:ecdKey(.)" 
          select="$ddd//(u:getMetadataErrors|u:processCasErrors|u:collectionProcessCompleteErrors)"> 
      <xsl:sequence select="."/>       
    </xsl:for-each-group>
  </xsl:variable>
  
  <xsl:template name="generateErrorConfigDetails">
      <xsl:sequence select="$nl"/>
      <xsl:sequence select="f:generateBlockComment('   E r r o r   D e t a i l s   ', 3)"/>
      
      <xsl:for-each select="$uniqueErrorConfigDetails/*">

        <bean id="{f:getErrorConfigDetailID(.)}"
          class="org.apache.uima.aae.error.Threshold" singleton="true">
          
          <xsl:if test="@maxRetries">
            <property name="maxRetries" value="{@maxRetries}"/>
          </xsl:if>
          
          <xsl:if test="@continueOnRetryFailure">
            <property name="continueOnRetryFailure" value="{@continueOnRetryFailure}"/>
          </xsl:if>
          
          <xsl:if test="@thresholdCount">
            <property name="threshold" value="{@thresholdCount}"/>
          </xsl:if>
          
          <xsl:if test="@thresholdWindow">
            <property name="window" value="{@thresholdWindow}"/>
          </xsl:if>
         
          <xsl:if test="@errorAction | @thresholdAction | @additionalErrorAction">
            <property name="action" value="{concat(@errorAction, @thresholdAction, @additionalErrorAction)}"/>
          </xsl:if>
          
        </bean>
        <xsl:sequence select="$nl2"/>
      </xsl:for-each>
  </xsl:template>
  
  <xsl:function name="f:getErrorConfigDetailID">
    <xsl:param name="errorNode"/>
    <xsl:sequence select="concat('errorConfig_', f:ecdKey($errorNode))"/>
  </xsl:function>  
      
    <!--
            select="$ddd/u:analysisEngineDeploymentDescription/
                u:deployment[@protocol='jms' and @provider='activemq']/              
                  u:service/
                    u:inputQueue[@brokerURL ne 'vm://localBroker']">

      
      
    <xsl:variable name="iqs">
      
        <xsl:sequence select=
      "$ddd/u:analysisEngineDeploymentDescription/
              u:deployment[@protocol='jms' and @provider='activemq']/              
                  u:service/
                    u:inputQueue[@brokerURL ne 'vm://localBroker']"/>
     
    </xsl:variable> 
    <xsl:if test="$iqs">
      <xsl:for-each-group group-by="@brokerURL" select="$iqs/u:inputQueue"> 
        <xsl:sequence select="."/>       
      </xsl:for-each-group>
    </xsl:if>
  -->
  
  
  <!--============================================================-->       
  <!--           Top level match                                  -->       
  <!--============================================================--> 
  <xsl:template match="u:analysisEngineDeploymentDescription">
    <!--xsl:message select="'debugging - verify version'"/-->
    <xsl:sequence select="f:generateBlockComment(
      ('Generated from', $document-uri, format-dateTime(current-dateTime(), '[D1] [MNn], [Y], [h]:[m]:[s] [PN]')), 1)"/>
    <xsl:apply-templates select="$ddd/u:analysisEngineDeploymentDescription/u:deployment"/> 
  </xsl:template>
  
  <xsl:template match="u:deployment[@protocol='jms' and @provider='activemq']">
    <beans>
      <xsl:if test="u:service[@i:isTopLvlCpp eq 'no']">
        <xsl:call-template  name="generateStandardBeans"/>
      </xsl:if> 
      <!--xsl:apply-templates select="u:applications"/-->
      <xsl:apply-templates select="u:service"/>
    </beans>
  </xsl:template>
 
  <!--============================================================-->       
  <!--=          applications                                    =-->       
  <!--============================================================--> 
  <xsl:template match="u:application/u:inputQueue">
    <!-- output something to specify where the application sends messages to -->
  </xsl:template>
  
  <!--============================================================-->       
  <!--=          service                                         =-->       
  <!--============================================================--> 
  <xsl:template  match="u:service">
    <!--xsl:message select="'*** uri '"/>
    <xsl:message select="document-uri(u:topDescriptor/u:import/@location)"/-->
 
    <xsl:variable name="iqn" select="concat('top_level_input_queue_service_',position())"/>
       
    <xsl:if test="@i:isTopLvlCpp eq 'no'">
      <xsl:sequence select="$nl"/>
      <xsl:comment select="'input queue for service'"/><xsl:sequence select="concat($nl,'   ')"/>
      <bean id="{$iqn}" 
          class="org.apache.activemq.command.ActiveMQQueue">
        <constructor-arg index="0" value="{u:inputQueue/@endpoint}"/>
      </bean>
    </xsl:if>
    
    <xsl:if test="u:analysisEngine/u:casMultiplier">
      <xsl:sequence select="$nl"/>
      <xsl:sequence select="f:generateBlockComment(
        ('Second Input Q needed for top-level CAS Multiplers',
        'Used to have the client signal completion',
        'of the processing of a CAS',
        'generated by the Cas Multiplier'), 3)"/>
      <bean id="{concat($iqn,'__CasSync')}" class="org.apache.activemq.command.ActiveMQQueue">
        <constructor-arg index="0" value="{concat(u:inputQueue/@endpoint, '__CasSync')}"/>
      </bean>
    </xsl:if>
    
    <xsl:apply-templates >
      <xsl:with-param tunnel="yes" name="input_q_ID" select="$iqn"/>
      <xsl:with-param tunnel="yes" name="inputQueueFactoryID" select="f:getQbrokerID(u:inputQueue)"/>
    </xsl:apply-templates>       
  </xsl:template>

  <!--============================================================-->       
  <!--=          Aggregate Analysis Engine (async=yes)           =-->       
  <!--============================================================--> 
  <xsl:template match="u:analysisEngine[@async eq 'true']">
    <xsl:param tunnel="yes" name="input_q_ID"/>
    <xsl:param tunnel="yes" name="inputQueueFactoryID"/>
    
    <!--xsl:message select="'*** Async AA'"/>
    <xsl:message select="."/-->
    
    <xsl:variable name="AEdescription"
      select="i:local_ae_descriptor/(u:analysisEngineDescription|u:casConsumerDescription|u:taeDescription)"/>
    
    <xsl:sequence
      select="f:generateBlockComment(
      concat('Async Aggregate: ', f:getAeNameUnique(.)),3)"/>
    
    <xsl:variable name="uniq" select="f:getUnique(.)"/>
    
    <xsl:variable name="aeNameUnique" select="f:getAeNameUnique(.)"/>
    <xsl:variable name="ctlrID" select="f:getControllerID(.)"/>
    <!--concat('asAggr_ctlr_', $aeNameUnique)"/>-->
    <xsl:variable name="delegateMapID"
      select="concat('delegate_map_', $aeNameUnique)"/>
    
    <xsl:variable name="flowControllerPath">
      <xsl:choose>
        <xsl:when test="i:flowController">
          <!--xsl:message select="'Custom flow controller being used'"/>
          <xsl:message select="."/-->
          <xsl:sequence select="string(i:flowController/@file_path)"/>
        </xsl:when>
        <xsl:when
          test="$AEdescription/u:analysisEngineMetaData/u:flowConstraints/u:fixedFlow">
          <xsl:sequence
            select="'*importByName:org.apache.uima.flow.FixedFlowController'"/>
        </xsl:when>
        <xsl:when
          test="$AEdescription/u:analysisEngineMetaData/u:flowConstraints/u:capabilityLanguageFlow">
          <xsl:sequence
            select="'*importByName:org/apache/uima/flow/CapabilityLanguageFlowController'"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:message select="'*** Internal error, i:local_ae_descriptor'"/>
          <xsl:message select="i:local_ae_descriptor"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    
    <!--xsl:message select="concat('Flow controller for ', $aeNameUnique, ' is ', $flowControllerPath)"/-->
    <!-- removed lazy init: lazy-init="{if (parent::u:service) then 'true' else 'false'}" -->
    <bean id="{$ctlrID}"
      class="org.apache.uima.aae.controller.AggregateAnalysisEngineController_impl"
      init-method="initialize">
      <!-- parent controller -->
      <xsl:choose>
        <xsl:when test="parent::u:delegates">
          <constructor-arg index="0"
            ref="{f:getControllerID(ancestor::u:analysisEngine[1])}"/>
        </xsl:when>
        <xsl:otherwise>
          <constructor-arg index="0"><null/></constructor-arg>
        </xsl:otherwise>
      </xsl:choose>
      <constructor-arg index="1" value="{@key}"/>
      <constructor-arg index="2" value="{i:local_ae_descriptor/@file_path}"/>
      <constructor-arg index="3" ref="casManager"/>
      <constructor-arg index="4" ref="inProcessCache"/>
      <constructor-arg index="5" ref="{$delegateMapID}"/>
      
      <!-- output channels needed if top level component - to return values to invoker
           or if one or more delegates is remote - to send to the remote -->
      <xsl:if test="parent::u:service or u:delegates/u:remoteAnalysisEngine">
        <property name="outputChannel" ref="{f:getOutputChannelID(.)}"/>
      </xsl:if>  
      <property name="serviceEndpointName" value="{$input_q_ID}"/>
      
      <property name="controllerBeanName" value="{$ctlrID}"/>
      <property name="errorHandlerChain" ref="{f:getErrorHandlerChainID(.)}"/>
      <property name="flowControllerDescriptor" value="{$flowControllerPath}"/>
    </bean>
  
    <!-- only generate aggregate input message handlers for top level - 
         others using internal queuing mechanisms -->
    <xsl:if test="parent::u:service">  
      <xsl:call-template name="generateMsgHandlerChain">
        <xsl:with-param name="aeNode" select="."/>
        <xsl:with-param name="kind" select="'aggregate_input'"/>
      </xsl:call-template>
    </xsl:if>
    
    <!-- only generate return message handlers if one or more of the 
         delegates are remote 
         All remotes share this one message handler chain -->
    
    <xsl:if test="u:delegates/u:remoteAnalysisEngine">
      <xsl:call-template name="generateMsgHandlerChain">
        <xsl:with-param name="aeNode" select="."/>
        <xsl:with-param name="kind" select="'aggregate_return'"/>
      </xsl:call-template>
    </xsl:if>
    
    <xsl:sequence
      select="f:generateLineComment('Create the endpoints + output channels, one per delegate', 3)"/>
    
    <xsl:variable name="remoteDelegates"
      select="u:delegates/u:remoteAnalysisEngine"/>
    <xsl:variable name="analysisEngine" select="."/>
    
    
    <!-- iterate over AE descriptor to get accurate position info for all delegates -->
    <xsl:for-each
      select="$AEdescription/u:delegateAnalysisEngineSpecifiers/u:delegateAnalysisEngine">
      
      
      <!-- these next 2 vars are the same, except the remoteAnalysisEngine is not-empty if it is remote -->
      <xsl:variable name="remoteAnalysisEngine"
        select="$remoteDelegates[@key eq current()/@key]"/>
      <xsl:variable name="aeDelegate"
        select="$analysisEngine/u:delegates/*[@key eq current()/@key]"/>
      <!--xsl:message select="('*** key is', @key)"/>
      <xsl:message select="$AEdescription/u:delegateAnalysisEngineSpecifiers/u:delegateAnalysisEngine"/>
      <xsl:message select="$analysisEngine/u:delegates"/>
      <xsl:message select="'*** remote delegate'"/>
      <xsl:message select="$remoteAnalysisEngine"/>
      <xsl:message select="'*** ae delegate'"/>
      <xsl:message select="$aeDelegate"/-->
      
      <xsl:sequence
        select="f:generateLineComment((
        '====================================',
        @key,
        '  ** Delegate Endpoint **',
        '===================================='
        ), 3)"/>
      <xsl:variable name="endpointID"
        select="f:getEndpointName(@key, concat($uniq, '.', position()))"/>
      <bean id="{$endpointID}"
        class="org.apache.uima.aae.controller.Endpoint_impl">
        
        <xsl:sequence select="f:generateLineComment('Broker URI', 5)"/>
        <xsl:variable name="brokerURL">
          <xsl:choose>
            <xsl:when test="$remoteAnalysisEngine">
              <xsl:sequence
                select="string($remoteAnalysisEngine/u:inputQueue/@brokerURL)"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:sequence select="'vm://localhost'"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:variable>
        <property name="serverURI" value="{$brokerURL}"/>
        
        <xsl:sequence
          select="f:generateLineComment('Delegate key name or remote queue name', 5)"/>
        <property name="endpoint"
          value="{if ($remoteAnalysisEngine) then $remoteAnalysisEngine/u:inputQueue/@endpoint
                                             else f:getInternalInputQueueName($aeDelegate)}"/>
        
        <xsl:if test="$remoteAnalysisEngine">
          <xsl:sequence
            select="f:generateLineComment('Queue name used for replies, on the remote broker',5)"/>
          <property name="replyToEndpoint"
            value="{f:getRemoteReturnQueueName($analysisEngine, $remoteAnalysisEngine)}"/>
        </xsl:if>
        
        <!--xsl:message select="'aeDelegates'"/>        
        <xsl:message select="count($aeDelegate/ancestor-or-self::node()
        [u:errorConfiguration/u:errorHandling/u:timeout[@event eq 'metadataRequest']][1]
        /u:errorConfiguration/u:errorHandling/u:timeout[@event eq 'metadataRequest']
        )
        "/> 
        <xsl:message select="'aeDelegates'"/-->
        
        <xsl:if test="$remoteAnalysisEngine/u:casMultiplier/@poolSize">
          <property name="shadowCasPoolSize" value="{$remoteAnalysisEngine/u:casMultiplier/@poolSize}"/>
        </xsl:if> 
        <xsl:if test="$remoteAnalysisEngine/u:casMultiplier/@initialFsHeapSize"> 
          <property name="initialFsHeapSize" value="{$remoteAnalysisEngine/u:casMultiplier/@initialFsHeapSize}"/>
        </xsl:if>
        
        <!-- jira UIMA-1245 -->
        <xsl:if test="$aeDelegate/u:casMultiplier/@processParentLast">
          <property name="processParentLast" value="{$aeDelegate/u:casMultiplier/@processParentLast}"/>
        </xsl:if>

        <!-- jira UIMA-2735 -->
        <xsl:if test="$aeDelegate/u:casMultiplier/@disableJCasCache">
          <property name="disableJCasCache" value="{$aeDelegate/u:casMultiplier/@disableJCasCache}"/>
        </xsl:if>
        
        
        <xsl:sequence select="f:generateLineComment('Timeouts', 5)"/>
        <property name="metadataRequestTimeout"
          value="{$aeDelegate/ancestor-or-self::node()
                [u:asyncAggregateErrorConfiguration/u:getMetadataErrors/@timeout][1]
                /u:asyncAggregateErrorConfiguration/u:getMetadataErrors/@timeout}"/>
        <property name="processRequestTimeout"
          value="{$aeDelegate/ancestor-or-self::node()
                [u:asyncAggregateErrorConfiguration/u:processCasErrors/@timeout][1]
                /u:asyncAggregateErrorConfiguration/u:processCasErrors/@timeout}"/>
        <property name="collectionProcessCompleteTimeout"
          value="{$aeDelegate/ancestor-or-self::node()
                [u:asyncAggregateErrorConfiguration/u:collectionProcessCompleteErrors/@timeout][1]
                /u:asyncAggregateErrorConfiguration/u:collectionProcessCompleteErrors/@timeout}"/>
        <xsl:if test="$remoteAnalysisEngine">
          <property name="serializer"
            value="{$remoteAnalysisEngine/u:serializer/@method}"/>
        </xsl:if>
        
        <xsl:if test="$remoteAnalysisEngine">
          <property name="tempReplyDestination" value="true"/>
        </xsl:if>
        
        <xsl:if test="not($remoteAnalysisEngine)">
          <xsl:choose>
            <xsl:when test="$aeDelegate[@async eq 'true']">
              <property name="concurrentRequestConsumers" value="{$aeDelegate/@inputQueueScaleout}"/>    
            </xsl:when>
            <xsl:otherwise>
              <property name="concurrentRequestConsumers" value="{$aeDelegate/u:scaleout/@numberOfInstances}"/>
            </xsl:otherwise>
          </xsl:choose> 
          
          <property name="concurrentReplyConsumers" value="{$aeDelegate/@internalReplyQueueScaleout}"/>
        </xsl:if>
        <!--
          <xsl:variable name="msgListenerContainerID" 
             select="concat('asAggr_return_msgLsnrCntnr_', $aeNameUnique,
             if ($remoteAnalysisEngine/@key) then concat('_', $remoteAnalysisEngine/@key) else '')"/> 
          <property name="listenerContainer" ref="{$msgListenerContainerID}"/>
        </xsl:if-->
      </bean>
      
     
    </xsl:for-each>
         
    <!-- only generate input channels (and listeners) for top level - 
         others using internal queuing mechanisms -->
    <xsl:if test="parent::u:service"> 
      <xsl:call-template name="generateInputChannel">
        <xsl:with-param name="aeNameUnique" select="$aeNameUnique"/>
        <xsl:with-param name="q_ID" select="$input_q_ID"/>
        <xsl:with-param name="q_endpointName" select="$input_q_ID"/>
        <xsl:with-param name="queueFactoryID" select="$inputQueueFactoryID"/>
        <xsl:with-param name="inputOrReturn" select="'input'"/>
        <xsl:with-param name="kind" select="'asAggr'"/>
        <!-- used as 1st part of ctlr name -->
        <xsl:with-param name="msgHandlerChainID"
          select="f:getMetaMsgHandlerID(., 'aggregate_input')"/>
        <xsl:with-param name="nbrConcurrentConsumers" select="string(@inputQueueScaleout)"/>
        <xsl:with-param name="remote" select="()"/>
        <xsl:with-param name="poolingTaskExecutor" select="()"/>
      </xsl:call-template>
    </xsl:if>
    
    <xsl:variable name="return_q_ID" select="f:getLocalReturnQueueEndpointID(.)"/>
    
    <!-- this next test seems strange, I think it always returns true.
      the test "starts-with(@brokerURL, '/')" would seem always to be false 
      The net effect is that the return queue for async aggr is always generated -->
    
    <!-- test changed: no common return input channel and listeners are used
         anymore - they were used only for co-located delegates, which now
         use an internal mechanism -->
    
    
      <!-- removed with change to JavaQ for local Q -->
      <!-- other reply queues (for remotes) are "remote" -->     
      <!--xsl:sequence
        select="f:generateLineComment('return queue for async aggregate', 3)"/>
      <bean id="{$return_q_ID}" class="org.apache.activemq.command.ActiveMQQueue">
        <constructor-arg index="0" value="{$return_q_ID}"/>
      </bean-->
      
      <!--xsl:call-template name="generateInputChannel">
        <xsl:with-param name="aeNameUnique" select="$aeNameUnique"/>
        <xsl:with-param name="q_ID" select="$return_q_ID"/>
        <xsl:with-param name="q_endpointName" select="$return_q_ID"/>        
        <xsl:with-param name="queueFactoryID" select="'controllerJmsFactory'"/>
        <xsl:with-param name="inputOrReturn" select="'return'"/>
        <xsl:with-param name="kind" select="'asAggr'"/-->
        <!-- used as 1st part of ctlr name -->
        <!--xsl:with-param name="msgHandlerChainID"
          select="f:getMetaMsgHandlerID($analysisEngine, 'aggregate_return')"/>
        <xsl:with-param name="nbrConcurrentConsumers" select="string(@internalReplyQueueScaleout)"/>
        <xsl:with-param name="remote" select="()"/>
        <xsl:with-param name="poolingTaskExecutor" select="()"/>
      </xsl:call-template-->
   
    
    <!-- we iterate over all the delegates in order to have the right value for position() -->
    <xsl:for-each select="u:delegates/*">
      <xsl:if test="self::u:remoteAnalysisEngine">
        <!--xsl:message select="('delegate', .)"/-->
        <xsl:variable name="returnQ_ID" select="f:getRemoteReturnQueueID($analysisEngine, .)"/>
        <xsl:variable name="returnQ_ID_GUID" select="f:getRemoteReturnQueueName($analysisEngine, .)"/>
        
        <!--xsl:if test="not(f:isRmtTempQ(.))"-->
          <xsl:sequence
            select="f:generateLineComment(('return queue for http or tcp remote service', 'on remote broker'), 3)"/>
          <bean id="{$returnQ_ID}" class="org.apache.activemq.command.ActiveMQQueue">
            <constructor-arg index="0" value="{$returnQ_ID_GUID}"/>
          </bean>
        <!--/xsl:if-->
        
        <xsl:variable name="brokerID" select="f:getQbrokerID(u:inputQueue)"/>
        <!--xsl:message select="('generating input channel for remote reply queue', u:remoteAnalysisEngine/@key)"/-->
        <xsl:call-template name="generateInputChannel">
          <xsl:with-param name="aeNameUnique" select="$aeNameUnique"/>
          <xsl:with-param name="q_ID" select="$returnQ_ID"/>
          <!-- if tempQ, use endpoint bean id instead of endpoint name in next parm -->
          <!--xsl:with-param name="q_endpointName"
            select="if (f:isRmtTempQ(.)) then f:getEndpointName(@key, concat($uniq, '.', position())) 
                             else $returnQ_ID_GUID"/-->
          <xsl:with-param name="q_endpointName"
            select="f:getEndpointName(@key, concat($uniq, '.', position()))"/>
          <xsl:with-param name="queueFactoryID" select="$brokerID"/>
          <xsl:with-param name="inputOrReturn" select="'return'"/>
          <xsl:with-param name="kind" select="'asAggr'"/>
          <!-- used as 1st part of ctlr name -->
          <xsl:with-param name="msgHandlerChainID"
            select="f:getProcessResponseHandlerID($analysisEngine, 'aggregate_return')"/>
          <xsl:with-param name="nbrConcurrentConsumers" 
            select="@remoteReplyQueueScaleout"/>  
          <xsl:with-param name="remote" select="."/>
          <xsl:with-param name="poolingTaskExecutor" select="()"/>
        </xsl:call-template>
      </xsl:if>
    </xsl:for-each>
    
    <xsl:variable name="origin_q_name"
      select="if (parent::u:service) then parent::u:service/u:inputQueue/@endpoint
                              else f:getInternalInputQueueName(.)"/>
    <!--xsl:message select="'*** origin_q_name: '"/>
    <xsl:message select="$origin_q_name"/-->
 
    <!-- skip generating output channel for internal reply queues 
         because internal reply queues no longer used 
         Still need this for returning values from top level component to invoker, or
         for sending value to remotes -->
    <xsl:if test="parent::u:service or u:delegates/u:remoteAnalysisEngine">       
      <xsl:call-template name="generateOutputChannel">
        <xsl:with-param name="aeNode" select="."/>
        <xsl:with-param name="origin_q_name" select="$origin_q_name"/>
        <xsl:with-param name="return_q_ID" select="$return_q_ID"/>
        <xsl:with-param name="kind" select="'asAggr'"/>
      </xsl:call-template>
    </xsl:if>
    
    <xsl:sequence select="f:generateLineComment('map for delegate keys', 3)"/>
    <bean id="{$delegateMapID}" class="java.util.HashMap" singleton="true">
      <constructor-arg>
        <map>
          <xsl:for-each
            select="$AEdescription/u:delegateAnalysisEngineSpecifiers/u:delegateAnalysisEngine">
            <entry key="{@key}">
              <ref
                bean="{f:getEndpointName(@key, concat($uniq, '.', position()))}"/>
            </entry>
          </xsl:for-each>
        </map>
      </constructor-arg>
    </bean>
    <xsl:sequence select="$nl2"/>
    
    <xsl:call-template name="generateErrorChainForAggr"/>
    <xsl:apply-templates select="u:delegates"/>
  </xsl:template>
  
  
  <!--============================================================-->       
  <!--=  Primitive or Sync Aggregate Analysis Engine (async=false)  =-->       
  <!--============================================================--> 
  <xsl:template  match="u:analysisEngine[@async=('false', 'no')]">
    <xsl:param tunnel="yes" name="input_q_ID"/>
    <xsl:param tunnel="yes" name="inputQueueFactoryID"/>
    <xsl:variable name="AEdescription" select="i:local_ae_descriptor/(u:analysisEngineDescription|u:casConsumerDescription|u:taeDescription)"/>
    
    <xsl:sequence select="f:generateBlockComment(
        (concat('Primitive or non-Async Aggr: ', f:getSimpleAeName(.))         
        ),3)"/>
       
    <xsl:variable name="uniq" select="f:getUnique(.)"/>

    <xsl:variable name="aeNameUnique" select="f:getAeNameUnique(.)"/>
    <xsl:variable name="ctlrID"   select="f:getControllerID(.)"/>
    
    <xsl:if test="self::u:analysisEngine"> <!-- ?? seems to always be true ... -->
          <!-- no error handler chain for primitives - use built-in one instead -->
      <!-- xsl:variable name="errorHandlerChainID" select="f:getErrorHandlerChainID(.)"/ --> 
      <xsl:choose>
        <xsl:when test="../../u:service[@i:isTopLvlCpp eq 'yes']">
          <!-- create bean to do placeholder substitution Jira UIMA-1288 -->
          <bean id="placeholderSubstitution" class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
            <property name="systemPropertiesModeName" value="SYSTEM_PROPERTIES_MODE_OVERRIDE"/>  
          </bean>
          <bean id="{$ctlrID}"
            class="org.apache.uima.aae.controller.UimacppServiceController">
            
            <xsl:sequence select="f:generateLineComment('path to top level descriptor', 5)"/>
            <constructor-arg index="0" value="{i:local_ae_descriptor/@file_path}"/>
            
            <xsl:sequence select="f:generateLineComment('input q name', 5)"/>
            <constructor-arg index="1" value="{../u:inputQueue/@endpoint}"/>
            
            <xsl:sequence select="f:generateLineComment('input q BrokerURL', 5)"/>
            <constructor-arg index="2" value="{../u:inputQueue/@brokerURL}"/>

            <xsl:sequence select="f:generateLineComment('scaleout number of instances', 5)"/>
            <constructor-arg index="3" value="{u:scaleout/@numberOfInstances}"/>
            
            <xsl:sequence select="f:generateLineComment('prefetch limit', 5)"/>
            <constructor-arg index="4" value="{../u:inputQueue/@prefetch}"/>

            <xsl:sequence select="f:generateLineComment('Env Vars map', 5)"/>
            <constructor-arg index="5">
              <map>
                <xsl:for-each select="../u:environmentVariables/u:environmentVariable">
                  <entry key="{@name}"><value><xsl:sequence select="text()"/></value></entry> 
                </xsl:for-each>
              </map>
            </constructor-arg>
            
           <xsl:sequence select="f:generateLineComment('Process CAS Error Threshold', 5)"/>
           <constructor-arg index="6" value="{u:asyncPrimitiveErrorConfiguration/u:processCasErrors/@thresholdCount}"/>
            
           <xsl:sequence select="f:generateLineComment('PprocessCas Error Window', 5)"/>
           <constructor-arg index="7" value="{u:asyncPrimitiveErrorConfiguration/u:processCasErrors/@thresholdWindow}"/>
            
           <xsl:sequence select="f:generateLineComment('Terminate on CPC Error', 5)"/>
           <constructor-arg index="8" value="{if ('terminate' eq 
             string(u:asyncPrimitiveErrorConfiguration/u:collectionProcessCompleteErrors/@additionalErrorAction))
             then 'true' else 'false'}"/>

           <xsl:variable name="initialHeapSize" select="if (../../u:casPool/@initialFsHeapSize eq 'defaultFsHeapSize') 
             then '40000' else ../../u:casPool/@initialFsHeapSize"/>
           <constructor-arg index="9" value="{$initialHeapSize}"/>           
            
          </bean>
        </xsl:when>
        <xsl:otherwise>  
          <!-- removed lazy init: lazy-init="{if (parent::u:service) then 'true' else 'false'}" -->   
          <bean id="{$ctlrID}"
            class="org.apache.uima.aae.controller.PrimitiveAnalysisEngineController_impl"
            init-method="initialize">
            <!-- parent controller -->
            <xsl:choose>
              <xsl:when test="parent::u:delegates">
                <constructor-arg index="0" ref="{f:getControllerID(ancestor::u:analysisEngine[1])}"/>
              </xsl:when>
              <xsl:otherwise>
                <constructor-arg index="0"><null/></constructor-arg>
              </xsl:otherwise>
            </xsl:choose>
            <xsl:sequence select="f:generateLineComment('Key name', 5)"/>
            <constructor-arg index="1" value="{@key}"/>
            <constructor-arg index="2" value="{i:local_ae_descriptor/@file_path}"/>
            <constructor-arg index="3" ref="casManager"/>
            <constructor-arg index="4" ref="inProcessCache"/>        
            
            <!-- This parameter is ignored -->
            <xsl:sequence select="f:generateLineComment('this parameter is ignored', 5)"/>
            <constructor-arg index="5" value="10"/>
            <!--xsl:message select="'*** Scaleout'"/>
            <xsl:message select="."/-->
            <xsl:sequence select="f:generateLineComment('scaleout number of instances', 5)"/>
            <constructor-arg index="6" value="{u:scaleout/@numberOfInstances}"/>
            <xsl:if test="u:casMultiplier/@poolSize">
              <xsl:sequence select="f:generateLineComment('CAS Multiplier poolSize', 5)"/>
              <constructor-arg index="7" value="{u:casMultiplier/@poolSize}"/>
            </xsl:if>
            <xsl:if test="u:casMultiplier/@initialFsHeapSize">
              <xsl:sequence select="f:generateLineComment('CAS Multiplier initial heap size', 5)"/>
              <constructor-arg index="8" value="{u:casMultiplier/@initialFsHeapSize}"/>
            </xsl:if>
            
            <!-- https://issues.apache.org/jira/browse/UIMA-2735 -->
            <xsl:if test="u:casMultiplier/@disableJCasCache">
              <xsl:sequence select="f:generateLineComment('CAS Multiplier disableJcasCache', 5)"/>
              <constructor-arg index="9" value="{u:casMultiplier/@disableJCasCache}" />
            </xsl:if>
            
            <xsl:if test="parent::u:service">
              <property name="outputChannel" ref="{f:getOutputChannelID(.)}"/>
            </xsl:if>  
            <xsl:if test="parent::u:service">
              <property name="errorHandlerChain" ref="{f:getErrorHandlerChainID(.)}"/>         
            </xsl:if>
          </bean>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:if>
    
      <!-- next logic is true for lower levels in an aggregate as well as top level not CPP -->
    <xsl:if test="not(../../u:service[@i:isTopLvlCpp eq 'yes'])">    
      <xsl:variable name="poolingTaskExecutorID" select="f:getPoolingTaskExecutorID(.)"/>
      <!--xsl:variable name="scaleoutPlus" select=
        "if (parent::u:service and u:casMultiplier) then
           number(u:scaleout/@numberOfInstances) + 1 else
           number(u:scaleout/@numberOfInstances) + 0"/-->
  
      <!-- removed when java q added for local  -->
      <!-- only generate ThreadPoolTaskExecutor for top level primitives -->
      <xsl:if test="parent::u:service">            
        <xsl:sequence select="f:generateLineComment('ThreadPool Task Executor',3)"/>    
        <bean id="{$poolingTaskExecutorID}" class="org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor">
          <property name="corePoolSize" value="{u:scaleout/@numberOfInstances}" />
          <property name="maxPoolSize" value="{u:scaleout/@numberOfInstances}" />
          <property name="queueCapacity" value="{u:scaleout/@numberOfInstances}" />
        </bean>
      </xsl:if>
     
      <!-- only generate input message handlers and input channels for top level - 
           others using internal queuing mechanisms -->
      <xsl:if test="parent::u:service">       
        <xsl:call-template name="generateMsgHandlerChain">
          <xsl:with-param name="aeNode" select="."/>
          <xsl:with-param name="kind" select="'primitive'"/>
        </xsl:call-template>

     
        <xsl:call-template name="generateInputChannel">
          <xsl:with-param name="aeNameUnique" select="$aeNameUnique"/> 
          <xsl:with-param name="q_ID" select="$input_q_ID"/>
          <xsl:with-param name="q_endpointName" select="$input_q_ID"/>      
          <xsl:with-param name="queueFactoryID" select="$inputQueueFactoryID"/>
          <xsl:with-param name="inputOrReturn" select="'input'"/>
          <xsl:with-param name="kind" select="'primitive'"/>  <!-- used in ctrl id name -->
          <xsl:with-param name="msgHandlerChainID" select="f:getMetaMsgHandlerID(., 'primitive')"/>
          <xsl:with-param name="nbrConcurrentConsumers" select="u:scaleout/@numberOfInstances"/> 
          <xsl:with-param name="remote" select="()"/>
          <xsl:with-param name="poolingTaskExecutor" select="$poolingTaskExecutorID"/>     
        </xsl:call-template>
 
      </xsl:if>    
      
      <!-- next to be commented out due to design change summer 2008 -->
      <!--xsl:if test="parent::u:service and u:casMultiplier">
        <xsl:call-template name="generateCMSyncInputChannel">
          <xsl:with-param name="aeNameUnique" select="$aeNameUnique"/> 
          <xsl:with-param name="q_ID" select="$input_q_ID"/>
          <xsl:with-param name="queueFactoryID" select="$inputQueueFactoryID"/>
          <xsl:with-param name="inputOrReturn" select="'input'"/>
          <xsl:with-param name="kind" select="'primitive'"/-->  <!-- used in ctrl id name -->
          <!--xsl:with-param name="msgHandlerChainID" select="f:getMetaMsgHandlerID(., 'primitive')"/>
          <xsl:with-param name="nbrConcurrentConsumers" select="u:scaleout/@numberOfInstances"/> 
          <xsl:with-param name="remote" select="()"/>
          <xsl:with-param name="poolingTaskExecutor" select="$poolingTaskExecutorID"/>       
        </xsl:call-template>      
      </xsl:if-->
      
      
      <xsl:variable name="origin_q_name" select=
        "if (parent::u:service) then parent::u:service/u:inputQueue/@endpoint
                                else f:getInternalInputQueueName(.)"/>
      <!--xsl:message select="'*** origin_q_name: '"/>
      <xsl:message select="$origin_q_name"/-->

      <!-- skip generating output channel for internal reply queues 
           because internal reply queues no longer used -->
      <xsl:if test="parent::u:service">                  
        <xsl:call-template name="generateOutputChannel">
          <xsl:with-param name="aeNode" select="."/>
          <xsl:with-param name="origin_q_name" select="$origin_q_name"/>
          <xsl:with-param name="kind" select="'primitive'"/>
        </xsl:call-template>
      </xsl:if>
      
      <!-- next call is called for all primitives, but only does something
           for top level primitives -->
      <xsl:call-template name="generateErrorChainForTop"/>
    </xsl:if>
      
  </xsl:template>

  <!--============================================================-->       
  <!--=  Remote Analysis Engine                                  =-->       
  <!--============================================================--> 
  <xsl:template  match="u:remoteAnalysisEngine">
    <xsl:param tunnel="yes" name="input_q_ID"/>
    <xsl:param tunnel="yes" name="inputQueueFactoryID"/>       
  </xsl:template>
         
  <!--======================================-->       
  <!--=  generateErrorChainForAggr         =-->
  <!--=    Context = aggr Analysis Engine  =-->
  <!--=              containing delegates  =-->       
  <!--======================================--> 
  <xsl:template name="generateErrorChainForAggr">
    <xsl:variable name="aec" select="u:delegates/(u:analysisEngine | u:remoteAnalysisEngine)/u:asyncAggregateErrorConfiguration"/>

    <xsl:sequence select="f:generateBlockComment('Delegate ErrorHandlers', 3)"/>
    <xsl:call-template name="generateErrorHandler">
      <xsl:with-param name="node" select="$aec/u:getMetadataErrors"/>
      <xsl:with-param name="kind" select="'GetMetaErrorHandler'"/> 
    </xsl:call-template>  
    
    <xsl:call-template name="generateErrorHandler">
      <xsl:with-param name="node" select="$aec/u:processCasErrors"/>
      <xsl:with-param name="kind" select="'ProcessCasErrorHandler'"/> 
    </xsl:call-template>  

    <xsl:call-template name="generateErrorHandler">
      <xsl:with-param name="node" select="$aec/u:collectionProcessCompleteErrors"/>
      <xsl:with-param name="kind" select="'CpcErrorHandler'"/> 
    </xsl:call-template>  
        
    <xsl:call-template name="generateErrorHandlerChain">
      <xsl:with-param name="aec" select="$aec"/>
    </xsl:call-template>      
    
  </xsl:template>

  <xsl:template name="generateErrorHandlerChain">
    <xsl:param name="aec"/>
    <xsl:sequence select="f:generateBlockComment((
        ' Error Handler Chain '        
        ), 3)"/>
    <bean id="{f:getErrorHandlerChainID(.)}"
          class="org.apache.uima.aae.error.ErrorHandlerChain">
      <constructor-arg>
        <list>
            
          <xsl:if test="$aec/u:getMetadataErrors">
            <ref local="{f:getErrorHandlerID(($aec/u:getMetadataErrors)[1])}"/>
          </xsl:if>
            
          <xsl:if test="$aec/u:processCasErrors">
            <ref local="{f:getErrorHandlerID(($aec/u:processCasErrors)[1])}"/>
          </xsl:if>
            
          <xsl:if test="$aec/u:collectionProcessCompleteErrors">
            <ref local="{f:getErrorHandlerID(($aec/u:collectionProcessCompleteErrors)[1])}"/>
          </xsl:if>
            
         </list>
      </constructor-arg>
    </bean>
  </xsl:template>
  
  <!--======================================-->       
  <!--=  generateErrorChainForTop          =-->
  <!--=    Context = top  Analysis Engine  =-->
  <!--=  if Top is async aggr, the config  =-->
  <!--=    is done within the error chain  =-->
  <!--=    for that aggr.                  =-->
  <!--=  else is done here                 =-->
  <!--=    context-node is analysisEngine  =-->       
  <!--======================================-->
  
  <xsl:template name="generateErrorChainForTop">
    <xsl:variable name="aec" select="u:asyncPrimitiveErrorConfiguration"/>
    <xsl:if test="@async eq 'false' and ($aec/u:processCasErrors or $aec/u:collectionProcessCompleteErrors)"> 
      <!--xsl:message select="'*generating err handler chain'"/-->   
      <xsl:if test="$aec/u:processCasErrors">
        <bean id="{f:getErrorHandlerID(($aec/u:processCasErrors)[1])}" class="org.apache.uima.aae.error.handler.ProcessCasErrorHandler">
          <constructor-arg>
            <map> <entry key=""> <ref bean="{f:getErrorConfigDetailID($aec/u:processCasErrors)}"/> </entry> </map>
          </constructor-arg>
        </bean>
      </xsl:if>      
      
      <xsl:if test="$aec/u:collectionProcessCompleteErrors">
        <bean id="{f:getErrorHandlerID(($aec/u:collectionProcessCompleteErrors)[1])}" class="org.apache.uima.aae.error.handler.CpcErrorHandler">
          <constructor-arg>
            <map> <entry key=""> <ref bean="{f:getErrorConfigDetailID($aec/u:collectionProcessCompleteErrors)}"/> </entry> </map>
          </constructor-arg>
        </bean>
      </xsl:if>      
      
      <xsl:call-template name="generateErrorHandlerChain">
        <xsl:with-param name="aec" select="$aec"/>
      </xsl:call-template>      

    </xsl:if>
  </xsl:template>


  <xsl:template name="generateErrorHandler">
    <xsl:param name="node"/>
    <xsl:param name="kind"/>
    <!--xsl:message select="('*** Generating keymaps for error handler given node set:', $node, '***end')"/-->
    <xsl:if test="$node">
      <bean id="{f:getErrorHandlerID($node[1])}" class="org.apache.uima.aae.error.handler.{$kind}">
        <constructor-arg>
          <map>
            <xsl:for-each select="$node">
              <entry key="{../../@key}"> <ref bean="{f:getErrorConfigDetailID(.)}"/> </entry> 
            </xsl:for-each>
            <!-- aec <- asyncAggrErrConfig <- ae or rmtae <- delegate <- ae <- service -->
            <xsl:variable name="topEc" select=
                "$node[1]/../../../parent::u:analysisEngine[parent::u:service]/u:asyncPrimitiveErrorConfiguration"/>
            <!-- ec is error spec matching the error class being worked on -->
            <xsl:variable name="ec" select="$topEc/node()[node-name(.) eq node-name($node[1])]"/>
            <xsl:if test="$ec">
              <!--xsl:message select="('*** found top level - inserting null key ref')"/-->
              <entry key=""> <ref bean="{f:getErrorConfigDetailID($ec)}"/> </entry>
            </xsl:if>
          </map>
        </constructor-arg>
      </bean>
      <xsl:sequence select="$nl2"/>
    </xsl:if>
  </xsl:template>

  <!-- errorKindNode is the <processCasErrors> etc. node  -->

  <xsl:function name="f:getErrorHandlerID">
    <xsl:param name="errorKindNode"/>
    <xsl:variable name="en" select="string(node-name($errorKindNode))"/>
    <!-- next substring drops u: in front and Errors in the back -->
    <xsl:variable name="kind" select="substring($en, 3, string-length($en)-6)"/>   
    <!-- if $errorKindNode is contained in an AS aggregate, go up to that aggregate -->
    <!-- if $errorKindNode is at the top level, skip the uniqifier -->
   
    <xsl:variable name="uniq" select="if ($errorKindNode/../../parent::u:service) then '' else f:getUnique($errorKindNode/../../..)"/>
    <xsl:sequence select="concat('err_hndlr_',$kind,'_',$uniq)"/>
  </xsl:function>              
              
    
  <!--==========================================-->
  <!--   Generate an input or return Channel    -->
  <!--==========================================-->
  <xsl:template name="generateInputChannel">
    <xsl:param name="aeNameUnique"/>
    <xsl:param name="q_ID"/>   <!-- this is a descriptor-unique bean id of the input (or ret) q endpoint -->
    <xsl:param name="q_endpointName"/>  <!-- for temp return q this is endpoint bean id -->
    <xsl:param name="queueFactoryID"/>
    <xsl:param name="inputOrReturn"/> <!-- input or return -->
    <xsl:param name="kind"/> <!-- either primitive or asAggr, used as 1st part of ctlr name -->
    <xsl:param name="msgHandlerChainID"/>
    <xsl:param name="nbrConcurrentConsumers"/>
    <xsl:param name="remote"/>  <!-- is () unless a remote return queue -->
    <xsl:param name="poolingTaskExecutor"/>
    
    <xsl:variable name="ctlrID"   select="concat($kind, '_ctlr_', $aeNameUnique)"/>    
    <xsl:variable name="q_listenerID" select="concat($kind, '_', $inputOrReturn, '_q_listenerID_',$aeNameUnique,
             if ($remote) then concat('_', $remote/@key) else '')"/>    
    <xsl:variable name="msgListenerContainerID" 
      select="concat($kind, '_', $inputOrReturn, '_msgLsnrCntnr_', $aeNameUnique,
             if ($remote) then concat('_', $remote/@key) else '')"/>    

  
    <xsl:sequence select="f:generateLineComment((
      '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',
 concat('JMS msg listener for ', $inputOrReturn, ' queue for:'),
       $q_ID,
      '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'),3)"/>
    <bean id="{$q_listenerID}"
        class="org.apache.uima.adapter.jms.activemq.JmsInputChannel">
      <property name="messageHandler" ref="{$msgHandlerChainID}"/>
      <!--property name="endpointName" value="{if (f:isRmtTempQ($remote)) then '' else $q_endpointName}"/-->
      <property name="endpointName" value="{if ($remote[self::u:remoteAnalysisEngine]) then '' else $q_endpointName}"/>
      <property name="controller" ref="{$ctlrID}"/>
      <xsl:if test="$inputOrReturn eq 'input'">
        <property name="listenerContainer" ref="{$msgListenerContainerID}"/>
      </xsl:if>
    </bean>

      
    <xsl:sequence select="f:generateLineComment((
      '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',
   concat('JMS msg listener container for ', $inputOrReturn, ' queue for:'),
       $q_ID,
      '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'),3)"/>    
    <bean id="{$msgListenerContainerID}"
      class="org.apache.uima.adapter.jms.activemq.UimaDefaultMessageListenerContainer">

      <xsl:if test="$poolingTaskExecutor">
        <xsl:sequence select="f:generateLineComment('Connect to pooling task executor for primitive',5)"/>
        <property name="taskExecutor" ref="{$poolingTaskExecutor}"/>
      </xsl:if>

      <xsl:sequence select="f:generateLineComment('Define number of JMS Consumers',5)"/>
      <property name="concurrentConsumers" value="{$nbrConcurrentConsumers}"/>
      
      <xsl:sequence select="f:generateLineComment(concat($inputOrReturn, ' Queue'), 5)"/>
      <xsl:choose>
        <xsl:when test="$remote[self::u:remoteAnalysisEngine]">
          <property name="destinationResolver" ref="{f:getDestinationResolverID($aeNameUnique, $remote/@key)}"/>
          <property name="destinationName" value="" />
          <property name="targetEndpoint" ref="{$q_endpointName}" />
          <xsl:sequence select="f:generateLineComment('POJO to delegate JMS Messages to', 5)"/>
          <property name="messageListener" ref="{$q_listenerID}"/>
          <property name="connectionFactory" ref="{$queueFactoryID}-reply"/>
        </xsl:when>
        <xsl:otherwise>
          <property name="destination" ref="{$q_ID}"/>      
          <xsl:sequence select="f:generateLineComment('POJO to delegate JMS Messages to', 5)"/>
          <property name="messageListener" ref="{$q_listenerID}"/>
          <property name="connectionFactory" ref="{$queueFactoryID}"/>
        </xsl:otherwise>
      </xsl:choose>
           
      <xsl:if test="$inputOrReturn eq 'input'">
        <property name="messageSelector" value="Command=2000 OR Command=2002" /> <!-- Process or CPC request -->
      </xsl:if>
    </bean>
     
    <xsl:if test="$remote[self::u:remoteAnalysisEngine]">
      <xsl:sequence select="f:generateLineComment((
        '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',
        concat('Destination Resolver for ',$remote/@key),
         $q_ID,
        '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'),3)"/>    
      <bean id="{f:getDestinationResolverID($aeNameUnique, $remote/@key)}"
        class="org.apache.uima.adapter.jms.activemq.TempDestinationResolver"
        singleton="false">
        <property name="connectionFactory" ref="{$queueFactoryID}-reply"/>
      </bean>
    </xsl:if>

    <xsl:if test="$inputOrReturn eq 'input'">
      <xsl:sequence select="f:generateLineComment((
        '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',
     concat('GetMeta JMS msg listener container for ', $inputOrReturn, ' queue for:'),
         $q_ID,
        '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'),3)"/>    
      <bean id="{concat($msgListenerContainerID, '_getMeta')}"
        class="org.apache.uima.adapter.jms.activemq.UimaDefaultMessageListenerContainer">
  
        <!-- next commented out to have this bean use a different pool than the other one
             because the other one has each thread "pinned" to a particular instance of
             an analysis engine (so the ae always runs on the same thread, as required by
             some impls (like python?) -->
        <!--xsl:if test="$poolingTaskExecutor">
          <xsl:sequence select="f:generateLineComment('Connect to pooling task executor for primitive',5)"/>
          <property name="taskExecutor" ref="{$poolingTaskExecutor}"/>
        </xsl:if-->
  
        <xsl:sequence select="f:generateLineComment('Define number of JMS Consumers',5)"/>
        <property name="concurrentConsumers" value="1"/>
        
        <xsl:sequence select="f:generateLineComment(concat($inputOrReturn, ' Queue'), 5)"/>
        <property name="destination" ref="{$q_ID}"/>
             
        <xsl:sequence select="f:generateLineComment('POJO to delegate JMS Messages to', 5)"/>
        <property name="messageListener" ref="{$q_listenerID}"/>
        
        <property name="connectionFactory" ref="{$queueFactoryID}"/>
        <property name="messageSelector" value="Command=2001" /> <!-- getMeta request -->
      </bean>
    </xsl:if>  

  </xsl:template>  

  <!--================================================-->
  <!--   Generate a Cas Multiplier Sync input Channel -->
  <!--================================================-->
  
  <!-- note: NO LONGER USED as of UIMA-1019 design change -->
  <!--xsl:template name="generateCMSyncInputChannel">
    <xsl:param name="aeNameUnique"/>
    <xsl:param name="q_ID"/-->   <!-- this is a descriptor-unique bean id of the input (or ret) q endpoint -->
    <!--xsl:param name="queueFactoryID"/>
    <xsl:param name="inputOrReturn"/--> <!-- input or return -->
    <!--xsl:param name="kind"/--> <!-- either primitive or asAggr, used as 1st part of ctlr name -->
    <!--xsl:param name="msgHandlerChainID"/>
    <xsl:param name="nbrConcurrentConsumers"/>
    <xsl:param name="remote"/-->  <!-- is always () -->
    <!--xsl:param name="poolingTaskExecutor"/>
    
    <xsl:variable name="q_listenerID" select="concat($kind, '_', $inputOrReturn, '_q_listenerID_',$aeNameUnique,
             if ($remote) then concat('_', $remote/@key) else '')"/>    
    <xsl:variable name="msgListenerContainerID" select="concat($kind, '_', $inputOrReturn, '_msgLsnrCntnr_', $aeNameUnique,
             if ($remote) then concat('_', $remote/@key) else '','__CasSync')"/>    


    <xsl:sequence select="f:generateLineComment((
      '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~',
      'JMS msg listener container for ', 
      'second listener handling messages from the throttle queue.',
      'This Q holds requests to free CAS Multiplier CASes',
      'back to the CM CAS Pool.',
       concat($q_ID, '__CasSync'),
      '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'),3)"/>    
    <bean id="{$msgListenerContainerID}"
      class="org.apache.uima.adapter.jms.activemq.UimaDefaultMessageListenerContainer"-->

      <!-- next commented out - because pool instances are "pinned" to particular ae engine instances -->
      <!--xsl:if test="$poolingTaskExecutor">
        <xsl:sequence select="f:generateLineComment('Connect to pooling task executor for primitive',5)"/>
        <property name="taskExecutor" ref="{$poolingTaskExecutor}"/>
      </xsl:if-->

      <!--xsl:sequence select="f:generateLineComment('Define number of JMS Consumers',5)"/>
      <property name="concurrentConsumers" value="{$nbrConcurrentConsumers}"/>
      
      <xsl:sequence select="f:generateLineComment(concat($inputOrReturn, ' Queue'), 5)"/>
      <property name="destination" ref="{concat($q_ID,'__CasSync')}"/>
           
      <xsl:sequence select="f:generateLineComment('POJO to delegate JMS Messages to', 5)"/>
      <property name="messageListener" ref="{$q_listenerID}"/>
      
      <property name="connectionFactory" ref="{$queueFactoryID}"/>
    </bean>
  </xsl:template-->
    
  <!--==========================================-->
  <!--   Generate an output Channel             -->
  <!--==========================================-->
  <xsl:template name="generateOutputChannel">
    <xsl:param name="aeNode"/>
    <xsl:param name="origin_q_name"/>
    <xsl:param name="return_q_ID"/>
    <xsl:param name="kind"/>

    <xsl:sequence select="f:generateLineComment((
      '==================================',
      'OutputChannel - for flows out of ',
      'this component, both to delegates',
      '(if aggr) and back up to sender',
      '=================================='
      ), 3)"/> 
    
    <bean id="{f:getOutputChannelID($aeNode)}"
          class="org.apache.uima.adapter.jms.activemq.JmsOutputChannel" init-method="initialize">
      <property name="serviceInputEndpoint" value="{$origin_q_name}"/>
      <property name="controller" ref="{f:getControllerID($aeNode)}" />
      <xsl:if test="$kind = 'asAggr'">
        <property name="controllerInputEndpoint" value="{$return_q_ID}" />
      </xsl:if>
    </bean>
  </xsl:template>

  <!-- ========================================================================= -->  
  <!--   async input:    meta, processRequest                                    -->  
  <!--   async internal: meta, processRequest, processResponse because used to   -->  
  <!--                     send to co-located delegates                          -->  
  <!--   primitive:      meta, processRequest                                    -->  
  <!-- ========================================================================= -->  
  <xsl:template name="generateMsgHandlerChain">
    <xsl:param name="aeNode"/>
    <xsl:param name="kind"/>
    <xsl:sequence select="$nl"/>
    <xsl:sequence select="f:generateBlockComment(
      (concat('  M E S S A G E    H A N D L E R: ', $kind),
       concat(' for controller: ', f:getControllerID($aeNode))), 3)"/>
       
     <xsl:sequence select="concat($nl,'   ')"/>
    
     <!-- either generate "return" msg handlers or "receiving requests" msg handlers -->
     <xsl:if test="$kind ne 'aggregate_return'"> 
       <bean id="{f:getMetaMsgHandlerID($aeNode, $kind)}"
         class="org.apache.uima.aae.handler.input.MetadataRequestHandler_impl">
        <!-- Define handlers name -->
        <constructor-arg index="0" value="MetadataRequestHandler"/>
        <property name="controller" ref="{f:getControllerID($aeNode)}"/>
        <!-- Link to the next handler in the chain -->
        <property name="delegate" ref="{f:getProcessRequestHandlerID($aeNode, $kind)}"/>
      </bean>
  
      <xsl:sequence select="$nl2"/>
      <bean id="{f:getProcessRequestHandlerID($aeNode, $kind)}" class="org.apache.uima.aae.handler.input.ProcessRequestHandler_impl" >
        <!-- Define handlers name -->
        <constructor-arg index="0" value="ProcessRequestHandler"/>
        <property name="controller" ref="{f:getControllerID($aeNode)}" />
        <!-- Link to the next handler in the chain -->
        <!--xsl:if test="$kind eq 'aggregate_return'">
          <property name="delegate" ref="{f:getProcessResponseHandlerID($aeNode, $kind)}" />
        </xsl:if-->
      </bean>
    </xsl:if>
    
    <xsl:if test="$kind eq 'aggregate_return'">
      
      <xsl:sequence select="$nl2"/>
      <bean id="{f:getProcessResponseHandlerID($aeNode, 'aggregate_return')}" 
        class="org.apache.uima.aae.handler.input.ProcessResponseHandler" >
       <!-- Define handlers name -->
        <constructor-arg index="0" value="ProcessResponseHandler"/>
        <property name="controller" ref="{f:getControllerID($aeNode)}" />
        <property name="delegate" ref="{f:getMetaMsgHandlerID($aeNode, concat($kind, '_response'))}" />
      </bean> 
     
      <xsl:sequence select="$nl2"/>
      <bean id="{f:getMetaMsgHandlerID($aeNode, concat($kind, '_response'))}" 
               class="org.apache.uima.aae.handler.input.MetadataResponseHandler_impl">
       <!-- Define handlers name -->
        <constructor-arg index="0" value="MetadataResponseHandler"/>
        <property name="controller" ref="{f:getControllerID($aeNode)}" />
        <property name="delegate" ref="{f:getProcessRequestHandlerID($aeNode, $kind)}" />
      </bean> 

      <xsl:sequence select="$nl2"/>
      <!-- the reason there is a ProcessRequest handler in the return queue is because
           this is how Cas Multiplier things are handled -->
      <bean id="{f:getProcessRequestHandlerID($aeNode, $kind)}" class="org.apache.uima.aae.handler.input.ProcessRequestHandler_impl" >
        <!-- Define handlers name -->
        <constructor-arg index="0" value="ProcessRequestHandler"/>
        <property name="controller" ref="{f:getControllerID($aeNode)}" />
      </bean> 

    </xsl:if>

  </xsl:template>
  <!--============================================================-->       
  <!--           Standard Beans                                  =-->       
  <!--============================================================--> 
  <xsl:template name="generateStandardBeans">

    
    <!-- no longer true need an internal broker if any of the following are true -->
    <!--   A service is async = true (implies aggregate)
    or The service specifies its input queue as "vm:..."
    -->

    <!-- only generate for inputq = localhost.  async=true handled with Java queue -->
    <!-- removed: boolean(u:service/u:analysisEngine[@async eq 'true']) or  -->    
    <!-- next removed per Jira UIMA-1288
    <xsl:if
      test="boolean(u:service/u:inputQueue[@brokerURL='vm://localhost'])">
      
      <xsl:sequence select="f:generateLineComment('connection factory for co-located things',3)"/>
      <bean id="controllerJmsFactory"
        class="org.apache.activemq.ActiveMQConnectionFactory"
        depends-on="brokerDeployerService">
        <property name="brokerURL" value="vm://localhost"/>
        <property name="prefetchPolicy" ref="prefetchPolicy"/>
      </bean>
    </xsl:if>
    -->
    
    <!-- no longer for async=true, only for inputq = localhost -->
    <!-- next removed per Jira UIMA-1288
    <xsl:if test="boolean(u:service/u:inputQueue[@brokerURL='vm://localhost'])">
      <xsl:sequence select="f:generateLineComment('Deploys a co-located broker',3)"/>
      <bean id="brokerDeployerService" class="org.apache.uima.adapter.jms.activemq.BrokerDeployer">
        <constructor-arg index="0" value="{1000 * 1024 * 1024}"/>
      </bean>
        
    </xsl:if>
    -->
        <!--    
    <xsl:message select="$uniqueInputQueueBrokers"/>
    <xsl:message select="name($uniqueInputQueueBrokers/u:inputQueue)"/>
    <xsl:message>*** unique Input Queue Brokers ***</xsl:message>
    <xsl:message select="name($uniqueInputQueueBrokers/holder) = ''"/>
    
    <xsl:message>*** end unique Input Queue Brokers ***</xsl:message> -->
    
    <!-- all input queues except the vm://localBroker one -->
    <xsl:for-each select="$uniqueInputQueueBrokers/u:inputQueue">
      <xsl:sequence select="f:generateLineComment(
        ('Factory for specific external queue broker:',
         @brokerURL),3)"/>
      <bean id="{f:getQbrokerID(.)}"
        class="org.apache.activemq.ActiveMQConnectionFactory">
        <property name="brokerURL" value="{@brokerURL}"/>
        <property name="prefetchPolicy" ref="prefetchPolicy"/>
      </bean>
    </xsl:for-each>
    
    <!-- all input queues except the vm://localBroker one -->
    <xsl:for-each select="$uniqueInputQueueBrokers/u:inputQueue">
      <xsl:sequence select="f:generateLineComment(
        ('Factory for specific external queue broker:',
         @brokerURL),3)"/>
      <bean id="{f:getQbrokerID(.)}-reply"
        class="org.apache.activemq.ActiveMQConnectionFactory">
        <property name="brokerURL" value="{@brokerURL}"/>
        <property name="prefetchPolicy" ref="prefetchPolicy-reply"/>
      </bean>
    </xsl:for-each>
        
              <!-- Creates an instance of the ResourceManager -->
   <xsl:sequence select="f:generateLineComment('Creates an instance of the ResourceManager',3)"/>
   <bean id="resourceManager" class="org.apache.uima.aae.UimaClassFactory"
    factory-method="produceResourceManager" singleton="true"/>

      <!-- Creates an instance of the CasManager -->
   <xsl:sequence select="f:generateLineComment('Creates an instance of the CasManager',3)"/>
   <bean id="casManager" class="org.apache.uima.aae.AsynchAECasManager_impl" singleton="true" >
      <constructor-arg index="0" ref="resourceManager"/>
      <xsl:sequence select="f:generateLineComment('Defines how many CASes will be in the CAS pool',5)"/>
      <property name="casPoolSize" value="{u:casPool/@numberOfCASes}" />
      <property name="disableJCasCache" value="{u:casPool/@disableJCasCache}" />
      <xsl:if test="u:casPool/@initialFsHeapSize">
        <xsl:sequence select="f:generateLineComment('Initial heap size for CASes',5)"/>
        <property name="initialFsHeapSize" value="{if (u:casPool/@initialFsHeapSize eq 'defaultFsHeapSize') 
            then '2000000' 
            else u:casPool/@initialFsHeapSize}" />
      </xsl:if>
    </bean>

    <!-- create bean to do placeholder substitution Jira UIMA-1288 -->
    <bean id="placeholderSubstitution" class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
      <property name="systemPropertiesModeName" value="SYSTEM_PROPERTIES_MODE_OVERRIDE"/>  
    </bean>
    
    <!-- Creates a Shared Cache  -->
    <xsl:sequence select="f:generateLineComment('Creates a Shared Cache',3)"/>
    <bean id="inProcessCache" class="org.apache.uima.aae.InProcessCache" /> 
    
    <xsl:sequence select="f:generateLineComment('Create prefetch policy', 3)"/>
    
    <bean id="prefetchPolicy" class="org.apache.activemq.ActiveMQPrefetchPolicy">
      <property name="queuePrefetch" 
        value="{u:service/u:inputQueue/@prefetch}"/>
    </bean>
    
    <bean id="prefetchPolicy-reply" class="org.apache.activemq.ActiveMQPrefetchPolicy">
      <property name="queuePrefetch" 
        value="1"/>
    </bean>
    
    <xsl:call-template name="generateErrorConfigDetails"/>
  </xsl:template>

  
  <xsl:template  match="u:delegates">
    
    <xsl:variable name="aggrNode" select="parent::node()"/>
 
    <!--xsl:message select="' *** in delegates ***'"/-->
      
    <xsl:for-each select="u:analysisEngine">
      
      <xsl:variable name="iqn" select="f:getInternalInputQueueName(.)"/>
      
      <!-- no longer generate internal input queues - switched to using Java queues
      <xsl:sequence select="f:generateLineComment((
        '~~~~~~~~~~~~~~~~~~~~~~~~~~~',
        'Internal Input Queue for',
        @key,
        '~~~~~~~~~~~~~~~~~~~~~~~~~~~'), 3)"/>
      <bean id="{$iqn}" 
            class="org.apache.activemq.command.ActiveMQQueue">
         <constructor-arg index="0" value="{$iqn}"/>
      </bean>
      -->
      
      <xsl:apply-templates  select=".">
        <xsl:with-param tunnel="yes" name="input_q_ID" select="$iqn"/>
        <xsl:with-param tunnel="yes" name="inputQueueFactoryID" select="'controllerJmsFactory'"/>  
      </xsl:apply-templates>  
    </xsl:for-each>

    <xsl:apply-templates select="u:remoteAnalysisEngine"/>
     
  </xsl:template>
          
  <!--============================================================-->       
  <!--           do nothing template - if not here, get the value=-->
  <!--             outputted by default                          =-->       
  <!--============================================================-->
  
  <xsl:template match="u:environmentVariable"/>
  
  <!--============================================================-->       
  <!--           pass 1 - put in defaults                        =-->       
  <!--============================================================-->
  
  <!-- ddd = deployment descriptor with defaults -->
  <xsl:variable name="ddd">
    <!-- <xsl:message>Applying defaults to</xsl:message>
    <xsl:message select="."/> -->
    <xsl:sequence select="f:validateDeploymentDescriptor(u:analysisEngineDeploymentDescription)"/>
    <xsl:variable name="result">
      <xsl:apply-templates mode="addDefaults" select="u:analysisEngineDeploymentDescription"/>
    </xsl:variable>
    <!--xsl:message select="'start of default output'"/>
    <xsl:message select="$result"/>
    <xsl:message select="'end of default output'"/-->
    <xsl:sequence select="$result"/>
  </xsl:variable>

  <xsl:template mode="addDefaults" match=
    "u:analysisEngineDeploymentDescription|
     u:topDescriptor|
     u:import|
     u:name|
     u:description|
     u:version|
     u:vendor|
     u:casManagement|
     u:errorHandling|
     u:timeout|
     u:exception|
     u:userErrorHandler|
     u:userErrorHandlerSpec
    ">
    <!--xsl:sequence select="f:validate(.)"/-->
    <xsl:copy>       
     <xsl:copy-of select='@*'/>
     <xsl:apply-templates mode="addDefaults"/> 
    </xsl:copy>
  </xsl:template>
  
  <xsl:template mode="addDefaults" match="u:deployment[@protocol='jms' and @provider='activemq']" >
    <!--xsl:sequence select="f:validate(.)"/-->
    <u:deployment protocol="jms" provider="activemq">
      <xsl:choose>
        <xsl:when test="not(u:casPool)">
          <xsl:choose>
            <xsl:when test=             "u:service/u:analysisEngine[(not(@async)) or (@async = ('no', 'false'))]/u:scaleout/@numberOfInstances">
              <u:casPool numberOfCASes="{u:service/u:analysisEngine[(not(@async)) or (@async = ('no', 'false'))]/u:scaleout/@numberOfInstances}"
                 initialFsHeapSize="defaultFsHeapSize" disableJCasCache="false" />
            </xsl:when>
            <xsl:otherwise>
              <u:casPool numberOfCASes="1" initialFsHeapSize="defaultFsHeapSize" disableJCasCache="false" />
            </xsl:otherwise>
          </xsl:choose>
        </xsl:when>
          
        <xsl:otherwise>
          <!-- casPool argument provided.  verify that the number of instances is >= scaleout if scaleout is present -->
          <!-- DONE later in this defaulting code - also handle case where not all parameters were specified -->
        </xsl:otherwise>
      </xsl:choose>
      <xsl:apply-templates mode="addDefaults"/>
    </u:deployment>
  </xsl:template>

  <!-- allow old services for now -->
  <xsl:template mode="addDefaults" match="u:services">
    <!--xsl:sequence select="f:validate(.)"/-->
    <xsl:apply-templates mode="addDefaults">
      <!-- keyPath is the list of keys with / inbetween, from the associated AE descriptor expansion, 
           for delegates, used to identify a particular one in case of errors -->
      <xsl:with-param name="keyPath"  tunnel="yes" select="''"/>
    </xsl:apply-templates>
  </xsl:template>          

  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:          -->
  <!--     <casPool>               -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ --> 
  <xsl:template mode="addDefaults" match="u:casPool">
    
    <!-- UIMA-1666
       if it's an non-async primitive at the top level which is scaled out,
         if the scaleout != the caspool size, give a warning message and force the caspool size to be the scaleout
    -->
    <xsl:variable name="isTopLvlSync" as="xs:boolean" select=
      "if (../u:service/u:analysisEngine[(not(@async)) or (@async = ('no', 'false'))]) then fn:true() else fn:false()"/>
    
    <xsl:variable name="nbrInstances" select="../u:service/u:analysisEngine/u:scaleout/@numberOfInstances"/>
    
    <xsl:variable name="casPoolSize" as="xs:integer">      
      <xsl:choose>
        <xsl:when test="$isTopLvlSync and 
          $nbrInstances and
          ./@numberOfCASes and
         ($nbrInstances ne ./@numberOfCASes)">
          <xsl:sequence select="f:msgWithLineNumber(
                'WARN',
                ('Top level Async Primitive specifies a scaleout of', $nbrInstances,
                 ', but also specifies a Cas Pool size of', ./@numberOfCASes,
                 '.  The Cas Pool size is being forced to be the same as the scaleout.'), 
                .)"/>
          <xsl:sequence select="$nbrInstances"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select="if (./@numberOfCASes) then ./@numberOfCASes else '1'"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <u:casPool numberOfCASes="{$casPoolSize}"
               initialFsHeapSize="{if (./@initialFsHeapSize) then ./@initialFsHeapSize else 'defaultFsHeapSize'}"
               disableJCasCache="{if (./@disableJCasCache) then ./@disableJCasCache else 'false'}" 
               />
  </xsl:template>
    
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:          -->
  <!--     <service>               -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->  
  <xsl:template mode="addDefaults" match="u:service">
    <!--xsl:sequence select="f:validate(.)"/-->   
    <xsl:variable name="ae_descriptor" select="f:getImport(u:topDescriptor/u:import)"/> 
    <!--xsl:message select="'*** top descriptor'"/>
    <xsl:message select="$ae_descriptor"/-->
    
    <xsl:variable name="isTopLvlCpp" as="xs:string" 
      select="if (f:isCPP($ae_descriptor) and 
                   (u:custom/@name eq 'run_top_level_CPP_service_as_separate_process'))
               then 'yes' else 'no'"/>        
    <u:service i:isTopLvlCpp="{$isTopLvlCpp}">
   
      <xsl:for-each select="u:custom">
        <xsl:copy-of select="."/>
      </xsl:for-each>
      
      <xsl:choose>
        <xsl:when test="u:inputQueue">
          <xsl:if test="not(starts-with(u:inputQueue/@brokerURL, 'tcp://')) and 
                        not(starts-with(u:inputQueue/@brokerURL, '${')) and
                        @brokerURL">
            <xsl:sequence select="f:msgWithLineNumber(
            'ERROR',
            'top level input Queue broker protocol must be tcp:// for a top level C++ component',
            .)"/>
          </xsl:if>
          <xsl:apply-templates mode="addDefaults" select="u:inputQueue"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select="f:msgWithLineNumber(
            'ERROR',
            'top level service element must have an inputQueue element',
            .)"/>
          <!--xsl:message select="'ERROR: top level service element must have an inputQueue element'"/-->
        </xsl:otherwise>
      </xsl:choose>
     
      <!--xsl:copy-of select="u:topDescriptor"/  not used in pass 2 -->
      <xsl:if test="not(u:topDescriptor/u:import/(@location|@name))">
        <xsl:sequence select="f:msgWithLineNumber(
            'ERROR',
            ('Service', string(u:inputQueue/@endpoint), 'missing required topDescriptor element'),
            .)"/>
        
        <!--xsl:message select=
          "concat('ERROR: Service ', u:inputQueue/@endpoint, ' missing required topDescriptor element')"/-->
      </xsl:if>
       
      <xsl:choose>
        <xsl:when test="$isTopLvlCpp eq 'yes'">
          <xsl:copy-of select="u:environmentVariables"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:if test="u:environmentVariables">
            <xsl:sequence select="f:msgWithLineNumber(
              'ERROR',
              ('Service element contains an environmentVariables element,', $nl,  
              'but the referenced top-level descriptor isn''t a C++ component'),
              .)"/>
          </xsl:if>
        </xsl:otherwise>
      </xsl:choose>
             
      <xsl:variable name="generatedAE">
        <!-- wrap the intended fragment in a service so that later, other code can 
             test the parent to see it is the top level one -->
        <u:service>
          <u:analysisEngine key="{u:inputQueue/(@endpoint|@queueName)}" async="false"/>
        </u:service>  
      </xsl:variable>
        
      <!--xsl:message select="'*** u:service/* '"/>
        <xsl:message select="."/-->
       <xsl:variable name="topLevelAe"
        select="if (u:analysisEngine) then u:analysisEngine else $generatedAE/u:service/u:analysisEngine"/>

      <!--xsl:message select="'top descriptor/import'"/>
      <xsl:message select="u:topDescriptor/u:import"/-->
      <xsl:variable name="aePath" select="f:fixupPath(u:topDescriptor/u:import, '.')"/>
      <xsl:apply-templates mode="addDefaults" select="$topLevelAe">
        <xsl:with-param name="defaultErrorConfig"  tunnel="yes" select="'topAe'"/>
        <xsl:with-param name="local_ae_descriptor" tunnel="yes" select="$ae_descriptor"/>
        <xsl:with-param tunnel="yes" name="local_ae_descriptor_file_path" select="$aePath"/>
      </xsl:apply-templates> 
       
    </u:service>  
  </xsl:template>

  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:          -->
  <!--     <inputQueue>            -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <xsl:template mode="addDefaults" match="u:inputQueue">
    <!--xsl:sequence select="f:validate(.)"/-->

      <!--xsl:message select="'*** defaulting inputqueue'"/>
      <xsl:message select="@brokerURL"/-->
    <xsl:if test="not(@endpoint or @queueName)">
      <xsl:sequence select="f:msgWithLineNumber('ERROR', 'missing endpoint name in inputQueue element', .)"/>
      <!--xsl:message select="'ERROR: missing endpoint name in inputQueue element'"/-->
      <xsl:message select="."/>
    </xsl:if>
    <xsl:if test="@brokerURL eq 'vm://localhost'">
      <xsl:sequence select="f:msgWithLineNumber(
        'ERROR',
        'broker protocol of vm://localhost not supported',
        .)"/>
    </xsl:if>
    <xsl:choose>
      <xsl:when test="../../u:remoteAnalysisEngine">
        <xsl:choose>     
          <xsl:when test="not(@brokerURL)">
            <!-- no longer an error - see UIMA-1288 -->
            <!--xsl:sequence select="f:msgWithLineNumber(
               'ERROR',
               'remote input Queue broker protocol must be specified',
               .)"/-->
            <u:inputQueue brokerURL="{'${defaultBrokerURL}'}"
                           endpoint="{if (@endpoint) then @endpoint
                                                     else if (@queueName) then @queueName
                                                                          else ''}"
                           prefetch="{if (@prefetch) then @prefetch else '0'}"/>
          </xsl:when>
          <xsl:when test="starts-with(@brokerURL, 'vm://')">
            <xsl:sequence select="f:msgWithLineNumber(
               'ERROR',
               'remote input Queue broker protocol cannot be a vm://... protocol',
               .)"/>
          </xsl:when>
          <xsl:otherwise>
            <u:inputQueue brokerURL="{@brokerURL}"
                           endpoint="{if (@endpoint) then @endpoint
                                                     else if (@queueName) then @queueName
                                                                          else ''}"
                           prefetch="{if (@prefetch) then @prefetch else '0'}"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:otherwise>
        <u:inputQueue brokerURL="{if (@brokerURL) then @brokerURL else '${defaultBrokerURL}'}"
                       endpoint="{if (@endpoint) then @endpoint
                                                 else if (@queueName) then @queueName
                                                                      else ''}"
                       prefetch="{if (@prefetch) then @prefetch else '0'}"/>    
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:          -->
  <!--     <analysisEngine>        -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ --> 
    
  <xsl:template mode="addDefaults" match="u:analysisEngine">
    <xsl:param tunnel="yes" name="local_ae_descriptor"/> 
    <xsl:param tunnel="yes" name="defaultErrorConfig"/>
    <xsl:param tunnel="yes" name="local_ae_descriptor_file_path"/>
    <xsl:param tunnel="yes" name="keyPath"/> 
   
    <!--xsl:message select="'******************start local ae descriptor'"/>
    <xsl:message select="$local_ae_descriptor"/>
    <xsl:message select="'******************end local ae descriptor'"/-->
      
    <!--xsl:sequence select="f:validate(.)"/-->     
    <xsl:variable name="key" as="xs:string">
      <xsl:choose>
        <xsl:when test="@key">
          <xsl:sequence select="@key"/>
        </xsl:when>
        <xsl:when test="parent::u:service">
          <!--
          <xsl:message select="'*** top key'"/>
          <xsl:message select="parent::node()/u:topDescriptor/u:import/@location"/> -->
          <xsl:sequence select="parent::u:service/u:inputQueue/(@endpoint|@queueName)"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence  select="f:msgWithLineNumber(
            'ERROR', 'required key name missing in delegate', .)"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    
    <!--xsl:message select="'key is'"/>
    <xsl:message select="$key"/ -->
    
    <xsl:if test="@async = ('yes', 'no')">
      <xsl:sequence select="f:msgWithLineNumber('WARNING',
             ('deployment descriptor for analysisEngine:', $keyPath,
             'specifies', @async, ' but the async value should be true or false'),
             .)"/>
    </xsl:if>
    
    <xsl:variable name="async">
      <xsl:choose>
        <xsl:when test="(string(@async) = ('yes','true')) and 
                        not(f:isAggr($local_ae_descriptor))">
          <xsl:sequence select="f:msgWithLineNumber('ERROR',
             ('deployment descriptor for analysisEngine:', $keyPath,
             'specifies async=&quot;true&quot; but the analysis engine is a primitive'),
             .)"/>
          <xsl:value-of select="'false'"/>
        </xsl:when>
        <xsl:when test="(string(@async) = ('yes','true')) and 
                        f:isCPP($local_ae_descriptor)">
          <xsl:sequence select="f:msgWithLineNumber('ERROR',
             ('deployment descriptor for analysisEngine:', $keyPath,
             'specifies async=''true''; but this isn''t supported for CPP components'),
             .)"/>
          <xsl:value-of select="'false'"/>
        </xsl:when>
        <xsl:when test="not(string(@async) = ('yes', 'no', 'true', 'false', ''))">
          <xsl:sequence select="f:msgWithLineNumber('ERROR',
             ('deployment descriptor for analysisEngine:', $keyPath,
             'specifies', concat('async=&quot;', string(@async),
                   '&quot;, but only true or false are allowed as values.')),
             .)"/>
          <xsl:value-of select="if (f:isAggr($local_ae_descriptor)) then 'true' else 'false'"/>    
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select=
            "if (@async) then if (string(@async) = ('yes', 'true')) then 'true' else 'false' 
                         else if (u:delegates and not(f:isCPP($local_ae_descriptor))) then 'true' else 'false'"/>    
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    
    <xsl:if test="(string($async) = ('false', 'no')) and u:delegates">
      <xsl:sequence select="f:msgWithLineNumber('ERROR',
        ('deployment descriptor for analysisEngine:', $keyPath,
         'specifies false for the async attribute, but contains a delegates element, which is not allowed in this case.'), .)"/>
    </xsl:if>
     
    <!--xsl:sequence select="f:msgWithLineNumber('INFO', 
        ('deployment descriptor for AE:', $key,
         'has delegate?', if (u:delegates) then 'true' else 'false',
         'async is:', $async), .)"/-->
    
    
    <xsl:variable name="internalReplyQueueScaleout" as="xs:string">
      <!-- next choose commentted out because it disallowed internal reply q scaleout
           for primitives - but this could be needed for primitives which are
           CAS multipliers -->
      <!--xsl:choose>
        <xsl:when test="(string($async) = 'false') and @internalReplyQueueScaleout">
          <xsl:sequence select="f:msgWithLineNumber('WARN',
              ('deployment descriptor for analysisEngine:', $key,
               'specifies', concat('internalReplyQueueScaleout=&quot;', string(@internalReplyQueueScaleout),
               '&quot;, this is ignored for async=&quot;false&quot; analysisEngine specifications.')),
              .)"/>
          <xsl:value-of select="'1'"/>
        </xsl:when>
        <xsl:otherwise-->
          <xsl:sequence select="if (@internalReplyQueueScaleout) then @internalReplyQueueScaleout else '1'"/>
        <!--/xsl:otherwise>
      </xsl:choose-->
    </xsl:variable>

    <xsl:variable name="inputQueueScaleout" as="xs:string">
      <xsl:choose>
        <xsl:when test="(string($async) = 'false') and @inputQueueScaleout">
          <xsl:sequence select="f:msgWithLineNumber('WARN',
              ('deployment descriptor for analysisEngine:', $keyPath,
               'specifies', concat('inputQueueScaleout=&quot;', string(@inputQueueScaleout),
               '&quot;, this is ignored for async=&quot;false&quot; analysisEngine specifications.'),
               'Use the scaleout numberOfInstances=xx element instead for this, for primitives.'),
              .)"/>
          <xsl:value-of select="'1'"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select="if (@inputQueueScaleout) then @inputQueueScaleout else '1'"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
         
    <u:analysisEngine key="{$key}" async="{$async}"
      internalReplyQueueScaleout="{$internalReplyQueueScaleout}"
      inputQueueScaleout        ="{$inputQueueScaleout}">
 
      <i:local_ae_descriptor file_path="{$local_ae_descriptor_file_path[1]}">
        <!--xsl:message select="'local_ae_descriptor'"/>
        <xsl:message select="$local_ae_descriptor"/-->  
        <xsl:sequence select="$local_ae_descriptor"/>
      </i:local_ae_descriptor>
      
      <!--xsl:apply-templates mode="addDefaults" select="u:casMultiplier"/-->
      <xsl:choose>
        <xsl:when test="(string($local_ae_descriptor/*/u:analysisEngineMetaData/u:operationalProperties/u:outputsNewCASes) eq 'true')
                        or $local_ae_descriptor/u:collectionReaderDescription">
          <!--xsl:if test="u:casMultiplier">
            <xsl:sequence select="f:validate(u:casMultiplier)"/>
          </xsl:if-->
          <xsl:choose>
            <xsl:when test="u:casMultiplier">
              <xsl:if test="(string($async) eq 'true') and
                      (u:casMultiplier/@poolSize or
                       u:casMultiplier/@initialFsHeapSize)">
                <xsl:sequence select="f:msgWithLineNumber('WARNING',
                   ('casMultiplier settings for poolSize (', u:casMultiplier/@poolSize, 
                    ') and initialFsHeapSize (', u:casMultiplier/@initialFsHeapSize, ')',
                    $nl, 'will be ignored',
                    'for the analysisEngine with key=', $keyPath, $nl,
                    'because the pool specs are set using the contained delegate cas multiplier specification.'
                   ),
                   .)"/>       
              </xsl:if>
              <xsl:if test="parent::u:service and u:casMultiplier/@processParentLast">
                <xsl:sequence select="f:msgWithLineNumber('WARNING',
                   ('casMultiplier settings for processParentLast will be ignored',
                    'for the top-level analysisEngine with key=', $key, $nl,
                    'To specify this value for the top level,', 
                    'specify it on the containing (remote) aggregate for this service.'
                   ),
                   .)"/>       
              </xsl:if>
              
              <!-- the above conditionally printed warnings 
                   The below generates the properly defaulted element -->
              <xsl:choose>
                <xsl:when test="(string($async) eq 'true') and not(parent::u:service)">
                  <u:casMultiplier 
                    processParentLast="{if (u:casMultiplier/@processParentLast) then u:casMultiplier/@processParentLast else 'false'}"
                    disableJCasCache="{if (u:casMultiplier/@disableJCasCache) then u:casMultiplier/@disableJCasCache else 'false'}"
                  />
                </xsl:when>
                <!--  top level async: no cas multiplier settings are used
                      size etc from contained CM;
                      processParentLast not applicable for top level - 
                      uses (remote) containing aggregate's setting, if any
                      
                      However the casMultiplier element is generated so that 
                      a Second Input Q is built for messages from remote cas 
                      Multipliers, at the top level -->
                <xsl:when test="string($async) eq 'true'">
                  <u:casMultiplier 
                      disableJCasCache="{if (u:casMultiplier/@disableJCasCache) then u:casMultiplier/@disableJCasCache else 'false'}"/>                  
                </xsl:when>
                
                <!-- case async = false and not top level -->
                <xsl:when test="not(parent::u:service)">
                  <u:casMultiplier poolSize="{if (u:casMultiplier/@poolSize) then u:casMultiplier/@poolSize else '1'}"
                          initialFsHeapSize="{if (u:casMultiplier/@initialFsHeapSize) then u:casMultiplier/@initialFsHeapSize else '2000000'}"
                          processParentLast="{if (u:casMultiplier/@processParentLast) then u:casMultiplier/@processParentLast else 'false'}"
                          disableJCasCache= "{if (u:casMultiplier/@disableJCasCache)  then u:casMultiplier/@disableJCasCache  else 'false'}" 
                  />  
                </xsl:when>
                <!-- case async = false, top level -->
                <xsl:otherwise>
                  <u:casMultiplier poolSize="{if (u:casMultiplier/@poolSize) then u:casMultiplier/@poolSize else '1'}"
                          initialFsHeapSize="{if (u:casMultiplier/@initialFsHeapSize) then u:casMultiplier/@initialFsHeapSize else '2000000'}"
                          disableJCasCache="{if (u:casMultiplier/@disableJCasCache) then u:casMultiplier/@disableJCasCache else 'false'}"
                  />  
                </xsl:otherwise>             
              </xsl:choose>
            </xsl:when>
            
            <!-- after this point, have a cas multiplier, without a <casMultiplier> element -->
            
            
            <!-- async true, not top level -->
            <xsl:when test="(string($async) eq 'true') and
                            (not(parent::u:service))">
             <xsl:sequence select="f:msgWithLineNumber('WARN',
               ('Deployment descriptor for analysisEngine:', $keyPath,
               'is for a non-top-level CAS Multiplier (or Collection Reader wrapped as a CAS Multiplier).', $nl,
               'However, the &lt;casMultiplier> element is missing.', $nl,
               'The &lt;casMultiplier> element is only used here for specifying the processParentLast attribute.', $nl,
               'Defaulting to a processParentLast to false for this case, to',
               'let the parent flow with its chlidren.'),
               .)"/>
              <u:casMultiplier processParentLast="false"/>
            </xsl:when>
             
            <!-- async true top level - no cas multiplier settings are used
                      size etc from contained CM;
                      processParentLast not applicable for top level - 
                      uses (remote) containing aggregate's setting, if any
                      
                      However the casMultiplier element is generated so that 
                      a Second Input Q is built for messages from remote cas 
                      Multipliers, at the top level -->
            <xsl:when test="(string($async) eq 'true') and
                            (parent::u:service)">
              <u:casMultiplier/>             
            </xsl:when>
            
            <!-- async = false, and not top level -->
            <xsl:when test="not(parent::u:service)">
              <xsl:sequence select="f:msgWithLineNumber('WARN',
             ('Deployment descriptor for analysisEngine:', $key, $keyPath,
             ' is for a synchronous CAS Multiplier (not top level) (or Collection Reader wrapped as a CAS Multiplier).', $nl,
             'However, the &lt;casMultiplier> element is missing.', $nl,
                'Defaulting to a poolSize of 1, initialFsHeapSize of 2,000,000.', $nl,
                'Defaulting to a processParentLast to false for this case, to',
                'let the parent flow with its chlidren.'),
             .)"/>
              <u:casMultiplier poolSize="1" initialFsHeapSize="2000000" processParentLast="false"/>
            </xsl:when>

            <!--xsl:when test="u:casMultiplier/@poolSize">
              <xsl:copy-of select="u:casMultiplier"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:sequence select="f:msgWithLineNumber('ERROR',
             ('deployment descriptor for analysisEngine:', $key,
             'is for a CAS Multiplier (or Collection Reader wrapped as a CAS Multiplier), but no pool size is specified'),
             .)"/>
              <u:casMultiplier poolSize="1" initialFsHeapSize="2000000"/>
            </xsl:otherwise-->
          </xsl:choose>
        </xsl:when>
        <xsl:otherwise>  <!-- is not a cas multiplier -->
          <xsl:if test="u:casMultiplier">
            <xsl:sequence select="f:msgWithLineNumber('ERROR',
             ('deployment descriptor for analysisEngine:', $keyPath,
             'specifies a casMultiplier element, but the analysisEngine is not a CAS multiplier'),
             .)"/>
          </xsl:if>
        </xsl:otherwise>
      </xsl:choose>
      
      <!-- xsl:message select="('parent of ae:', parent::u:service)"/-->
      <xsl:choose>
        <xsl:when test="parent::u:service">
          <xsl:choose>
            <xsl:when test="u:asyncPrimitiveErrorConfiguration">
              <xsl:apply-templates mode="addDefaults" select="u:asyncPrimitiveErrorConfiguration"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:variable name="primErr">
                <u:asyncPrimitiveErrorConfiguration/>
              </xsl:variable>
              <xsl:apply-templates mode="addDefaults" select="$primErr"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:when>
        <xsl:otherwise>
          <xsl:choose>
            <xsl:when test="u:asyncAggregateErrorConfiguration">
              <xsl:apply-templates mode="addDefaults" select="u:asyncAggregateErrorConfiguration"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:variable name="aggrErr">
                <u:asyncAggregateErrorConfiguration/>
              </xsl:variable>
              <xsl:apply-templates mode="addDefaults" select="$aggrErr"/>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:otherwise>
      </xsl:choose>
      
      <xsl:if test="$async eq 'true'">
        <xsl:if test="$local_ae_descriptor/*/u:flowController">
          <!--xsl:message select="$local_ae_descriptor"/>
          <xsl:message select="f:fixupPath($local_ae_descriptor/*/u:flowController/u:import, $local_ae_descriptor_file_path[2])[1]"/-->        
          <i:flowController file_path="{f:fixupPath($local_ae_descriptor/*/u:flowController/u:import, $local_ae_descriptor_file_path[2])[1]}"/>
        </xsl:if>
        <!--xsl:message select="'*** u:ae w delegates/* '"/>
        <xsl:message select="."/-->
        
        <!--xsl:sequence select="f:validate(u:delegates)"/-->     

        <xsl:call-template name="defaultDelegates">
          <xsl:with-param name="ddDelegates" select="u:delegates/*"/>
        </xsl:call-template>
      </xsl:if>
         
      <xsl:if test="$async = ('false', 'no')">
        <xsl:choose>
          <xsl:when test="u:scaleout">
            <!--xsl:message select="'*** defaulting scaleout'"/>
            <xsl:message select="u:scaleout"/-->
            <u:scaleout numberOfInstances="{if (u:scaleout/@numberOfInstances) then u:scaleout/@numberOfInstances else 1}"/>
          </xsl:when>
          <xsl:otherwise>
            <u:scaleout numberOfInstances="1"/>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:if>       
    </u:analysisEngine>
  </xsl:template> 

  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:          -->
  <!--     <delegates>             -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->  
  <xsl:template name="defaultDelegates">
    <xsl:param name="ddDelegates"/>  <!-- analysisEngine(s) or remoteAnalysisEngine(s) -->
    <xsl:param tunnel="yes" name="defaultErrorConfig"/>
    <xsl:param tunnel="yes" name="local_ae_descriptor"/>
    <xsl:param tunnel="yes" name="local_ae_descriptor_file_path"/>
    <xsl:param tunnel="yes" name="keyPath"/>              

    <xsl:variable name="nextLevelDefaultErrorConfig" select=
          "if ($defaultErrorConfig eq 'topAe') then 'topDelegate' else 'none'"/>        

    <xsl:variable name="delegatesFromLocalAeDescriptor" select=
      "$local_ae_descriptor/(u:analysisEngineDescription|u:taeDescription)/u:delegateAnalysisEngineSpecifiers/u:delegateAnalysisEngine"/>
    
    <xsl:for-each select="$ddDelegates">
      <xsl:if test="not($delegatesFromLocalAeDescriptor[@key = current()/@key])">
        <xsl:sequence select="f:msgWithLineNumber('ERROR',
          ('The delegate in the deployment descriptor with', @key, ' does not match any delegates in the referenced descriptor'),
          .)"/>
      </xsl:if>
        
    </xsl:for-each>
    
    <!-- next limitation lifted for 2.3.0 
    <xsl:if test="count($ddDelegates[self::u:remoteAnalysisEngine[u:casMultiplier]]) > 1">
      <xsl:sequence select="f:msgWithLineNumber('ERROR',
        ('More than one remote delegate is a CAS Multiplier. This implementation only supports having one'),
         .)"/>
    </xsl:if>
    -->
    
    <u:delegates>
      <xsl:for-each select="$delegatesFromLocalAeDescriptor">
        
        <xsl:variable name="delegateAE" select="f:getDelegatePart($local_ae_descriptor, @key)"/>
        <!--sl:message select="('*** debug delegate from localAE:', @key)"/-->
        <!--xsl:message select="$ddDelegates"/-->
        <xsl:variable name="aeOrRemote" as="node()">
          <xsl:choose>
            <xsl:when test="$ddDelegates[string(@key) = string(current()/@key)]">             
              <xsl:sequence select="$ddDelegates[string(@key) eq string(current()/@key)]"/>
            </xsl:when>
            <xsl:otherwise>
              <!-- next line commented out - 
                   Wrong logic
                   If a deployment descriptor doesn't have a analysisEngine or remoteAnalysisEngine
                     for a delegate, make the default for this always be asynch = false
                -->          
              <!-- u:analysisEngine key="{@key}" async="{if (f:isAggr($delegateAE)) then 'true' else 'false'}"/-->
              <u:analysisEngine key="{@key}" async="false"/>
            </xsl:otherwise> 
          </xsl:choose>              
        </xsl:variable> 
        
        <!--xsl:message select="concat(
          '*** aeOrRemote defaulting, errorConfig Flag is ', $nextLevelDefaultErrorConfig)"/-->
        <!--xsl:message select="$aeOrRemote"/-->
        
        <xsl:variable name="aePath" select="f:fixupPath(u:import, $local_ae_descriptor_file_path[2])"/>
        
        <xsl:apply-templates mode="addDefaults" select="$aeOrRemote">
          <xsl:with-param tunnel="yes" name="defaultErrorConfig" select="$nextLevelDefaultErrorConfig"/>
          <xsl:with-param tunnel="yes" name="local_ae_descriptor" select="$delegateAE"/>
          <xsl:with-param tunnel="yes" name="local_ae_descriptor_file_path" select="$aePath"/>
          <xsl:with-param tunnel="yes" name="keyPath" select="if ($keyPath eq '') then @key else concat($keyPath, '/', @key)"/>
        </xsl:apply-templates>
      </xsl:for-each>
    </u:delegates>
  </xsl:template>
  
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:                          -->
  <!--     <u:asyncPrimitiveErrorConfiguration>    -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->  
  <xsl:template mode="addDefaults" match="u:asyncPrimitiveErrorConfiguration">
    <u:asyncPrimitiveErrorConfiguration>
      <xsl:choose>
        <xsl:when test="u:processCasErrors">
          <xsl:if test="u:processCasErrors/@thresholdCount and u:processCasErrors/@thresholdWindow">
            <xsl:if test="(u:processCasErrors/@thresholdWindow ne '0') and 
                           ((u:processCasErrors/@thresholdCount cast as xs:integer) gt (u:processCasErrors/@thresholdWindow cast as xs:integer))">
              <xsl:sequence select="f:msgWithLineNumber('ERROR',
                ('The', u:processCasErrors/@thresholdWindow, ' must be either 0, or larger than the', u:processCasErrors/@thresholdCount), 
                .)"/>
            </xsl:if>
          </xsl:if>
          
          <u:processCasErrors 
            thresholdCount= "{if (u:processCasErrors/@thresholdCount)  then u:processCasErrors/@thresholdCount  else 0}"
            thresholdWindow="{if (u:processCasErrors/@thresholdWindow) then u:processCasErrors/@thresholdWindow else 0}"
            thresholdAction="{if (u:processCasErrors/@thresholdAction) then u:processCasErrors/@thresholdAction else ''}"
          />          
        </xsl:when>
        <xsl:otherwise>
          <u:processCasErrors thresholdCount="0" thresholdWindow="0" thresholdAction=""/>
        </xsl:otherwise>     
      </xsl:choose>
      
      <xsl:choose>
        <xsl:when test="u:collectionProcessCompleteErrors">
          <u:collectionProcessCompleteErrors 
            additionalErrorAction="{if (u:collectionProcessCompleteErrors/@additionalErrorAction) 
                                      then u:collectionProcessCompleteErrors/@additionalErrorAction
                                      else ''}"
          />
        </xsl:when>
        <xsl:otherwise>
          <u:collectionProcessCompleteErrors additionalErrorAction=""/>
        </xsl:otherwise>
      </xsl:choose>
        
    </u:asyncPrimitiveErrorConfiguration>  
  </xsl:template>
  
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:                          -->
  <!--     <u:asyncAggregateErrorConfiguration>    -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->  
  <xsl:template mode="addDefaults" match="u:asyncAggregateErrorConfiguration">
    <u:asyncAggregateErrorConfiguration>
      <xsl:choose>
        <xsl:when test="u:getMetadataErrors">
          <u:getMetadataErrors 
            maxRetries= "{if (u:getMetadataErrors/@maxRetries)  then u:getMetadataErrors/@maxRetries  else 0}"
            timeout=    "{if (u:getMetadataErrors/@timeout)     then u:getMetadataErrors/@timeout     else 60000}"
            errorAction="{if (u:getMetadataErrors/@errorAction) then u:getMetadataErrors/@errorAction else 'terminate'}"
          />
        </xsl:when>
        <xsl:otherwise>
          <u:getMetadataErrors maxRetries="0" timeout="60000" errorAction="terminate"/> 
        </xsl:otherwise>
      </xsl:choose>
      
      <xsl:choose>
        <xsl:when test="u:processCasErrors">
          <xsl:if test="u:processCasErrors/@thresholdCount and u:processCasErrors/@thresholdWindow">
            <!--xsl:message select="('*** Running test', 'gt 9', ('100' cast as xs:integer) gt ('9' cast as xs:integer), 'more' )"/-->
            <xsl:if test="(u:processCasErrors/@thresholdWindow ne '0') and 
                           ((u:processCasErrors/@thresholdCount cast as xs:integer) gt (u:processCasErrors/@thresholdWindow cast as xs:integer))">
              <xsl:sequence select="f:msgWithLineNumber('ERROR',
                ('The', u:processCasErrors/@thresholdWindow, ' must be either 0, or larger than the', u:processCasErrors/@thresholdCount), 
                .)"/>
            </xsl:if>
          </xsl:if>
          <u:processCasErrors
            maxRetries= "{if (u:processCasErrors/@maxRetries)  then u:processCasErrors/@maxRetries  else 0}"
            timeout=    "{if (u:processCasErrors/@timeout)     then u:processCasErrors/@timeout     else 0}"
            continueOnRetryFailure=
              "{if (u:processCasErrors/@continueOnRetryFailure) then u:processCasErrors/@continueOnRetryFailure 
                                                                else 'false'}" 
            thresholdCount= "{if (u:processCasErrors/@thresholdCount)  then u:processCasErrors/@thresholdCount  else 0}"
            thresholdWindow="{if (u:processCasErrors/@thresholdWindow) then u:processCasErrors/@thresholdWindow else 0}"
            thresholdAction="{if (u:processCasErrors/@thresholdAction) then u:processCasErrors/@thresholdAction else ''}"
          />
        </xsl:when>
        <xsl:otherwise>
          <u:processCasErrors maxRetries="0" timeout="0" continueOnRetryFailure="false" 
                              thresholdCount="0" thresholdWindow="0" thresholdAction=""/>
        </xsl:otherwise>     
      </xsl:choose>
      
      <xsl:choose>
        <xsl:when test="u:collectionProcessCompleteErrors">
          <u:collectionProcessCompleteErrors
            timeout="{if (u:collectionProcessCompleteErrors/@timeout) then u:collectionProcessCompleteErrors/@timeout
                                                                      else 0}"
            additionalErrorAction="{if (u:collectionProcessCompleteErrors/@additionalErrorAction) 
                                       then u:collectionProcessCompleteErrors/@additionalErrorAction
                                       else ''}"
          />
        </xsl:when>
        <xsl:otherwise>
          <u:collectionProcessCompleteErrors timeout="0" additionalErrorAction=""/>
        </xsl:otherwise>
      </xsl:choose>
        
    </u:asyncAggregateErrorConfiguration>  
  </xsl:template>

  <!--
  <xsl:template mode="addDefaults" match="u:errorConfiguration">
    <xsl:param tunnel="yes" name="defaultErrorConfig"/> 
    
=       
    <xsl:variable name="errorHandling" select="u:errorHandling"/>
    <xsl:choose>
      <xsl:when test="$defaultErrorConfig eq 'topAe'">
        <xsl:sequence select="f:msgWithLineNumber('WARNING', 'errorConfiguration at top level not supported', .)"/>
        <!- -xsl:message select="'*** WARNING errorConfiguration at top level not supported'"/- ->
      </xsl:when>
      <xsl:otherwise>
        <xsl:choose>
          <xsl:when test="u:import">
            <xsl:apply-templates mode="addDefaults" select="f:getImport(u:import)"/>   
          </xsl:when>
          <xsl:otherwise>
            <u:errorConfiguration>
              <u:casManagement enableRevert="false"/>    
              <u:errorHandling>
                <xsl:for-each select="('metadataRequest', 'processRequest', 'collectionProcessingCompleteRequest')">
                  <xsl:call-template name="defaultTimeout">
                    <xsl:with-param name="timeoutNodes" select="$errorHandling/u:timeout"/>
                    <xsl:with-param name="eventKind" select="."/>
                  </xsl:call-template>
                </xsl:for-each>
                <xsl:call-template name="defaultException">
                  <xsl:with-param name="exceptionNodes" select="$errorHandling/u:exception"/>
                </xsl:call-template>
                <xsl:copy-of select="$errorHandling/u:userErrorHandler"/>
              </u:errorHandling>
            </u:errorConfiguration>
          </xsl:otherwise>
        </xsl:choose>   
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  
  <xsl:template name="defaultTimeout">
    <xsl:param tunnel="yes" name="defaultErrorConfig"/>
    <xsl:param name="timeoutNodes"/>
    <xsl:param name="eventKind"/>
   
    <xsl:choose>
      <xsl:when test="$timeoutNodes[@event eq $eventKind]">

        <xsl:copy-of select="$timeoutNodes[@event eq $eventKind]"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:if test="$defaultErrorConfig eq 'topDelegate'">
          <u:timeout event="{$eventKind}"
                     milliseconds="0"
                     threshold="0"
                     action="{'DropCas'}"/>
        </xsl:if>      
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
 
  <xsl:template name="defaultException">
    <xsl:param tunnel="yes" name="defaultErrorConfig"/>
    <xsl:param name="exceptionNodes"/>
    
    <xsl:choose>
      <xsl:when test="$exceptionNodes">
        <xsl:for-each select="$exceptionNodes/u:exception">
          <xsl:copy-of select="."/>
        </xsl:for-each>
        
      </xsl:when>
      <xsl:otherwise>
        <xsl:if test="$defaultErrorConfig eq 'topDelegate'">
          <u:exception event="" threshold="1" action="DropCas"/>
        </xsl:if>      
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template-->

  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->
  <!--     D E F A U L T:          -->
  <!--     <remoteAnalysisEngine>  -->
  <!-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->       
  <xsl:template mode="addDefaults" match="u:remoteDelegate|u:remoteAnalysisEngine">
    <xsl:param tunnel="yes" name="defaultErrorConfig"/>
    <!--xsl:message select="'*** remote delegate defaulting pass 1 called '"/-->
    <!--xsl:sequence select="f:validate(.)"/-->  
    
    <xsl:variable name="remoteReplyQueueScaleout" as="xs:string"
      select="if (@remoteReplyQueueScaleout) then @remoteReplyQueueScaleout else '1'"/>
      
    <u:remoteAnalysisEngine key="{@key}" remoteReplyQueueScaleout="{$remoteReplyQueueScaleout}">
      <xsl:if test="u:casMultiplier">
        <u:casMultiplier poolSize="{if (u:casMultiplier/@poolSize) then u:casMultiplier/@poolSize else '1'}"
                initialFsHeapSize="{if (u:casMultiplier/@initialFsHeapSize) then u:casMultiplier/@initialFsHeapSize else '2000000'}"
                processParentLast="{if (u:casMultiplier/@processParentLast) then u:casMultiplier/@processParentLast else 'false'}"
                disableJCasCache= "{if (u:casMultiplier/@disableJCasCache)  then u:casMultiplier/@disableJCasCache  else 'false'}"
                />
      </xsl:if>
      <xsl:variable name="tmp">
        <xsl:apply-templates mode="addDefaults" select="u:inputQueue"/>
      </xsl:variable>
      <!--xsl:message select="'*** remote delegate input q'"/>
      <xsl:message select="u:inputQueue"/-->
      <xsl:sequence select="$tmp"/>
      
      <xsl:choose>
        <xsl:when test="u:replyQueue">
          <xsl:sequence select="f:msgWithLineNumber('WARNING',
            ('replyQueue element no longer used - all reply queues are remote for remote delegates'), u:replyQueue)"/>
          <!--xsl:if test="not(u:replyQueue/@location = ('local', 'remote')) and u:replyQueue/@location">
            <xsl:sequence select="f:msgWithLineNumber('ERROR', 
              ('replyQueue location attribute, ', u:replyQueue/@location, ', must have a value of either ''local'' or ''remote'''), u:replyQueue)"/>
          </xsl:if>
          <xsl:if test="(u:replyQueue/@location eq 'local') and
                         starts-with($tmp/u:inputQueue/@brokerURL, 'http://')">
            <xsl:sequence select="f:msgWithLineNumber('ERROR', 
              '''local'' replyQueue location is not supported for ''http://'' style connections', u:replyQueue)"/>
          </xsl:if>
          <xsl:if test="(u:replyQueue/@location eq 'local')">
            <xsl:sequence select="f:msgWithLineNumber('ERROR',
              '''local'' replyQueue location is no longer supported and will be forcedto remote', u:replyQueue)"/>   
          </xsl:if-->
          <!--u:replyQueue location="{if (u:replyQueue/@location) then u:replyQueue/@location else
            if (starts-with($tmp/u:inputQueue/@brokerURL, 'http://')) then 'remote' else 'local'}"/-->
          <!--u:replyQueue location="remote" concurrentConsumers=
            "{if (u:replyQueue/@concurrentConsumers) then u:replyQueue/@concurrentConsumers else '1'}"/-->
        </xsl:when>
        <xsl:otherwise>
          <!--u:replyQueue location="remote" concurrentConsumers="1"/-->
          <!--u:replyQueue location="{
            if (starts-with($tmp/u:inputQueue/@brokerURL, 'http://')) then 'remote' else 'local'}"/-->
          <!--xsl:message select="('*** replyQueue', 
            if (starts-with($tmp/u:inputQueue/@brokerURL, 'http://')) then 'remote' else 'local')"/-->
        </xsl:otherwise>
      </xsl:choose>
      
      <xsl:choose>
        <xsl:when test="u:serializer">
          <xsl:copy-of select="u:serializer"/>
        </xsl:when>
        <xsl:otherwise>
          <u:serializer method="xmi"/>
        </xsl:otherwise>
      </xsl:choose>

      <xsl:choose>
        <xsl:when test="u:asyncAggregateErrorConfiguration">
          <xsl:apply-templates mode="addDefaults" select="u:asyncAggregateErrorConfiguration"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:variable name="rmtErr">
            <u:asyncAggregateErrorConfiguration/>
          </xsl:variable>
          <xsl:apply-templates mode="addDefaults" select="$rmtErr"/>
        </xsl:otherwise>
      </xsl:choose>     
      
      <!--xsl:choose>
        <xsl:when test="not(u:errorConfiguration) and ($defaultErrorConfig eq 'topDelegate')">
          <xsl:variable name="errorConfigElement">
            <u:errorConfiguration/>
          </xsl:variable>
          <xsl:apply-templates mode="addDefaults" select="$errorConfigElement"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:apply-templates mode="addDefaults" select="u:errorConfiguration"/>
        </xsl:otherwise>
      </xsl:choose-->
     </u:remoteAnalysisEngine>
  </xsl:template>

  <!--============================================================-->       
  <!--             Subroutines and Utilities                     =-->       
  <!--============================================================-->

  <!--============================================================-->       
  <!--|            Functions                                     |-->       
  <!--============================================================-->
 
  <xsl:function name="f:getImport">
    <xsl:param name="importNode"/>
    <xsl:choose>
      <xsl:when test="$importNode/@location">
        <xsl:sequence select="document($importNode/@location)"/>
      </xsl:when>
      <xsl:when test="$importNode/@name">
        <xsl:sequence select="document(x1:resolveByName($importNode/@name))"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:sequence select=
          "f:msgWithLineNumber('ERROR', 'import missing location or name attribute', $importNode)"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:function>
  
  
  <xsl:function name="f:fixupPath">
    <xsl:param name="node"/>  <!-- node has location or name attribute -->
    <xsl:param name="relBase"/>
    <!--xsl:message select="'*** fixup path'"/>
    <xsl:message select="$node"/>
    <xsl:message select="$relBase"/-->    

    <xsl:choose>
      <xsl:when test="$node/@location">
        <!-- some paths start with file: 
             strip that out if found, so the subsequent logic for
             detecting "absolute" paths works -->
        <xsl:variable name="relOrAbsPath" select=
          "if (starts-with($node/@location, 'file:')) then
             substring($node/@location, 6) else $node/@location"/>
        <!--xsl:variable name="relOrAbsPath" select="$node/@location"/-->
        <xsl:variable name="fwdSlashes" select="replace(string($relOrAbsPath), '\\', '/')"/>
        <xsl:variable name="isAbsPath"  select="matches($fwdSlashes, '^/|^.:')"/>
        <!--xsl:message select="concat('relOrAbsPath = ',string($relOrAbsPath))"/-->
        <xsl:choose>
          <xsl:when test="$isAbsPath">
            <xsl:sequence select="($fwdSlashes, $fwdSlashes)"/>  
          </xsl:when>
          <xsl:otherwise>
            <xsl:variable name="relBaseDir" select="replace(string($relBase), '/[^/]*?\.[xX][mM][lL]', '')"/>
            <xsl:variable name="relPath" select="concat($relBaseDir, '/', $fwdSlashes)"/>
            <xsl:choose>
              <xsl:when test="$useRelativePaths">
                <xsl:sequence select="($relPath, $relPath)"/>
              </xsl:when>
              <xsl:otherwise>
                <xsl:variable name="absPath" select="resolve-uri($relPath, $document-uri)"/>
                <xsl:sequence select="($absPath, $absPath)"/>
              </xsl:otherwise>
            </xsl:choose>
          </xsl:otherwise>
        </xsl:choose>   
      </xsl:when>
      <xsl:when test="$node/@name">
        <xsl:sequence select="(concat('*importByName:', $node/@name), x1:resolveByName($node/@name))"/> 
      </xsl:when> 
      <xsl:otherwise>
        <xsl:sequence select=
          "f:msgWithLineNumber('ERROR', 'import element missing name or location attribute', $node)"/>
      </xsl:otherwise>
    </xsl:choose>    
  </xsl:function>
 
  <!--xsl:function name="f:isRmtTempQ">
    <xsl:param name="rmtNode"/-->
    <!--xsl:message select="not($noTempQueues) and 
      ($rmtNode/u:replyQueue/@location eq 'remote') and
      ($topLevelInputQueueBroker ne 'vm://localhost') and
      ($rmtNode/u:inputQueue/@brokerURL eq $topLevelInputQueueBroker)"/-->
    <!--xsl:sequence select="not($noTempQueues) and 
      ($rmtNode/u:replyQueue/@location eq 'remote')"/>
  </xsl:function--> 
  
  <xsl:function name="f:isAggr">
    <xsl:param name="aeNode"/>
    <xsl:sequence select="$aeNode/(u:analysisEngineDescription|u:taeDescription)/u:primitive[text() eq 'false']"/>
  </xsl:function>

  <xsl:function name="f:isCPP">
    <xsl:param name="aeNode"/>
    <xsl:sequence select="$aeNode/*/u:frameworkImplementation[text() eq 'org.apache.uima.cpp']"/>
  </xsl:function>
  
  <xsl:function name="f:getUserHandlerDispatcherID">
    <xsl:param name="node"/>
    <xsl:sequence select="concat('usrHdlr_dispatcher', f:getAeNameUnique($node))"/>    
  </xsl:function>
    
  <xsl:function name="f:getTimeoutHandlerID">
    <xsl:param name="node"/>
    <xsl:sequence select="concat('tmOutHdlr_', f:getAeNameUnique($node))"/>
  </xsl:function>

  <xsl:function name="f:getExceptionHandlerID">
    <xsl:param name="node"/>
    <xsl:sequence select="concat('excptnHdlr_', f:getAeNameUnique($node))"/>
  </xsl:function>
  
  <xsl:function name="f:getUserErrorHandlerID">
    <xsl:param name="errorHandlerNode"/>
    <xsl:sequence select="concat(
      'user_error_handler_', 
      f:getAeNameUnique($errorHandlerNode), 
      '.', 
      $errorHandlerNode/position())"/>
  </xsl:function>
  
  <xsl:function name="f:getTimeoutThresholdsID">
    <xsl:param name="delegateNode"/> 
    <xsl:variable name="nodeHavingTimeoutThresholdsElement"
          select="$delegateNode/ancestor-or-self::*[u:errorConfiguration/u:errorHandling/u:timeout][1]"/>
    <xsl:sequence select="concat('timeout_thresholdsActions_', 
                      f:getAeNameUnique($nodeHavingTimeoutThresholdsElement))"/>    
  </xsl:function>

  <xsl:function name="f:getExceptionThresholdsID">
    <xsl:param name="delegateNode"/> 
    <xsl:variable name="nodeHavingExceptionThresholdsElement"
          select="$delegateNode/ancestor-or-self::*[u:errorConfiguration/u:errorHandling/u:exception][1]"/>
    <xsl:sequence select="concat('exception_thresholdsActions_', 
                      f:getAeNameUnique($nodeHavingExceptionThresholdsElement))"/>    
  </xsl:function>

  <xsl:function name="f:getUserErrorHandlerThresholdsID">
    <xsl:param name="delegateNode"/> 
    <xsl:variable name="nodeHavingUserErrorHandlerThresholdsElement"
          select="$delegateNode/ancestor-or-self::*[u:errorConfiguration/u:errorHandling/u:userErrorHandler/userErrorHandlerSpec][1]"/>
    <xsl:sequence select="concat('userErrorHandler_thresholdsActions_', 
                      f:getAeNameUnique($nodeHavingUserErrorHandlerThresholdsElement))"/>    
  </xsl:function>
      
  <xsl:function name="f:getThresholdActionID">
    <xsl:param name="node"/>
    <xsl:variable name="taKey" select="concat('t', $node/@threshold, 'a', $node/@action)"/>
    <xsl:sequence select="concat('threshold_action_', $taKey)"/>
  </xsl:function>
  
  <xsl:function name="f:getControllerID">
    <xsl:param name="aeNode"/>
    <xsl:choose>
      <xsl:when test="$aeNode/self::u:analysisEngine[@async eq 'true']">
        <xsl:sequence select="concat('asAggr_ctlr_', f:getAeNameUnique($aeNode))"/>    
      </xsl:when>
      <xsl:otherwise>
        <xsl:sequence select="concat('primitive_ctlr_', f:getAeNameUnique($aeNode))"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:function>
  
  <xsl:function name="f:getPoolingTaskExecutorID">
    <xsl:param name="aeNode"/>
    <xsl:sequence select="concat('pooling_', f:getAeNameUnique($aeNode))"/>    
  </xsl:function>
      
  <xsl:function name="f:getMetaMsgHandlerID">
    <xsl:param name="aeNode"/>
    <xsl:param name="kind"/>
    <xsl:sequence select="concat($kind, '_metaMsgHandler_', f:getAeNameUnique($aeNode))"/>
  </xsl:function>

  <xsl:function name="f:getProcessRequestHandlerID">
    <xsl:param name="aeNode"/>
    <xsl:param name="kind"/>
    <xsl:sequence select="concat($kind, '_processRequestHandler_', f:getAeNameUnique($aeNode))"/>
  </xsl:function>
  
  <xsl:function name="f:getProcessResponseHandlerID">
    <xsl:param name="aeNode"/>
    <xsl:param name="kind"/>
    <xsl:sequence select="concat($kind, '_processResponseHandler_', f:getAeNameUnique($aeNode))"/>
  </xsl:function>
  
  <xsl:function name="f:getDestinationResolverID">
    <xsl:param name="aeNameUnique"/>
    <xsl:param name="key"/>
    <xsl:sequence select="concat('destinationResolver_', $aeNameUnique, '_', $key)"/>
  </xsl:function>
  
  <xsl:function name="f:getAeNameUnique">
    <xsl:param name="aeNode"/>
    <xsl:if test="not($aeNode)">
      <xsl:sequence select="f:msgWithLineNumber('ERROR', 'empty node passed to f:getAeNameUnique', $aeNode)"/>
    </xsl:if>
    <xsl:sequence select="concat(f:getSimpleAeName($aeNode),'_', f:getUnique($aeNode))"/>
  </xsl:function>
  
  <xsl:function name="f:getUnique">
    <xsl:param name="node"/>
    <xsl:variable name="aeNode" select="$node/ancestor-or-self::node()[self::u:analysisEngine|self::u:remoteAnalysisEngine][1]"/>
    <xsl:number level="multiple" count="u:remoteAnalysisEngine|u:analysisEngine" format="1.1" select="$aeNode"/>
  </xsl:function>
  
  <xsl:function name="f:getSimpleAeName">
    <xsl:param name="node"/>
 <!--    <xsl:param tunnel="yes" name="topKey"/> -->
    <xsl:variable name="nearestKey" select=
      "$node/ancestor-or-self::node()[@key][1]"/>
   <!-- <xsl:message select="'*** Tracing getSimpleAeName'"/>
    <xsl:message select="$node/ancestor-or-self::node()[@key][1]"/>  
    <xsl:message select="'nearest key var is: '"/>
    <xsl:message select="$nearestKey"/>
    <xsl:message select="'end of nearest key'"/> -->
    <xsl:choose>
      <xsl:when test="$nearestKey">
        <xsl:sequence select="f:createValidName(string($nearestKey/@key))"/>
      </xsl:when>
      <xsl:otherwise>      
        <!--xsl:message select="'nearest key was false, using topkey'"/-->
    <!--    <xsl:sequence select="$topKey"/> -->
        <!--xsl:sequence select="f:getSimpleNameFromParentKeyOrEndpoint($node/ancestor::u:service/u:inputQueue/(@endpoint|@queueName))"/-->
        <xsl:sequence select="f:msgWithLineNumber(
          'ERROR', 
          ('Missing key name on', node-name($node)),
          $node)"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:function>
 
  <xsl:function name="f:createValidName">
    <xsl:param name="inputName"/>
    <xsl:variable name="u">
      <xsl:choose>
        <xsl:when test="starts-with($inputName, '${')">
          <xsl:sequence select=
            "replace(replace($inputName, '\$\{', ''), '\}', '')"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select=
            "replace(
              replace(
               replace(
                replace(
                 replace(
                  replace(
                   replace( $inputName,'\?.*', '')
                           ,',','_cm_')
                          ,':','_c_')
                         ,'//','_ss_')
                       ,'/', '_s_')
                      ,'\(','_op_')
                     ,'\)','_cp_')
                     "/>     
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
             
    <xsl:sequence select="$u"/>
  </xsl:function>         
 
 
  <xsl:function name="f:getQbrokerID">
    <xsl:param name="inputQueue"/>
    <!-- xsl:variable name="u">
      <xsl:choose>
        <xsl:when test="starts-with($inputQueue/@brokerURL, '${')">
          <xsl:sequence select=
            "replace(replace($inputQueue/@brokerURL, '\$\{', ''), '\}', '')"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select=
            "replace(
              replace(
               replace(
                replace(
                 replace(
                  replace(
                   replace( $inputQueue/@brokerURL,'\?.*', '')
                           ,',','_cm_')
                          ,':','_c_')
                         ,'//','_ss_')
                       ,'/', '_s_')
                      ,'\(','_op_')
                     ,'\)','_cp_')
                     "/>     
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable-->
             
    <xsl:sequence select="concat('qBroker_',f:createValidName($inputQueue/@brokerURL))"/>
  </xsl:function>         
  
  <xsl:function name="f:getEndpointName">
    <xsl:param name="containerKeyName" as="xs:string"/>  <!-- key name of containing delegate -->
    <xsl:param name="uniq"/>
    <!-- xsl:message select="'*** Tracing GetEndpointName'"/ -->

    <xsl:sequence select="concat('endpt_', $containerKeyName, '_', $uniq)"/>
  </xsl:function>
                      
  <xsl:function name="f:getOutputChannelID">
    <xsl:param name="aeNode"/>
    <xsl:sequence select="concat('outChnl_', f:getAeNameUnique($aeNode))"/> 
  </xsl:function>
  
  <xsl:function name="f:getInternalInputQueueName">
    <xsl:param name="delegateNode"/>  
    <xsl:sequence select="concat('inQ_',f:getAeNameUnique($delegateNode))"/>
  </xsl:function>
  
  <xsl:function name="f:getLocalReturnQueueEndpointID">
    <xsl:param name="aeNode"/>
    <xsl:sequence select="concat('asynAggr_retQ_', f:getAeNameUnique($aeNode))"/>
  </xsl:function>

  <xsl:function name="f:getRemoteReturnQueueID">
    <xsl:param name="aeNode"/>
    <xsl:param name="remoteDelegate"/>
    <xsl:sequence select="concat('rmtRtrnQ_', f:getAeNameUnique($aeNode), 
        '_', $remoteDelegate/@key)"/>
  </xsl:function>

  <xsl:function name="f:getRemoteReturnQueueName">
    <xsl:param name="aeNode"/>
    <xsl:param name="remoteDelegate"/>
    <xsl:sequence select="concat(f:getRemoteReturnQueueID($aeNode, $remoteDelegate), '_', $guid)"/>
  </xsl:function>
        
  <xsl:function name="f:getDelegatePart">
    <xsl:param name="local_ae_descriptor"/>
    <xsl:param name="key"/>
    <!-- xsl:message select="'*** local ae descriptor'"/>
    <xsl:message select="$local_ae_descriptor"/>
    <xsl:message select="$key"/>
    <xsl:message select="$local_ae_descriptor/(u:analysisEngineDescription|u:casConsumerDescription|u:taeDescription)/u:delegateAnalysisEngineSpecifiers/u:delegateAnalysisEngine[@key eq string($key)]"/>
    <xsl:message select="'*** end local ae descriptor'"/ -->
    <xsl:variable name="result" select=
        "f:resolveImport($local_ae_descriptor/(u:analysisEngineDescription|u:taeDescription)/u:delegateAnalysisEngineSpecifiers/u:delegateAnalysisEngine[@key=$key])"/>
    <xsl:if test="not($result)">
      <xsl:sequence select="f:msgWithLineNumber(
        'ERROR', 
        ('ERROR cannot load delegate descriptor with key', $key),
        $local_ae_descriptor)"/> 
    </xsl:if>
    <xsl:sequence select="$result"/>
  </xsl:function>  
  
  <xsl:function name="f:resolveImport">
    <xsl:param name="nodeset"/>
    <xsl:choose>
      <xsl:when test="$nodeset/u:import">
        <xsl:sequence select="f:getImport($nodeset/u:import)"/>   
      </xsl:when>
      <xsl:otherwise>
        <!--xsl:message select="'*** This nodeset didnt have an import '"/>
        <xsl:message select="$nodeset"/-->
        <xsl:sequence select="$nodeset"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:function>
        
  <xsl:function name="f:getTimeoutClass">
    <xsl:param name="timeout"/>
    <xsl:choose>
      <xsl:when test="$timeout/@event eq 'metadataRequest'">
        <xsl:sequence select="'MetadataRequestTimeoutCount'"/>
      </xsl:when>
      <xsl:when test="$timeout/@event eq 'processRequest'">
        <xsl:sequence select="'ProcessRequestTimeoutCount'"/>
      </xsl:when>
      <xsl:when test="$timeout/@event eq 'collectionProcessingCompleteRequest'">
        <xsl:sequence select="'CollectionProcessingCompleteTimeoutCount'"/>
      </xsl:when>
    </xsl:choose>
  </xsl:function>
  
  <xsl:function name="f:getErrorHandlerChainID">
    <xsl:param name="node"/>
    <!-- xsl:message select="' *** ancestor test ErrorConfig *** '"/>
    <xsl:message select="$node/parent::node()"/ -->
    
    <!--xsl:variable name="firstContainingNodeWithErrorConfig" select=
      "if  ($node/ancestor-or-self::u:analysisEngine[u:delegates/*/u:errorConfiguration]) 
       then $node/ancestor-or-self::u:analysisEngine[u:delegates/*/u:errorConfiguration][1]
       else $node/ancestor-or-self::node()[u:errorConfiguration][1]"/-->
    <!--xsl:message select="'*** first containing node with error config parent'"/>
    <xsl:message select="$firstContainingNodeWithErrorConfig/parent::node()"/ -->
    <xsl:sequence select=
          "concat('err_hdlr_chn_', f:getAeNameUnique($node))"/>
   </xsl:function>
  <!--============================================================-->       
  <!--                Comment Generation                         =-->       
  <!--============================================================-->
  <xsl:variable name="nl" select="'&#xA;'"/>
  <xsl:variable name="quote" select="'&#x22;'"/>
  <xsl:variable name="nl2" select="'&#xA;&#xA;'"/>
  <xsl:variable name="commentBorder" select=
    "'=================================================================================='"/>
  <xsl:variable name="blanks" select=
    "'                                                                                  '"/>
  <xsl:function name="f:generateBlockComment">
    <xsl:param name="comment"/>
    <xsl:param name="spaces"/>
    <xsl:call-template name="commentGenerate">
      <xsl:with-param name="comment" select="$comment"/>
      <xsl:with-param name="spaces" select="$spaces"/>
    </xsl:call-template>
  </xsl:function>

  <xsl:function name="f:generateLineComment">
    <xsl:param name="comment"/>
    <xsl:param name="spaces"/>
    <xsl:call-template name="commentGenerateLine">
      <xsl:with-param name="comment" select="$comment"/>
      <xsl:with-param name="spaces" select="$spaces"/>
    </xsl:call-template>
  </xsl:function>
  
  <xsl:template name="commentGenerate">
    <xsl:param name="comment"/>
    <xsl:param name="spaces"/>
    <xsl:variable name="maxLength" select="max((20, 2 + max(for $s in $comment return string-length(string($s)))))"/>
    
    <xsl:sequence select="$nl"/>
    <xsl:comment select="substring($commentBorder,1,$maxLength)"/>
    <xsl:sequence select="$nl"/>
    <xsl:value-of select="substring($blanks,1,$spaces)"/>   
    <xsl:for-each select="$comment">
      <xsl:comment select="concat(' ', ., substring($blanks, 1, $maxLength - (string-length(string(.)) + 1)))"/>
      <xsl:sequence select="$nl"/>
      <xsl:value-of select="substring($blanks,1,$spaces)"/>      
    </xsl:for-each>
    <xsl:comment select="substring($commentBorder,1,$maxLength)"/>
    <xsl:sequence select="$nl"/>
    <xsl:value-of select="substring($blanks,1,$spaces)"/>   
  </xsl:template>
  
  <xsl:template name="commentGenerateLine">
    <xsl:param name="comment"/>
    <xsl:param name="spaces"/>
    <xsl:sequence select="$nl2"/>
    <xsl:value-of select="substring($blanks,1,$spaces)"/>   
    <xsl:variable name="maxLength" select="max((20, 2 + max(for $s in $comment return string-length(string($s)))))"/>
    <xsl:for-each select="$comment">
      <xsl:variable name="stretched" select="f:stretchComment(., $maxLength)"/>
      <xsl:variable name="comment2" select="f:evenLengthString($stretched)"/>
      <xsl:variable name="padLength" select=
        "(max((0, $maxLength - string-length($comment2)))) div 2"/> 
      <xsl:variable name="c" select="substring($blanks, 1, $padLength)"/>
      <xsl:comment select="concat($c, $comment2, $c)"/>
      <xsl:sequence select="$nl"/>
      <xsl:value-of select="substring($blanks,1,$spaces)"/>
    </xsl:for-each>
  </xsl:template>

  <xsl:variable name="lotsOfTildes"
    select="'~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'"/>
  <xsl:function name="f:stretchComment">
    <xsl:param name="s"/>
    <xsl:param name="length" />
    <xsl:sequence select=
      "if (starts-with($s, '~~~~~~~~~~')) then substring($lotsOfTildes, 1, min(($length, string-length($lotsOfTildes))))
                                          else $s"/>
  </xsl:function>
  
  <xsl:function name="f:evenLengthString">
    <xsl:param name="s"/>
       <xsl:choose>
        <xsl:when test="(string-length($s) mod 2) = 1">
          <xsl:sequence select="concat($s, ' ')"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select="$s"/>
        </xsl:otherwise>
      </xsl:choose>
  </xsl:function>
  
  <!--============================================================-->       
  <!--           validation                                      =-->       
  <!--============================================================-->
  
  <xsl:variable name="validDeploymentDescriptorDefinition">
    <u:analysisEngineDeploymentDescription>
      <u:name i:maxone=""/>
      <u:description i:maxone=""/>
      <u:version i:maxone=""/>
      <u:vendor i:maxone=""/>
      <u:deployment i:maxone="" i:required="" protocol="" provider="">
          <u:casPool i:maxone="" numberOfCASes=""  initialFsHeapSize="" disableJCasCache=""/>
        
          <u:service i:required="">
            <u:custom name="" value=""/>
            <u:inputQueue i:maxone="" i:required="" brokerURL="" endpoint="" queueName="" prefetch=""/>
            <u:topDescriptor i:maxone="" i:required="">
              <u:import i:maxone="" i:required="" location="" name=""/>
            </u:topDescriptor>
            <u:environmentVariables i:maxone="">
              <u:environmentVariable name=""/>
            </u:environmentVariables>
            <u:analysisEngine i:maxone="" key="" async="" 
                internalReplyQueueScaleout=""
                inputQueueScaleout="">
              <u:scaleout i:maxone="" numberOfInstances=""/>
                <!-- top level cas multiplier can't specify processParentLast -->
              <u:casMultiplier i:maxone="" poolSize="" initialFsHeapSize="" disableJCasCache="" />
              <u:asyncPrimitiveErrorConfiguration i:maxone="">
                <u:processCasErrors i:maxone="" 
                  thresholdCount="" thresholdWindow="" thresholdAction=""/>
                <u:collectionProcessCompleteErrors i:maxone="" additionalErrorAction=""/>
              </u:asyncPrimitiveErrorConfiguration>
              <u:asyncAggregateErrorConfiguration i:maxone="">
                <u:getMetadataErrors i:maxone="" maxRetries="" timeout="" errorAction=""/>
                <u:processCasErrors i:maxone="" maxRetries="" timeout="" continueOnRetryFailure=""
                      thresholdCount="" thresholdWindow="" thresholdAction=""/>
                <u:collectionProcessCompleteErrors i:maxone="" timeout="" additionalErrorAction=""/>
              </u:asyncAggregateErrorConfiguration>
              <u:delegates i:maxone="">
                <u:analysisEngine/>
                <u:remoteAnalysisEngine key="" remoteReplyQueueScaleout="">
                  <u:casMultiplier i:maxone="" poolSize="" initialFsHeapSize="" processParentLast="" disableJCasCache="" />
                  <u:inputQueue i:maxone="" i:required="" brokerURL="" endpoint="" queueName=""/>
                  <u:replyQueue i:maxone="" location=""/>
                  <u:serializer i:maxone="" method=""/>
                  <u:asyncAggregateErrorConfiguration i:maxone="">
                    <u:getMetadataErrors i:maxone="" maxRetries="" timeout="" errorAction=""/>
                    <u:processCasErrors i:maxone="" maxRetries="" timeout="" continueOnRetryFailure=""
                      thresholdCount="" thresholdWindow="" thresholdAction=""/>
                    <u:collectionProcessCompleteErrors i:maxone="" timeout="" additionalErrorAction=""/>
                  </u:asyncAggregateErrorConfiguration>
                </u:remoteAnalysisEngine>
              </u:delegates>
            </u:analysisEngine>
          </u:service>   
      </u:deployment>
    </u:analysisEngineDeploymentDescription>
  </xsl:variable>
  
  <xsl:key name="validElement" match="*" use="node-name(.)"/>
   
  <xsl:function name="f:validateDeploymentDescriptor">
    <xsl:param name="top"/>
    <xsl:if test="not($top)">
      <xsl:message select="('*** ERROR: The top level item is not an analysisEngineDeploymentDescription')"/>
    </xsl:if>
    <xsl:sequence select="f:validateDDNodeAndChildren($top)"/>
  </xsl:function>
  
  <xsl:function name="f:validateDDNodeAndChildren">
    <xsl:param name="node"/>
    <!--xsl:message select="('validating:', node-name($node))"/-->
    <xsl:variable name="node2" as="node()">
      <xsl:choose>
        <xsl:when test="$node/u:errorConfiguration/u:import">
          <xsl:sequence select="f:getImport($node/u:errorConfiguration/u:import)"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:sequence select="$node"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <!--xsl:message select="('validating2:', node-name($node2))"/-->
    <xsl:sequence select="f:validate($node2)"/>
    <xsl:for-each select="$node2/*">
      <xsl:sequence select="f:validateDDNodeAndChildren(.)"/>
    </xsl:for-each>
  </xsl:function>
  
  <xsl:function name="f:validate">
    <xsl:param name="node"/>
    <xsl:variable name="p" select="key('validElement', node-name($node), $validDeploymentDescriptorDefinition)"/>
    
    <xsl:for-each select="$node/attribute::*">
      <xsl:if test="not(node-name(.) = $p/attribute::*/node-name(.))">
        <xsl:sequence select="f:msgWithLineNumber('ERROR',
          ('following element has an unknown attribute: ', node-name(.)), $node)"/>
        <xsl:message select="$node"/>
      </xsl:if>
    </xsl:for-each>
    
    <xsl:for-each select="$p/*[@i:maxone]">
      <xsl:if test="1 &lt; count($node/*[node-name(.) eq node-name(current())])">
        <xsl:sequence select="f:msgWithLineNumber(
        'ERROR',
        (concat('&quot;', node-name(.),     '&quot;'),
         'occurs more than once within',
         concat('&quot;', node-name($node), '&quot;,'),
         'but may occur a maximum of 1 time in this context'
        ),
        $node)"/>  
      <!--xsl:message select="$node"/--> 
      </xsl:if>
    </xsl:for-each>
    
    <xsl:for-each select="$p/*[@i:required]">
      <xsl:if test="not($node/*[node-name(.) eq node-name(current())])">
        <xsl:sequence select="f:msgWithLineNumber(
          'ERROR',
          (concat('&quot;', node-name($node),   '&quot;'),
           'in this context must contain the element',
            concat('&quot;', node-name(.), '&quot;;'),
           'but that element is missing'
          ),
          $node)"/>  
      </xsl:if>
    </xsl:for-each>
      
    <xsl:if test="$node/u:inputQueue/@queueName">
      <xsl:sequence select="f:msgWithLineNumber(
        'WARNING',
        (concat($quote,'queueName',$quote), 
         'attribute was used in an inputQueue element; it has been changed to', concat($quote, 'endpoint', $quote)),
        $node)"/>
    </xsl:if>
    
    <xsl:for-each select="$node/*">
      <!--xsl:message select="('node-name:',node-name(.))"/-->
      <xsl:if test="not(node-name(.) = $p/*/node-name(.))">
        <xsl:message select="f:msgWithLineNumber('ERROR',
          ('The element', node-name(.), 'cannot be contained in the element', node-name($node)),
          $node)"/>
      <!--xsl:message select="'*** p/*/node-name(.)'"/>
      <xsl:message select="$p/*/node-name(.)"/-->
      </xsl:if>
    </xsl:for-each>
    
    <xsl:for-each select="$node/u:errorConfiguration/u:errorHandling/u:timeout">
      <xsl:if test="not(string(@event) = 
        ('processRequest', 'metadataRequest', 'collectionProcessingCompleteRequest'))">
        <xsl:message select="f:msgWithLineNumber(
          'ERROR',
          ('This timeout element needs to specify an event which is one of',
           'processRequest, metadataRequest, or collectionProcessingCompleteRequest'),
          $node/u:errorConfiguration/u:errorHandling)"/>  
      </xsl:if>      
    </xsl:for-each>
   </xsl:function>

  <xsl:function name="f:msgWithLineNumber">
    <xsl:param name="kind"/>
    <xsl:param name="msg"/>  <!-- can be multi-element sequence -->
    <xsl:param name="node"/>
    <xsl:message>
      *** <xsl:sequence select="concat($kind,':')"/> line-number: <xsl:sequence select="saxon:line-number($node)"/>
      <xsl:sequence select="$msg"/> 
    </xsl:message>
  </xsl:function>
</xsl:stylesheet>
