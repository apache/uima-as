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

/**
 * 
 *
 */
public interface AEDeploymentConstants 
{
    final static int        DEFAULT_CAS_POOL_SIZE       = 5;
    final static int        DEFAULT_CAS_INITIAL_HEAP_SIZE = 2000000;
    final static int        DEFAULT_SCALEOUT_INSTANCES  = 1;
    final static int        DEFAULT_CAS_MULTIPLIER_POOL_SIZE  = 1;
    final static boolean    DEFAULT_ASYNC               = false;
    final static int        DEFAULT_GETMETADATA_TIMEOUT = 60000;
    final static int        DEFAULT_GETMETADATA_NO_TIMEOUT = 0;
    final static int        DEFAULT_PROCESSCASERROR_TIMEOUT  = 0;
    final static int        DEFAULT_COLLPROCESSCOMPLETEERROR_TIMEOUT  = 0;
    final static int        DEFAULT_MAX_RETRIES         = 0;
    final static int        DEFAULT_THRESHOLD_COUNT     = 0;
    final static int        DEFAULT_THRESHOLD_WINDOW    = 0;
    final static String     DEFAULT_THRESHOLD_ACTION    = "terminate";
    final static String     DEFAULT_ERROR_ACTION        = "terminate";
    final static String     DEFAULT_ADDITIONAL_ERROR_ACTION = "terminate";
    final static boolean    DEFAULT_CONTINUE_ON_RETRY_FAILURE = false;
    final static String     DEFAULT_REPLY_QUEUE_LOCATION = "local";
    final static int        DEFAULT_CONCURRENT_CONSUMERS = 1;

    final static public int    UNDEFINED_INT   = -1; // used to identify undefined integer

    ///////////////////////////////////////////////////////////////////////////

    final static public String ERROR_KIND_STRING_NO_TIMEOUT     = "no timeout";
    final static public String ERROR_KIND_STRING_NO_RETRIES     = "no retries";
    final static public String ERROR_KIND_STRING_NO_THRESHOLD_COUNT = "no threshold";
    final static public String ERROR_KIND_STRING_NO_THRESHOLD_WINDOW = "no window";
    final static public String ERROR_KIND_STRING_NO_THRESHOLD_ACTION = "no action";
    
    /*    
    // GetMetadataErrors
    final static public int ERROR_KIND_MAX_RETRIES          = 1;
    final static public int ERROR_KIND_KIND_ERRORACTION     = 3;
    
    // For ProcessCasErrors
    final static int        KIND_THRESHOLD_COUNT    = 1;
    final static int        KIND_THRESHOLD_WINDOW   = 2;
    final static int        KIND_THRESHOLD_ACTION   = 3;
    final static int        KIND_TIMEOUT            = 4;
    final static int        KIND_MAX_RETRIES        = 5;
    final static int        KIND_CONTINUE_ON_RETRY  = 6;

    // CollectionProcessCompleteErrors
    final static public int        KIND_CPC_TIMEOUT                = 1;
    final static public int        KIND_ADDITIONA_ERROR_ACTION = 2;
*/
    ///////////////////////////////////////////////////////////////////////////
    
    final static public String TAG_NAME                = "name";
    final static public String TAG_DESCRIPTION         = "description";
    final static public String TAG_VERSION             = "version";
    final static public String TAG_VENDOR              = "vendor";
    final static public String TAG_DEPLOYMENT          = "deployment";
    final static public String TAG_CASPOOL             = "casPool";
    final static public String TAG_SERVICES            = "services";
    final static public String TAG_SERVICE             = "service";
    final static public String TAG_ANALYSIS_ENGINE     = "analysisEngine";
    final static public String TAG_REMOTE_DELEGATE     = "remoteAnalysisEngine";

    final static public String TAG_INPUT_QUEUE             = "inputQueue";
    final static public String TAG_TOP_DESCRIPTOR          = "topDescriptor";
    final static public String TAG_IMPORT                  = "import";

    final static public String TAG_REPLY_QUEUE             = "replyQueue";
    final static public String TAG_ATTR_INPUT_QUEUE_SCALEOUT = "inputQueueScaleout";
    final static public String TAG_ATTR_INTERNAL_REPLYQUEUE_SCALEOUT = "internalReplyQueueScaleout";
    final static public String TAG_ATTR_REMOTE_REPLYQUEUE_SCALEOUT = "remoteReplyQueueScaleout";
    
    final static public String TAG_SERIALIZER              = "serializer";
    final static public String TAG_ATTR_METHOD             = "method";

    final static public String TAG_SCALE_OUT               = "scaleout";
    final static public String TAG_CAS_MULTIPLIER          = "casMultiplier";
    final static public String TAG_DELEGATES               = "delegates";
    final static public String TAG_ERROR_CONFIGURATION     = "errorConfiguration";
    final static public String TAG_ASYNC_AGGREGATE_ERROR_CONFIGURATION = "asyncAggregateErrorConfiguration";
    final static public String TAG_ASYNC_PRIMITIVE_ERROR_CONFIGURATION = "asyncPrimitiveErrorConfiguration";
    
    // Error Configuration
    final static public String TAG_GET_METADATA_ERRORS             = "getMetadataErrors";
    final static public String TAG_PROCESS_CAS_ERRORS              = "processCasErrors";
    final static public String TAG_COLLECTION_PROCESS_COMPLETE_ERRORS = "collectionProcessCompleteErrors";
    final static public String TAG_ATTR_MAX_RETRIES    = "maxRetries";
    final static public String TAG_ATTR_TIMEOUT        = "timeout";
    final static public String TAG_ATTR_ERROR_ACTION   = "errorAction";
    final static public String TAG_ATTR_CONTINUE_ON_RETRY_FAILURE = "continueOnRetryFailure";
    final static public String TAG_ATTR_THRESHOLD_COUNT = "thresholdCount";
    final static public String TAG_ATTR_THRESHOLD_WINDOW = "thresholdWindow";
    final static public String TAG_ATTR_THRESHOLD_ACTION = "thresholdAction";
    final static public String TAG_ATTR_ADDITIONAL_ERROR_ACTION = "additionalErrorAction";
    
    
    final static public String TAG_ATTR_PROVIDER           = "provider";

    final static public String TAG_ATTR_END_POINT          = "endpoint";
    final static public String TAG_ATTR_BROKER_URL         = "brokerURL";
    final static public String TAG_ATTR_PREFETCH           = "prefetch";
    final static public String TAG_ATTR_LOCATION           = "location";
    final static public String TAG_ATTR_NAME               = "name";
    final static public String TAG_ATTR_PROTOCOL           = "protocol";
    final static public String TAG_ATTR_NUMBER_OF_CASES    = "numberOfCASes";
    final static public String TAG_ATTR_INIT_SIZE_OF_CAS_HEAP = "initialFsHeapSize";

    final static public String TAG_ATTR_PROCESS_PARENT_CAS_LAST = "processParentLast";

    final static public String TAG_ATTR_KEY                = "key";
    final static public String TAG_ATTR_ASYNC              = "async";
    final static public String TAG_ATTR_NUMBER_OF_INSTANCES = "numberOfInstances";
    final static public String TAG_ATTR_POOL_SIZE          = "poolSize";

    // C++ Support
    // Environment Variables (name/value pair)
    final static public String TAG_CUSTOM           = "custom";
    final static public String TAG_CUSTOM_ATTR_NAME = "run_top_level_CPP_service_as_separate_process";
    final static public String TAG_ENV_VAR          = "environmentVariable";
    final static public String TAG_ENV_VARS         = "environmentVariables";

}
