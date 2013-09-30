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

package org.apache.uima.aae.message;

public interface AsynchAEMessage {
  public static final String TotalTimeSpentInAnalytic = "TimeInAnalytic";

  public static final String TimeInService = "TimeInService";

  public static final String Endpoint = "Endpoint";

  public static final String DelegateStats = "DelegateStats";

  public static final String RequestFailed = "RequestFailed";

  public static final String CasReference = "CasReference";

  public static final String InputCasReference = "InputCasReference";

  public static final String MessageFrom = "MessageFrom";

  // 5/2013 xcas not used as a serialization form
//  public static final String XCASREFId = "XCASRefId";

//  public static final String XCas = "XCas";

  public static final String AEMetadata = "Metadata";

  public static final String CasSequence = "CasSequence";

  public static final String ReplyToEndpoint = "ReplyToEndpoint";

  public static final String EndpointServer = "EndpointServer";

  public static final String ServerIP = "ServerIP";
  
  public static final String UimaASProcessPID = "ProcessPID";

  public static final String RemoveEndpoint = "RemoveEndpoint";

  public static final String Aborted = "Aborted";

  public static final String TimeToSerializeCAS = "TimeToSerializeCAS";

  public static final String TimeToDeserializeCAS = "TimeToDeserializeCAS";

  public static final String TimeWaitingForCAS = "TimeWaitingForCAS";

  public static final String TimeInProcessCAS = "TimeInProcessCAS";

  public static final String CASPerComponentMetrics = "CASPerComponentMetrics";

  public static final String IdleTime = "IdleTime";

  public static final String CAS = "CAS";

  public static final String Cargo = "Cargo";

  public static final String SkipSubordinateCountUpdate = "SkipSubordinateCountUpdate";

  public static final String SkipPendingLists = "SkipPendingLists";

  public static final String Payload = "Payload";

  public static final int XMIPayload = 1000;

  public static final int CASRefID = 1001;

  public static final int Metadata = 1002;

  public static final int Exception = 1003;

  // 5/2013 xcas not used as a serialization form
//  public static final int XCASPayload = 1004;

  public static final int None = 1005;

  public static final int BinaryPayload = 1006;

  public static final String Command = "Command";

  public static final int Process = 2000;

  public static final int GetMeta = 2001;

  public static final int CollectionProcessComplete = 2002;

  public static final int Terminate = 2003;

  public static final int ACK = 2004;

  public static final int ReleaseCAS = 2005;

  public static final int Stop = 2006;

  public static final int Ping = 2007;

  public static final int ServiceInfo = 2008;

  public static final String MessageType = "MessageType";

  public static final int Request = 3000;

  public static final int Response = 3001;

  public static final String AcceptsDeltaCas = "AcceptsDeltaCas";

  public static final String SentDeltaCas = "SentDeltaCas";

  public static final String SERIALIZATION = "Serialization";
  
  /*
   * Serialization codes are done as a bit position, to allow 
   * future expansion - 
   *   Implementations will be able to turn on bits to indicate what they support
   *   Code:
   *   0x 01 bb bb bb
   *   where the bb bb bb represent 24 bits of alternatives being supported
   *   
   *   XMI is always supported, and has a code of 01 00 00 00
   *     or for old services, 4000 (but wasn't used in old code)
   *     (Binary was 4001 but that wasn't used in old code)
   *     
   */

  public static final int XMI_SERIALIZATION = 0x01000000;  

  public static final int BINARY_SERIALIZATION = 0x01000001; // supports uncompressed binary serialization, identical ts required, but not compressed binary serialization
  
  public static final int BINARY_COMPRESSED_FILTERED_SERIALIZATION = 0x01000002;  // supports use of BinaryCasSerDes6

  public static final String ErrorCause = "Cause";

  public static final int PingTimeout = 5001;
}
