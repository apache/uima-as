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

import org.apache.uima.UIMAFramework;
import org.apache.uima.aae.UIMAEE_Constants;
import org.apache.uima.aae.controller.AnalysisEngineController;
import org.apache.uima.aae.message.AsynchAEMessage;
import org.apache.uima.aae.spi.transport.UimaMessage;
import org.apache.uima.util.Level;

public class UimaMessageValidator {
  private static final Class CLASS_NAME = UimaMessageValidator.class;

  private static String getEndpointName(UimaMessage aMessage) throws IllegalArgumentException {
    String endpointName = aMessage.getStringProperty(AsynchAEMessage.MessageFrom);
    if (endpointName == null) {
      throw new IllegalArgumentException("Invalid Message. Missing 'MessageFrom' Property.");
    }
    return endpointName;
  }

  /**
   * Validate message type contained in the JMS header.
   * 
   * @param aMessage
   *          - jms message retrieved from queue
   * @param properties
   *          - map containing message properties
   * @return
   * @throws Exception
   */

  public static boolean validMessageType(UimaMessage aMessage, String endpointName)
          throws Exception {

    if (aMessage.containsProperty(AsynchAEMessage.MessageType)) {
      int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
      if (msgType != AsynchAEMessage.Response && msgType != AsynchAEMessage.Request) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validMessageType", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_msgtype_in_message__INFO",
                  new Object[] { msgType, endpointName });
        }
        return false;
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                "validMessageType", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                "UIMAJMS_msgtype_notin_message__INFO", new Object[] { endpointName });
      }
      return false;
    }

    return true;
  }

  public static boolean isRequest(UimaMessage aMessage) throws Exception {
    if (aMessage.containsProperty(AsynchAEMessage.MessageType)) {
      int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
      if (msgType != AsynchAEMessage.Request) {
        return false;
      }
      return true;
    }
    return false;
  }

  public static boolean isProcessRequest(UimaMessage aMessage) throws Exception {
    if (!isRequest(aMessage)) {
      return false;
    }
    if (aMessage.containsProperty(AsynchAEMessage.Command)) {
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      if (command != AsynchAEMessage.Process) {
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Validate command contained in the header of the JMS Message
   * 
   * @param aMessage
   *          - JMS Message received
   * @param properties
   *          - Map containing header properties
   * @return - true if the command received is a valid one, false otherwise
   * @throws Exception
   */
  public static boolean validCommand(UimaMessage aMessage, String endpointName) throws Exception {
    if (aMessage.containsProperty(AsynchAEMessage.Command)) {
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      if (command != AsynchAEMessage.Process && command != AsynchAEMessage.GetMeta
              && command != AsynchAEMessage.ReleaseCAS && command != AsynchAEMessage.Stop
              && command != AsynchAEMessage.Ping
              && command != AsynchAEMessage.CollectionProcessComplete) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validCommand", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_command_in_message__INFO",
                  new Object[] { command, endpointName });
        }
        return false;
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "validCommand",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_command_notin_message__INFO",
                new Object[] { endpointName });
      }
      return false;
    }

    return true;
  }

  /**
   * Validates payload in the JMS Message.
   * 
   * @param aMessage
   *          - JMS Message received
   * @param properties
   *          - Map containing header properties
   * @return - true if the payload is valid, false otherwise
   * @throws Exception
   */
  public static boolean validPayload(UimaMessage aMessage, String endpointName) throws Exception {
    if (aMessage.containsProperty(AsynchAEMessage.Command)) {
      int command = aMessage.getIntProperty(AsynchAEMessage.Command);
      if (command == AsynchAEMessage.GetMeta
              || command == AsynchAEMessage.CollectionProcessComplete
              || command == AsynchAEMessage.Ping || command == AsynchAEMessage.Stop
              || command == AsynchAEMessage.ReleaseCAS) {
        // Payload not included in GetMeta Request
        return true;
      }
    }

    if (aMessage.containsProperty(AsynchAEMessage.Payload)) {
      int payload = aMessage.getIntProperty(AsynchAEMessage.Payload);
      if (payload != AsynchAEMessage.XMIPayload && payload != AsynchAEMessage.CASRefID
              && payload != AsynchAEMessage.BinaryPayload && payload != AsynchAEMessage.Exception
              && payload != AsynchAEMessage.Metadata) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(),
                  "validPayload", UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_invalid_payload_in_message__INFO",
                  new Object[] { payload, endpointName });
        }
        return false;
      }
    } else {
      if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.INFO)) {
        UIMAFramework.getLogger(CLASS_NAME).logrb(Level.INFO, CLASS_NAME.getName(), "validPayload",
                UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE, "UIMAJMS_payload_notin_message__INFO",
                new Object[] { endpointName });
      }
      return false;
    }

    return true;
  }

  public static boolean isStaleMessage(UimaMessage aMessage, boolean isStopped,
          String endpointName, boolean entryExists) {
    if (isStopped) {
      // Shutting down
      return true;
    }
    int command = aMessage.getIntProperty(AsynchAEMessage.Command);
    int msgType = aMessage.getIntProperty(AsynchAEMessage.MessageType);
    if (command == AsynchAEMessage.Process && msgType == AsynchAEMessage.Response) {
      String casReferenceId = aMessage.getStringProperty(AsynchAEMessage.CasReference);
      if (!entryExists) {
        if (UIMAFramework.getLogger(CLASS_NAME).isLoggable(Level.FINE)) {
          UIMAFramework.getLogger(CLASS_NAME).logrb(
                  Level.FINE,
                  CLASS_NAME.getName(),
                  "isStaleMessage",
                  UIMAEE_Constants.JMS_LOG_RESOURCE_BUNDLE,
                  "UIMAJMS_stale_message__FINE",
                  new Object[] { endpointName, casReferenceId,
                      aMessage.getStringProperty(AsynchAEMessage.MessageFrom) });
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Validates contents of the message. It checks if command, payload and message types contain
   * valid data.
   * 
   * @param aMessage
   *          - JMS Message to validate
   * @return - true if message is valid, false otherwise
   * @throws Exception
   */
  public static boolean isValidMessage(UimaMessage aMessage, AnalysisEngineController controller)
          throws Exception {
    String endpointName = getEndpointName(aMessage);
    if (!validMessageType(aMessage, endpointName)) {
      return false;
    }
    if (!validCommand(aMessage, endpointName)) {
      return false;
    }
    if (!validPayload(aMessage, endpointName)) {
      return false;
    }
    String casReferenceId = aMessage.getStringProperty(AsynchAEMessage.CasReference);

    if (isStaleMessage(aMessage, controller.isStopped(), endpointName, controller
            .getInProcessCache().entryExists(casReferenceId))) {
      return false;
    }
    return true;
  }

  public static String decodeIntToString(String aTypeToDecode, int aValueToDecode) {
    if (AsynchAEMessage.MessageType.equals(aTypeToDecode)) {
      switch (aValueToDecode) {
        case AsynchAEMessage.Request:
          return "Request";
        case AsynchAEMessage.Response:
          return "Response";
      }
    } else if (AsynchAEMessage.Command.equals(aTypeToDecode)) {
      switch (aValueToDecode) {
        case AsynchAEMessage.Process:
          return "Process";
        case AsynchAEMessage.GetMeta:
          return "GetMetadata";
        case AsynchAEMessage.CollectionProcessComplete:
          return "CollectionProcessComplete";
        case AsynchAEMessage.ReleaseCAS:
          return "ReleaseCAS";
        case AsynchAEMessage.Stop:
          return "Stop";
        case AsynchAEMessage.Ping:
          return "Ping";
      }

    } else if (AsynchAEMessage.Payload.equals(aTypeToDecode)) {
      switch (aValueToDecode) {
        case AsynchAEMessage.XMIPayload:
          return "XMIPayload";
        case AsynchAEMessage.BinaryPayload:
          return "BinaryPayload";
        case AsynchAEMessage.CASRefID:
          return "CASRefID";
        case AsynchAEMessage.Metadata:
          return "Metadata";
        case AsynchAEMessage.Exception:
          return "Exception";
          // 5/2013 xcas not used
//        case AsynchAEMessage.XCASPayload:
//          return "XCASPayload";
        case AsynchAEMessage.None:
          return "None";
      }
    }
    return "UNKNOWN";
  }

}
