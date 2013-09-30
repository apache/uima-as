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

import org.apache.uima.util.InvalidXMLException;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class DDParserUtil {


  /**
   * Check and get the attribute value 
   * 
   * @param parentElementName
   * @param attributeName
   * @param aElement
   * @throws InvalidXMLException
   * @return String
   */
  static public String checkAndGetAttributeValue (String parentElementName, String attributeName, 
          Element aElement, boolean throwException) throws InvalidXMLException {
    // Check for attributeName attribute
    boolean found = false;
    String val = null;
    NamedNodeMap map = aElement.getAttributes();
    if (map != null) {
      for (int i=0; i<map.getLength(); ++i) {
        Node item = map.item(i);
        
        String name = item.getNodeName();
        if (attributeName.equalsIgnoreCase(name)) {
          // Found
          found = true;
          val = item.getNodeValue();
          if (val == null) {
            val = "";
          } else {
            val = val.trim();
          }
          break;

//        } else {
//          throw new InvalidXMLException(InvalidXMLException.UNKNOWN_ELEMENT,
//                  new Object[]{name, parentElementName});
        }
      }
    }

    // Check for missing attributes
    if (!found && throwException) {
      throw new InvalidXMLException(InvalidXMLException.ELEMENT_NOT_FOUND,
              new Object[]{attributeName, parentElementName});
    }
    return val;
  }

}
