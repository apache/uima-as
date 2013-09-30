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

package org.apache.uima.aae;

import java.util.concurrent.ConcurrentHashMap;

public class SerializerCache {
  
  //  Map storing Serializer instance for each thread. The key to the 
  //  map is a thread id.
  private static ConcurrentHashMap<Long, UimaSerializer> serializerMap = 
    new ConcurrentHashMap<Long, UimaSerializer>();
  /**
   * Returns an instance of a Serializer object. Uses current thread id
   * to look up the Map. If the Map does not contain an entry for current
   * thread, a new Serializer instance is created and added to the Map for
   * future use.
   * 
   * @return - Serializer instance 
   */
  public static UimaSerializer lookupSerializerByThreadId() {
    if ( !serializerMap.containsKey(Thread.currentThread().getId())) {
      serializerMap.put(Thread.currentThread().getId(), new UimaSerializer());
    }
    return serializerMap.get(Thread.currentThread().getId());
  }
  
}
