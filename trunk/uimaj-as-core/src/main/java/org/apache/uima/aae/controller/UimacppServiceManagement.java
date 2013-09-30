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

package org.apache.uima.aae.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * This class implements the JMX MBean interface to provide operational statistics about the C++
 * service.
 */
public class UimacppServiceManagement implements UimacppServiceManagementMBean {

  private static final long serialVersionUID = -2507413276728501209L;

  private static final long MAX_TIME_VALID = 500;

  private static long lastRefreshTime;

  HashMap<String, String> jmxInfo;

  private String uniqueMBeanName;

  private String aeDescriptor;

  private int aeInstances;

  private String queueBrokerURL;

  private String queueName;

  Socket socket;

  BufferedReader rdr;

  PrintWriter writer;

  public UimacppServiceManagement(String domainName, Socket sock, String aeDescriptor,
          int numInstances, String brokerURL, String queueName) throws IOException {

    if (domainName == null || domainName.length() == 0) {
      domainName = "org.apache.uima:type=ee.jms.services,s=" + queueName + " Uima EE Service,";
    }
    uniqueMBeanName = domainName + "name=" + queueName + "_Service";
    socket = sock;
    rdr = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    writer = new PrintWriter(socket.getOutputStream(), true);
    this.aeDescriptor = aeDescriptor;
    this.queueBrokerURL = brokerURL;
    this.queueName = queueName;
    this.aeInstances = numInstances;
    this.jmxInfo = new HashMap();
    this.lastRefreshTime = 0;
  }
  
  synchronized public String quiesceAndStop() throws IOException {

	  if (socket != null) {
		// System.out.println("UimacppServiceManagement::quiesceAndStop()
		// Sending QUIESCEANDSTOP");
	
		writer.write("QUIESCEANDSTOP");
		writer.flush();
	
		BufferedReader in = new BufferedReader(new InputStreamReader(socket
				.getInputStream()));
	
		StringBuffer sb = new StringBuffer();
		int c = in.read();
		while (c >= 0) {
		  sb.append((char) c);
		  c = in.read();
		  if (c == '\n') {
			break;
		  }
		}
		System.out.println("UimacppServiceManagement service reports QuiesceAndStop " + sb.toString());
		return sb.toString();
		} else {
		  throw new IOException("Error: no socket connection.");
		}
	}


  synchronized public String getStatisticsAsString() throws IOException {

    if (socket != null) {
      // System.out.println("UimacppServiceManagement::getStatisticsAsString() Sending GETSTATS");

      writer.write("GETSTATS");
      writer.flush();

      // OutputStream os = socket.getOutputStream();
      // os.flush();
      // os.write(getstats.getBytes());

      // System.out.println("getStatistics() Sent GETSTATS Waiting for reply");
      BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

      StringBuffer sb = new StringBuffer();
      int c = in.read();
      while (c >= 0) {
        sb.append((char) c);
        c = in.read();
        if (c == '\n') {
          break;
        }
      }
      this.lastRefreshTime = System.currentTimeMillis();

      return sb.toString();
    } else {
      throw new IOException("Error: no socket connection.");
    }
  }

  synchronized public void getStatistics() throws IOException {

    String stats = getStatisticsAsString();
    String name = null;
    String value = null;
    if (stats != null) {
      StringTokenizer tokenizer = new StringTokenizer(stats);
      while (tokenizer.hasMoreTokens()) {
        String aStat = tokenizer.nextToken();
        StringTokenizer st2 = new StringTokenizer(aStat, "=");
        name = null;
        value = null;
        if (st2.hasMoreElements()) {
          name = st2.nextToken();
        }
        if (st2.hasMoreElements()) {
          value = st2.nextToken();
        }
        if (name == null) {
          throw new IOException("Statistic name is not set.");
        }
        if (value == null) {
          throw new IOException("Statistic value is not set for " + name);
        }
        jmxInfo.put(name, value);
      }
    }
  }

  synchronized public String getQueueBrokerURL() throws IOException {
    // System.out.println("QueueBrokerURL");
    /*
     * We are assuming this method gets called first by the MBeanServer. So we send a request to C++
     * service to get latest stats.
     */

    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }

    return queueBrokerURL;
  }

  synchronized public String getQueueName() throws IOException {
    // System.out.println("QueueName");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    return queueName;
  }

  synchronized public String getAEDescriptor() throws IOException {
    // System.out.println("aeDescriptor");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    return aeDescriptor;
  }

  synchronized public int getAEInstances() throws IOException {
    // System.out.println("aeInstances");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    return aeInstances;

  }

  synchronized public long getErrorsGetMeta() throws IOException {
    // System.out.println("GETMETAERRORS");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("GETMETAERRORS");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getErrorsProcessCas() throws IOException {
    // System.out.println("errorsProcessCas");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("PROCESSCASERRORS");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getErrorsCPC() throws IOException {
    // System.out.println("errorsCPC");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("CPCERRORS");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTotalNumCasProcessed() throws IOException {
    // System.out.println("TotalNumCasProcessed");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("NUMCASPROCESSED");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTimingGetMeta() throws IOException {
    // System.out.println("getTimingGetMeta");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("GETMETATIME");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTimingCPC() throws IOException {
    // System.out.println("getTimingCPC");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("CPCTIME");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTimingSerialization() throws IOException {
    // System.out.println("getTimingSerialization");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("SERIALIZETIME");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTimingAnnotatorProcess() throws IOException {
    // System.out.println("getTimingAnnotatorProcess");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("ANNOTATORTIME");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTimingDeserialization() throws IOException {
    // System.out.println("getTimingDeserialization");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("DESERIALIZETIME");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTimingMessageProcessing() throws IOException {
    // System.out.println("getTimingMessageProcessing");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("MESSAGEPROCESSTIME");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public long getTimingIdle() throws IOException {
    // System.out.println("getTimingIdle");
    if (System.currentTimeMillis() - this.lastRefreshTime > MAX_TIME_VALID) {
      getStatistics();
    }
    String v = jmxInfo.get("IDLETIME");
    if (v == null) {
      return -1;
    } else {
      return Long.valueOf(v);
    }
  }

  synchronized public void resetStats() throws IOException {
    writer.write("RESET");
    writer.flush();
    getStatistics();
  }

  public void increaseAEInstances(int num) {
    // TODO Auto-generated method stub

  }

  public void decreaseAEInstances(int num) {
    // TODO Auto-generated method stub

  }

  synchronized public void shutdown() throws IOException {
    if (this.socket != null) {
      // System.out.println("UimacppServiceManagement sending shutdown message");
      writer.write("SHUTDOWN");
      writer.flush();
      // System.out.println("UimacppServiceManagement sent shutdown message");
  		BufferedReader in = new BufferedReader(new InputStreamReader(socket
  				.getInputStream()));
  	
  		StringBuffer sb = new StringBuffer();
  		int c = in.read();
  		while (c >= 0) {
  		  sb.append((char) c);
  		  c = in.read();
  		  if (c == '\n') {
  			break;
  		  }
  		}
  	  System.out.println("UimacppServiceManagement service reports shutdown " + sb.toString());
      return;
    } else {
      System.err.println("Error no connection");
    }
  }

  public String getUniqueMBeanName() {
    return uniqueMBeanName;
  }

	synchronized public void quiesceAndShutdown() throws IOException {
		quiesceAndStop();
	}

}
