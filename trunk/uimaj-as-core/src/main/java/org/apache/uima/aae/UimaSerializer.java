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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;

import org.apache.uima.aae.InProcessCache.CacheEntry;
import org.apache.uima.aae.monitor.statistics.AnalysisEnginePerformanceMetrics;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Marker;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.impl.AllowPreexistingFS;
import org.apache.uima.cas.impl.BinaryCasSerDes6;
import org.apache.uima.cas.impl.BinaryCasSerDes6.ReuseInfo;
import org.apache.uima.cas.impl.MarkerImpl;
import org.apache.uima.cas.impl.OutOfTypeSystemData;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.cas.impl.TypeSystemImpl;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.util.XMLSerializer;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
//import java.util.concurrent.ConcurrentHashMap;

public class UimaSerializer {
  private final ThreadLocal<XMLReader> localXmlReader = new ThreadLocal<XMLReader>();

//  /**
//   * deserializes XCas into CAS
//   * 
//   * @param anXcas
//   * @param aCas
//   * @throws Exception
//   */
//  public OutOfTypeSystemData deSerialiazeFromXCAS(String anXcas, CAS aCas) throws Exception {
//    OutOfTypeSystemData otsd = new OutOfTypeSystemData();
//    TypeSystem typesToLoad2 = aCas.getTypeSystem();
//    ByteArrayInputStream bis = new ByteArrayInputStream(anXcas.getBytes());
//    XCASDeserializer deser2 = new XCASDeserializer(typesToLoad2);
//    ContentHandler deserHandler2 = deser2.getXCASHandler(aCas, otsd);
//
//    SAXParserFactory fact2 = SAXParserFactory.newInstance();
//    SAXParser parser2;
//    parser2 = fact2.newSAXParser();
//    XMLReader xmlReader2 = parser2.getXMLReader();
//    xmlReader2.setContentHandler(deserHandler2);
//    xmlReader2.parse(new InputSource(bis));
//    return otsd;
//  }

//  /**
//   * Serializes CAS into a given OutputStream in XCAS format
//   * 
//   * @param stream
//   * @param aCAS
//   * @param encoding
//   * @param typeSystem
//   * @throws IOException
//   * @throws SAXException
//   */
//  public void serializeToXCAS(OutputStream stream, CAS aCAS, String encoding,
//          TypeSystem typeSystem, OutOfTypeSystemData otsd) throws IOException, SAXException {
//
//    if (typeSystem == null)
//      typeSystem = aCAS.getTypeSystem();
//    XMLSerializer xmlSer = new XMLSerializer(stream, false);
//    if (encoding != null)
//      xmlSer.setOutputProperty(OutputKeys.ENCODING, encoding);
//    XCASSerializer ser = new XCASSerializer(typeSystem);
//    ser.serialize(aCAS, xmlSer.getContentHandler(), false, otsd);
//  }

  /**
   * Serializes CAS into a given OutputStream in Xmi format
   * 
   * @param stream
   * @param aCAS
   * @param encoding
   * @param typeSystem
   * @throws IOException
   * @throws SAXException
   */
  public void serializeToXMI(OutputStream stream, CAS aCAS, String encoding, TypeSystem typeSystem,
          OutOfTypeSystemData otsd) throws IOException, SAXException {

    if (typeSystem == null)
      typeSystem = aCAS.getTypeSystem();
    XMLSerializer xmlSer = new XMLSerializer(stream, false);
    if (encoding != null)
      xmlSer.setOutputProperty(OutputKeys.ENCODING, encoding);

    XmiCasSerializer ser = new XmiCasSerializer(typeSystem);

    ser.serialize(aCAS, xmlSer.getContentHandler());
  }

  /**
   * Utility method for serializing a CAS to an XMI String
   */
  public String serializeCasToXmi(CAS aCAS, XmiSerializationSharedData serSharedData)
          throws Exception {
    Writer writer = new StringWriter();
    try {
      XMLSerializer xmlSer = new XMLSerializer(writer, false);
      XmiCasSerializer ser = new XmiCasSerializer(aCAS.getTypeSystem());
      ser.serialize(aCAS, xmlSer.getContentHandler(), null, serSharedData);
      return writer.toString();
    } catch (SAXException e) {
      throw e;
    } finally {
      writer.close();
    }
  }

  public String serializeCasToXmi(CAS aCAS, XmiSerializationSharedData serSharedData, Marker aMarker)
          throws Exception {
    Writer writer = new StringWriter();
    try {
      XMLSerializer xmlSer = new XMLSerializer(writer, false);
      XmiCasSerializer ser = new XmiCasSerializer(aCAS.getTypeSystem());
      ser.serialize(aCAS, xmlSer.getContentHandler(), null, serSharedData, aMarker);
      return writer.toString();
    } catch (SAXException e) {
      throw e;
    } finally {
      writer.close();
    }
  }

  /** 
   * Utility method for deserializing a CAS from an XMI String
   * Does both processing of requests arriving to this service
   *   and responses returning to this service, or to a client. 
   */
  public void deserializeCasFromXmi(String anXmlStr, CAS aCAS,
          XmiSerializationSharedData aSharedData, boolean aLenient, int aMergePoint)
          throws FactoryConfigurationError, ParserConfigurationException, SAXException, IOException {

    if (localXmlReader.get() == null) {
      localXmlReader.set(XMLReaderFactory.createXMLReader());
    }
    XMLReader xmlReader = XMLReaderFactory.createXMLReader(); // localXmlReader.get();

    Reader reader = new StringReader(anXmlStr);
    XmiCasDeserializer deser = new XmiCasDeserializer(aCAS.getTypeSystem());
    ContentHandler handler = deser.getXmiCasHandler(aCAS, aLenient, aSharedData, aMergePoint);
    xmlReader.setContentHandler(handler);
    xmlReader.parse(new InputSource(reader));
  }

  /**
   * Only does the processing of responses (not requests) returning from remotes.
   * Has extra param: AllowPreexistingFS which can be allow, disallow, and ignore
   *   This is for parallel dispatch of remotes, normally configured to disallow
   *     modifications below the delta-cas point
   *     3 cases: allow - for non parallel
   *              disallow - for parallel, with delta cas being returned
   *              ignore - for parallel, with no delta cas being returned
   *                       because earlier version of client wasn't supporting delta cas
   * See above method for requests and responses
   */
  public void deserializeCasFromXmi(String anXmlStr, CAS aCAS,
          XmiSerializationSharedData aSharedData, boolean aLenient, int aMergePoint,
          AllowPreexistingFS allow) throws FactoryConfigurationError, ParserConfigurationException,
          SAXException, IOException {

    if (localXmlReader.get() == null) {
      localXmlReader.set(XMLReaderFactory.createXMLReader());
    }
    XMLReader xmlReader = localXmlReader.get();
    Reader reader = new StringReader(anXmlStr);
    XmiCasDeserializer deser = new XmiCasDeserializer(aCAS.getTypeSystem());
    ContentHandler handler = deser
            .getXmiCasHandler(aCAS, aLenient, aSharedData, aMergePoint, allow);
    xmlReader.setContentHandler(handler);
    xmlReader.parse(new InputSource(reader));
  }

  /** Utility method for deserializing a CAS from a binary */
  public SerialFormat deserializeCasFromBinary(byte[] binarySource, CAS aCAS) throws Exception {
    ByteArrayInputStream fis = null;
    try {
      fis = new ByteArrayInputStream(binarySource);
      return Serialization.deserializeCAS(aCAS, fis);
    } catch (Exception e) {
      throw e;
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
  }

  public byte[] serializeCasToBinary(CAS aCAS) throws Exception {
    ByteArrayOutputStream fos = null;
    try {
      fos = new ByteArrayOutputStream();
      Serialization.serializeCAS(aCAS, fos);
      return fos.toByteArray();
    } catch (Exception e) {
      throw e;
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
  }

  public byte[] serializeCasToBinary(CAS aCAS, Marker aMark) throws Exception {
    ByteArrayOutputStream fos = null;
    try {
      fos = new ByteArrayOutputStream();
      Serialization.serializeCAS(aCAS, fos, aMark);
      return fos.toByteArray();
    } catch (Exception e) {
      throw e;
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
  }
  
  // used to return non-delta cas's (used if delta cas disallowed, for instance by having a CPP delegate)
  public byte[] serializeCasToBinary6(CAS aCAS) throws Exception {
    ByteArrayOutputStream fos = null;
    fos = new ByteArrayOutputStream();
    BinaryCasSerDes6 bcs = new BinaryCasSerDes6(aCAS);
    bcs.serialize(fos);
    return fos.toByteArray();
  }

  // used to send CASes to remotes
  public byte[] serializeCasToBinary6(CAS aCAS, CacheEntry entry, TypeSystemImpl tgtTs) throws Exception {
    ByteArrayOutputStream fos = null;
    fos = new ByteArrayOutputStream();
    BinaryCasSerDes6 bcs = new BinaryCasSerDes6(aCAS, tgtTs);
    bcs.serialize(fos);
    entry.setCompress6ReuseInfo(bcs.getReuseInfo());
    return fos.toByteArray();
  }
  
  public byte[] serializeCasToBinary6(CAS aCAS, Marker aMark, ReuseInfo reuseInfo) throws Exception {
    ByteArrayOutputStream fos = null;
    fos = new ByteArrayOutputStream();
    BinaryCasSerDes6 bcs = new BinaryCasSerDes6(aCAS, (MarkerImpl) aMark, null, reuseInfo);
    bcs.serialize(fos);
    return fos.toByteArray();
  }
  
  @SuppressWarnings("unchecked")
  public static List<AnalysisEnginePerformanceMetrics> deserializePerformanceMetrics(String serializedComponentStats) {
    // check if we received components stats. Currently UIMA AS is not supporting per component
    // stats in asynch aggregates. If the service is asynch, just return an empty list
    if ( serializedComponentStats == null || serializedComponentStats.trim().length() == 0 ) {
      // return an empty list
      return new ArrayList<AnalysisEnginePerformanceMetrics>();
    }
    XStream xstream = new XStream(new DomDriver());
    return (List<AnalysisEnginePerformanceMetrics>)xstream.fromXML(serializedComponentStats);
  }
  
}
