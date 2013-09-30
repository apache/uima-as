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
package org.apache.uima.as.dd;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.uima.adapter.jms.service.Dd2spring;
import org.apache.uima.test.junit_extension.FileCompare;
import org.apache.uima.test.junit_extension.TeePrintStream;

public class Dd2SpringTest extends TestCase{

  private static String dd2SpringXsltFilePath = "src/main/scripts/dd2spring.xsl";
  private static String saxonClasspath = "file:src/main/saxon/saxon8.jar";
  private static String pathToDds = "src/test/resources/deploy/";
  private static String pathToExpected = "src/test/resources/deploy/expected/";

  // get rid of parts of generated file that are specific to the regen, which are 
  // the header which includes the path to the file, and the date of generation        
  private static String sourceFileCommentPattern = 
      // match start-of-line, spaces, "<!--  -->"      
      "^\\s*<!-- (" +       // match start-of-line, spaces, "<!-- 
      "(file\\:)|" +             // match file:  or
                                 // dd September, 2007, 7:03:00 etc
      "(\\d{1,2} \\w*, \\d{4}, \\d{1,2}\\:\\d{1,2}\\:\\d{1,2} ))" +
      "[^\\n]*?-->[ \\t]*$";  // followed by some other non-newline chars, and then --> at end of line 

  // get rid of uniqifiers that are generated
  private static String uniquifiersPattern = 
      // sample:   9.67.165.27-44fb4c2a:1165912164d:-8000
      // another:  9.2.34.65754d945a:11bfaf7fad7:-7fff
      "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.?[\\-0-9a-fA-F]{6,12}\\:[0-9a-fA-F]{8,12}\\:\\-[0-9a-fA-F]{4,4}"
      ;
  
  // get rid of part of file name that is system specific
    // get rid of variable length comment header lines
  
  private static String sourceFileCommentBorder =
    // match start-of-line, spaces, "<!--===========-->"      
    "^\\s*<!--=*-->[ \\t]*$";     
  
  /* pattern to remove installation-specific paths 
   * file:/C:/a/Eclipse/3.3/apache/uima-as/src/test/resources/descriptors/tutorial/ex2/RoomNumberAnnotator.xml
   */
  
  private static String specificPaths = "\\\"file:[\\w/:\\-\\.]*?src/test/resources/";

  private static Pattern compareFilter = Pattern.compile(
      // turn on multiline mode (?m) so ^ and $ match for each line
      "(?m)(" + 
      sourceFileCommentPattern + 
      ")|(" + 
      uniquifiersPattern +
      ")|(" +
      sourceFileCommentBorder +
      ")|(" + 
      specificPaths +
      ")");
 
  private static Pattern nevermatch = Pattern.compile("     ");
  
  private static Dd2spring dd2SpringInstance = new Dd2spring();
  
  protected void setUp() {

  }
  
  protected void tearDown() {
    
  }
  
  public void testAggregateWithInnerAggregateErMsgKeys() throws Exception {
    checkDd2SpringErrMsg("aggregateWithInnerAggregateErMsgKeys.xml",
       "running test aggregateWithInnerAggregateErMsgKeys.xml: \n" +
       "      *** ERROR: line-number: 44 deployment descriptor for analysisEngine: MeetingDetectorAggregate/NoOp " +
       "specifies a casMultiplier element, but the analysisEngine is not a CAS multiplier" );  
  }

  
  public void testDd2Spring_dfltPrimMultiInstanceSame() throws Exception {
    checkDd2Spring("defaultingPrimAEMultInstanceSameCasPool.xml");  
  }

  public void testDd2Spring_dfltPrimMultiInstanceDiff() throws Exception {
//    checkDd2Spring("defaultingPrimAEMultInstanceDiffCasPool.xml");  
    checkDd2SpringErrMsg(
        "defaultingPrimAEMultInstanceDiffCasPool.xml",
        "running test defaultingPrimAEMultInstanceDiffCasPool.xml: \n" +
"      *** WARN: line-number: 26 Top level Async Primitive specifies a scaleout of numberOfInstances=\"40\", but also specifies a Cas Pool size of numberOfCASes=\"30\".  The Cas Pool size is being forced to be the same as the scaleout.");

  }
  
  public void testAggregateWithInnerAggregateCM() throws Exception {
    checkDd2Spring("aggregateWithInnerAggregateCM.xml");  
  }

  public void testDd2Spring_BrokerURL_params() throws Exception {
    checkDd2Spring("defaultingAEwithBrokerParms.xml");  
  }
  
  public void testDd2Spring_Bad_Window() throws Exception {
    checkDd2Spring("TestThresholdWindow.xml");
  }
  
  public void testDd2Spring_Deploy_AggregateUsingRemoteTempQueueNoBroker() throws Exception {
    checkDd2Spring("Deploy_AggregateUsingRemoteTempQueueNoBroker.xml");
  }

  public void testDd2Spring_defaultingAENoBroker() throws Exception {
    checkDd2Spring("defaultingAENoBroker.xml");
  }

  public void testDd2Spring_var1() throws Exception {
    checkDd2Spring("var1.xml");
  }
  
  public void testDd2Spring_CMAggregateWithCollocatedCM() throws Exception {
    checkDd2Spring("Deploy_CMAggregateWithCollocatedCM.xml");
  }
  
  public void testDd2Spring_complex_inner_CM_colocated() throws Exception {
    checkDd2Spring("Deploy_ComplexAggregateWithInnerAggregateCM.xml");
  }

  public void testDd2Spring_remoteSerializer() throws Exception {
    checkDd2Spring("remoteSerializer.xml");
  }  
  
  public void testDd2Spring_deployAggrUsingRmtTempQ() throws Exception {
    checkDd2Spring("Deploy_AggregateUsingRemoteTempQueue.xml");
  }
  
  public void testDd2Spring_concurRemoteConsumers() throws Exception {
    checkDd2Spring("aggrRmtConcurrentReply.xml");
  }

  public void testVmBkrBad() throws Exception {
    checkDd2Spring("vmBkrBad.xml");
  }

  public void testDd2Spring_inputQueueScaleout3() throws Exception {
    checkDd2Spring("iq3.xml");
  }
  
  public void testDd2Spring_inputQueueScaleout2g2() throws Exception {
    checkDd2Spring("iq2AEg2.xml");
  }
  
  public void testDd2Spring_inputQueueScaleout2g() throws Exception {
    checkDd2Spring("iq2AEg.xml");
  }

  public void testDd2Spring_inputQueueScaleout2() throws Exception {
    checkDd2Spring("iq2AE.xml");
  }

  public void testDd2Spring_inputQueueScaleout1() throws Exception {
    checkDd2Spring("iq1AE.xml");
  }


  public void testDd2Spring_multiLevelAsyncDefaulting() throws Exception {
    checkDd2Spring("Deploy_MeetingFinder.xml");
  }
  
  public void testDd2Spring_dfltPrimMultiInstance() throws Exception {
    checkDd2Spring("defaultingPrimAEMultInstance.xml");  
  }
  
  public void testDd2Spring_tempQ() throws Exception {
    checkDd2Spring("tempQ1.xml");
  }

  public void testDd2Spring_NotCPP() throws Exception {   
    checkDd2SpringErrMsg(
        "envVar/envVarNotCPP.xml",
        "running test envVar/envVarNotCPP.xml: \n *** ERROR: line-number: 28 Service element contains an environmentVariables element, \n" +
          " but the referenced top-level descriptor isn't a C++ component");
  }
 
  public void testDd2Spring_defaults_aggr_async() throws Exception {
    checkDd2Spring("defaultingAEaggrAsync.xml");
  }
    
  public void testDd2Spring_defaults_aggr() throws Exception {
    checkDd2Spring("defaultingAEaggr.xml");
  }
  
  public void testDd2Spring_defaults() throws Exception {
    checkDd2Spring("defaultingAE.xml");
  }
  
  public void testDd2Spring_envVarCPP() throws Exception {
    checkDd2Spring("envVar/envVarCPP.xml");
  }
  
  public void testDd2Spring_envVarCPP1() throws Exception {
    checkDd2Spring("envVar/envVarCPP1.xml");
  }

  public void testDd2Spring_CPPwrongProtocol() throws Exception {   
    checkDd2SpringErrMsg(
        "envVar/envVarCPPwrongProtocol.xml",
        "running test envVar/envVarCPPwrongProtocol.xml: \n *** ERROR: line-number: 31 broker protocol of vm://localhost not supported");
  }


  private void checkDd2SpringErrMsg(String dd, String errmsg) throws Exception {
    PrintStream err = System.err;
    try {
      ByteArrayOutputStream errOut = new ByteArrayOutputStream();
      System.setErr(new TeePrintStream(new PrintStream(errOut), err));
      
      checkDd2Spring(dd);
      assertTrue(FileCompare.compareStringsWithFilter(
          errmsg, errOut.toString(), nevermatch));
      
    } finally {
      System.setErr(err);
    }
  }
 
  private void checkDd2Spring(String dd) throws Exception {
    System.err.print("running test " + dd + ": ");
    File springContextFile = 
      dd2SpringInstance.convertDd2Spring(pathToDds + dd, dd2SpringXsltFilePath, saxonClasspath, "");

    assertTrue (FileCompare.compareWithFilter(springContextFile.getAbsolutePath(), pathToExpected + dd,
        compareFilter));
    
  }
  
  public static void main(String[] args) {
    junit.textui.TestRunner.run(Dd2SpringTest.class);
    System.out.println ("done"); // dummy line to have breakpoint
  }

}
