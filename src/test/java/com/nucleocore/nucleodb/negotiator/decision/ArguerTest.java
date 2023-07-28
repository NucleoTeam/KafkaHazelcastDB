package com.nucleocore.nucleodb.negotiator.decision;

import com.nucleocore.nucleodb.NucleoDBNode;
import com.nucleocore.nucleodb.negotiator.decision.hash.HashMeta;
import com.nucleocore.nucleodb.negotiator.decision.support.ArgumentKafkaMessage;
import com.nucleocore.nucleodb.negotiator.decision.support.ArgumentStep;
import junit.framework.TestCase;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class ArguerTest extends TestCase {
  final Logger rootLogger = LogManager.getRootLogger();
  public void setUp() throws Exception {
    super.setUp();
    rootLogger.atLevel(Level.ALL);

  }

  public void tearDown() throws Exception {
  }

  @Test
  public void testAdd() {
    Arguer arguer = new Arguer(null, new NucleoDBNode(75), "");
    arguer.setDebug(true);
    //arguer.getArgumentMessageQueue().add(new ArgumentKafkaMessage(ArgumentStep.NEW, new HashMeta("thisNode", "taco", 25)));
    arguer.run();
    assertTrue(true);
  }
}