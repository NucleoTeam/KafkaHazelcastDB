package com.nucleocore.nucleodb.negotiator.examples;

import com.nucleocore.nucleodb.NucleoDBNode;
import com.nucleocore.nucleodb.negotiator.decision.Arguer;
import com.nucleocore.nucleodb.negotiator.decision.hash.HashMeta;
import com.nucleocore.nucleodb.negotiator.decision.support.ArgumentKafkaMessage;
import com.nucleocore.nucleodb.negotiator.decision.support.ArgumentStep;

public class ArguerExample {
  public static void main(String[] args) {
    Arguer arguer = new Arguer(null, new NucleoDBNode(75), "");
    arguer.setDebug(true);
    arguer.getArgumentMessageQueue().add(new ArgumentKafkaMessage(ArgumentStep.NEW, new HashMeta("thisNode", "FDSG34DS", 1)));
    arguer.run();
  }
}
