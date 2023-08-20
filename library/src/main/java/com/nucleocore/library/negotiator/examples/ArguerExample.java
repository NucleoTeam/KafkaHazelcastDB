package com.nucleocore.library.negotiator.examples;

import com.nucleocore.library.NucleoDBNode;
import com.nucleocore.library.negotiator.decision.Arguer;
import com.nucleocore.library.negotiator.decision.hash.HashMeta;
import com.nucleocore.library.negotiator.decision.support.ArgumentKafkaMessage;
import com.nucleocore.library.negotiator.decision.support.ArgumentStep;

public class ArguerExample {
  public static void main(String[] args) {
    Arguer arguer = new Arguer(null, new NucleoDBNode(75), "");
    arguer.setDebug(true);
    arguer.getArgumentMessageQueue().add(new ArgumentKafkaMessage(ArgumentStep.NEW, new HashMeta("thisNode", "FDSG34DS", 1, 0)));
    arguer.run();
  }
}
