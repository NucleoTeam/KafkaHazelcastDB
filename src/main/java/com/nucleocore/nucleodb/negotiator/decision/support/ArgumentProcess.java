package com.nucleocore.nucleodb.negotiator.decision.support;

import com.nucleocore.nucleodb.NucleoDBNode;

public interface ArgumentProcess {
  void process(NucleoDBNode node, ArgumentStep argumentType, ArgumentMessageData processData, ArgumentCallback<Object> runner);
  void action(NucleoDBNode node, ArgumentResult argumentResult);
}
