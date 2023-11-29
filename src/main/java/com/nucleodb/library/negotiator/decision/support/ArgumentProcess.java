package com.nucleodb.library.negotiator.decision.support;

import com.nucleodb.library.NucleoDBNode;

public interface ArgumentProcess {
  void process(NucleoDBNode node, ArgumentStep argumentType, ArgumentMessageData processData, ArgumentCallback<Object> runner);
  void action(NucleoDBNode node, ArgumentResult argumentResult);
}
