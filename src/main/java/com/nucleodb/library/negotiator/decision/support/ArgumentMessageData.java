package com.nucleodb.library.negotiator.decision.support;

import java.io.Serializable;

public class ArgumentMessageData<T> implements Serializable {
  Class<T> logicProcessorClass;
  public ArgumentMessageData(Class<T> logicProcessorClass) {
    this.logicProcessorClass = logicProcessorClass;
  }

  public Class<T> getLogicProcessorClass() {
    return logicProcessorClass;
  }
}
