package com.nucleocore.library.negotiator.decision.support;

import java.io.Serializable;

public class ArgumentKafkaMessage implements Serializable {
  ArgumentStep argumentStep;
  ArgumentMessageData processData;

  public ArgumentKafkaMessage(ArgumentStep argumentType, ArgumentMessageData processData) {
    this.argumentStep = argumentType;
    this.processData = processData;
  }

  public ArgumentStep getArgumentStep() {
    return argumentStep;
  }

  public void setArgumentStep(ArgumentStep argumentStep) {
    this.argumentStep = argumentStep;
  }

  public ArgumentMessageData getProcessData() {
    return processData;
  }

  public void setProcessData(ArgumentMessageData processData) {
    this.processData = processData;
  }
}
