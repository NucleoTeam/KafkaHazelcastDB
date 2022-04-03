package com.nucleocore.nucleodb.negotiator.decision.support;

import org.apache.commons.collections4.map.HashedMap;

import java.io.Serializable;
import java.util.Map;

public class ArgumentErrorResponse implements Serializable {
  String message;
  ArgumentErrorType argumentError;

  Map<String, Object> objects = new HashedMap<>();

  public ArgumentErrorResponse(String message, ArgumentErrorType argumentError) {
    this.message = message;
    this.argumentError = argumentError;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public ArgumentErrorType getArgumentError() {
    return argumentError;
  }

  public void setArgumentError(ArgumentErrorType argumentError) {
    this.argumentError = argumentError;
  }

  public Map<String, Object> getObjects() {
    return objects;
  }

  public void setObjects(Map<String, Object> objects) {
    this.objects = objects;
  }
  public <T> T get(String key){
    return (T) objects.get(key);
  }
}
