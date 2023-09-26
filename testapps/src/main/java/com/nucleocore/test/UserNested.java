package com.nucleocore.test;

import java.io.Serializable;

public class UserNested implements Serializable{
  private static final long serialVersionUID = 1;
  public UserNested() {
      nestedValue = "woot";
  }
  private String nestedValue;

  public String getNestedValue() {
      return nestedValue;
  }

  public void setNestedValue(String nestedValue) {
      this.nestedValue = nestedValue;
  }
}