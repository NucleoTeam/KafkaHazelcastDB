package com.nucleocore.memcache;

import java.io.Serializable;

public class KeyVal implements Serializable{
  private static final long serialVersionUID = 1;
  private String name;
  private String value;

  public KeyVal() {
  }

  public KeyVal(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }
}
