package com.nucleocore.nucleodb.sqltest;

public class AnimeDTO{
  String uniqueKey;
  String name;

  public AnimeDTO() {
  }

  public String getUniqueKey() {
    return uniqueKey;
  }

  public void setUniqueKey(String uniqueKey) {
    this.uniqueKey = uniqueKey;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
