package com.nucleocore.spring.repository;

public class NDBConfiguration{
  String[] packages;

  public NDBConfiguration(String... packages) {
    this.packages = packages;
  }

  public String[] getPackages() {
    return packages;
  }

  public void setPackages(String[] packages) {
    this.packages = packages;
  }
}
