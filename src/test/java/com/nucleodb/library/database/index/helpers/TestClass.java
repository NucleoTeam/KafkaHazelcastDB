package com.nucleodb.library.database.index.helpers;

import com.nucleodb.library.database.tables.annotation.Table;


public class TestClass {
    String name = "test";

    public TestClass() {
    }

    public TestClass(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }