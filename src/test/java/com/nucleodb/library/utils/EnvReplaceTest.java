package com.nucleodb.library.utils;

import org.junit.jupiter.api.Test;

import static com.nucleodb.library.utils.EnvReplace.replaceEnvVariables;
import static org.junit.jupiter.api.Assertions.*;

class EnvReplaceTest{

  @Test
  void replaceEnvVariablesTest() {
    assertEquals("127.0.0.1:2021,127.0.0.1:2022", replaceEnvVariables("${TEST:127.0.0.1:2021,127.0.0.1:2022}"));
  }

  @Test
  void replaceEnvVariablesPath() {
    assertEquals(System.getenv("PATH"), replaceEnvVariables("${PATH}"));
  }

  @Test
  void replaceEnvVariablesSetEnvAndRead() {
    assertEquals("CHECK", System.getenv("TEST2"));
    assertEquals(System.getenv("TEST2"), replaceEnvVariables("${TEST2:CHECKWRONG}"));
  }
}