package com.nucleocore.nucleodb.database.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Serializer{
  private static Serializer objectMapper = new Serializer();

  ObjectMapper om;
  public Serializer() {
    om = new ObjectMapper();
    om.findAndRegisterModules();
  }

  public ObjectMapper getOm() {
    return om;
  }

  public void setOm(ObjectMapper om) {
    this.om = om;
  }

  public static Serializer getObjectMapper() {
    return objectMapper;
  }

  public static void setObjectMapper(Serializer objectMapper) {
    Serializer.objectMapper = objectMapper;
  }
}
