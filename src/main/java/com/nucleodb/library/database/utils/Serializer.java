package com.nucleodb.library.database.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Serializer{
  private static Serializer objectMapper = new Serializer();

  ObjectMapper om;
  ObjectMapper omNonType;

  public Serializer() {
    om = new ObjectMapper();
    om.findAndRegisterModules();
    om.enableDefaultTyping(ObjectMapper.DefaultTyping.EVERYTHING);
    om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    omNonType = new ObjectMapper();
    omNonType.findAndRegisterModules();
    omNonType.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static void log(Object o){
    try {
      System.out.println(getObjectMapper().getOm().writeValueAsString(o));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public ObjectMapper getOm() {
    return om;
  }

  public ObjectMapper getOmNonType() {
    return omNonType;
  }

  public void setOmNonType(ObjectMapper omNonType) {
    this.omNonType = omNonType;
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
