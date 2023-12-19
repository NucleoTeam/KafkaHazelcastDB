package com.nucleodb.library.mqs.config;

public class MQSConstructorSettings<T> {
    Class<T> clazz;
    String[] constructorGetterElements;
    Class[] constructorTypes;

    public MQSConstructorSettings(Class<T> clazz, String[] constructorGetterElements, Class[] constructorTypes) {
      this.clazz = clazz;
      this.constructorGetterElements = constructorGetterElements;
      this.constructorTypes = constructorTypes;
    }

    public Class<T> getClazz() {
      return clazz;
    }

    public void setClazz(Class<T> clazz) {
      this.clazz = clazz;
    }

    public String[] getConstructorGetterElements() {
      return constructorGetterElements;
    }

    public void setConstructorGetterElements(String[] constructorGetterElements) {
      this.constructorGetterElements = constructorGetterElements;
    }

    public Class[] getConstructorTypes() {
      return constructorTypes;
    }

    public void setConstructorTypes(Class[] constructorTypes) {
      this.constructorTypes = constructorTypes;
    }
  }