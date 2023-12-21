package com.nucleodb.library.database.tables.table;

import java.lang.reflect.Type;

public class DataTableConsumer{
  Type clazz;
  DataTableConfig dataTableConfig;

  public DataTableConsumer(Type clazz, DataTableConfig dataTableConfig) {
    this.clazz = clazz;
    this.dataTableConfig = dataTableConfig;
  }

  public Type getClazz() {
    return clazz;
  }

  public void setClazz(Type clazz) {
    this.clazz = clazz;
  }

  public DataTableConfig getDataTableConfig() {
    return dataTableConfig;
  }

  public void setDataTableConfig(DataTableConfig dataTableConfig) {
    this.dataTableConfig = dataTableConfig;
  }
}