package com.nucleodb.library.mqs.config;

import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.mqs.ConsumerHandler;

import java.util.Map;

public class MQSSettings{
  ConsumerHandler consumerHandler;
  String table;

  public MQSSettings(Map<String, Object> objs) {
    this.table = (String) objs.get("table");
    this.consumerHandler = (ConsumerHandler) objs.get("consumerHandler");

  }

  public ConsumerHandler getConsumerHandler() {
    return consumerHandler;
  }

  public void setConsumerHandler(ConsumerHandler consumerHandler) {
    this.consumerHandler = consumerHandler;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }
}
