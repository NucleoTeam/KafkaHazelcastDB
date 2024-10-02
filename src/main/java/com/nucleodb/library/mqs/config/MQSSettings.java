package com.nucleodb.library.mqs.config;

import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.mqs.ConsumerHandler;

import java.util.Map;

public class MQSSettings{
  ConsumerHandler consumerHandler;
  String topic;

  public MQSSettings(Map<String, Object> objs) {
    this.topic = (String) objs.get("topic");
    this.consumerHandler = (ConsumerHandler) objs.get("consumerHandler");

  }

  public ConsumerHandler getConsumerHandler() {
    return consumerHandler;
  }

  public void setConsumerHandler(ConsumerHandler consumerHandler) {
    this.consumerHandler = consumerHandler;
  }

  public String getTable() {
    return topic;
  }

  public void setTable(String table) {
    this.topic = table;
  }
}
