package com.nucleodb.library.mqs;

import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.exceptions.RequiredMethodNotImplementedException;
import org.apache.kafka.clients.producer.Callback;

public class ProducerHandler {
  private String table;
  private MQSSettings settings;

  public ProducerHandler(MQSSettings settings, String table) {
    this.table = table;
    this.settings = settings;
  }


  public void push(String key, long version, Modify modify, Callback callback){
    try {
      throw new RequiredMethodNotImplementedException("Consumer Handler not implemented");
    } catch (RequiredMethodNotImplementedException e) {
      e.printStackTrace();
    }
    System.exit(1);
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public MQSSettings getSettings() {
    return settings;
  }

  public void setSettings(MQSSettings settings) {
    this.settings = settings;
  }
}
