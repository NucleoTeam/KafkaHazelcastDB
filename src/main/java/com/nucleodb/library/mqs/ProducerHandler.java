package com.nucleodb.library.mqs;

import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.exceptions.RequiredMethodNotImplementedException;
import org.apache.kafka.clients.producer.Callback;

public class ProducerHandler {
  private String topic;
  private MQSSettings settings;

  public ProducerHandler(MQSSettings settings) {
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

  public void push(String key, String message){
    try {
      throw new RequiredMethodNotImplementedException("Consumer Handler not implemented");
    } catch (RequiredMethodNotImplementedException e) {
      e.printStackTrace();
    }
    System.exit(1);
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public MQSSettings getSettings() {
    return settings;
  }

  public void setSettings(MQSSettings settings) {
    this.settings = settings;
  }
}
