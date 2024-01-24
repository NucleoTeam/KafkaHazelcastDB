package com.nucleodb.library.database.lock;

import com.nucleodb.library.mqs.config.MQSConfiguration;
import com.nucleodb.library.mqs.kafka.KafkaConfiguration;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class LockConfig implements Serializable {
  private static final long serialVersionUID = 1;
  MQSConfiguration mqsConfiguration = new KafkaConfiguration();
  Map<String, Object> settingsMap = new TreeMap<>(){{
    put("partitions", 1);
    put("replicas", 3);
  }};
  String topic = "locks";

  public LockConfig() {
  }

  public MQSConfiguration getMqsConfiguration() {
    return mqsConfiguration;
  }

  public void setMqsConfiguration(MQSConfiguration mqsConfiguration) {
    this.mqsConfiguration = mqsConfiguration;
  }

  public Map<String, Object> getSettingsMap() {
    return settingsMap;
  }

  public void setSettingsMap(Map<String, Object> settingsMap) {
    this.settingsMap = settingsMap;
  }

  public String getTopic() {
    return topic;

  }

  public void setTopic(String topic) {
    this.topic = topic;
    this.settingsMap.put("table", topic);
  }
}
