package com.nucleodb.library.mqs.kafka;

import com.nucleodb.library.mqs.config.MQSSettings;

import java.util.Map;
import java.util.UUID;

public class KafkaSettings extends MQSSettings{
  String servers = System.getenv().getOrDefault("KAFKA_SERVERS","127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092");
  String groupName = System.getenv().getOrDefault("KAFKA_GROUP_ID", UUID.randomUUID().toString());
  int partitions = 36;
  int replicas = 3;
  String offsetReset = "earliest";



  public KafkaSettings(Map<String, Object> objs) {
    super(objs);
    if(objs.containsKey("servers")) {
      this.servers = (String) objs.get("servers");
    }
    if(objs.containsKey("groupName")) {
      this.groupName = (String) objs.get("groupName");
    }

    if(objs.containsKey("partitions")) {
      this.partitions = (int) objs.get("partitions");
    }
    if(objs.containsKey("replicas")) {
      this.replicas = (int) objs.get("replicas");
    }

    if(objs.containsKey("offsetReset")) {
      this.replicas = (int) objs.get("offsetReset");
    }


  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public String getServers() {
    return servers;
  }

  public void setServers(String servers) {
    this.servers = servers;
  }

  public int getPartitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public int getReplicas() {
    return replicas;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public String getOffsetReset() {
    return offsetReset;
  }

  public void setOffsetReset(String offsetReset) {
    this.offsetReset = offsetReset;
  }
}
