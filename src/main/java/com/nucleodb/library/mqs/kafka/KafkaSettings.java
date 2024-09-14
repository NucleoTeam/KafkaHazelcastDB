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
    Object servers = objs.get("servers");
    Object groupName = objs.get("groupName");
    Object partitions = objs.get("partitions");
    Object replicas = objs.get("replicas");
    Object offsetReset = objs.get("offsetReset");
    if(servers!=null) {
      this.servers = (String) servers;
    }
    if(groupName!=null) {
      this.groupName = (String) groupName;
    }
    if(partitions!=null) {
      this.partitions = (int) partitions;
    }
    if(replicas!=null) {
      this.replicas = (int) replicas;
    }
    if(offsetReset!=null) {
      this.offsetReset = (String) offsetReset;
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
