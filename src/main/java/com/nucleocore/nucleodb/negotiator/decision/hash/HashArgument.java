package com.nucleocore.nucleodb.negotiator.decision.hash;

import com.nucleocore.nucleodb.negotiator.decision.hash.responses.HashArgumentResponseTrack;
import org.apache.commons.collections4.map.HashedMap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

public class HashArgument {
  String hashPrefix;

  Map<String, HashArgumentResponseTrack> responses = new HashedMap<>();

  ScheduledFuture executor = null;

  public HashArgument(String hashPrefix) {
    this.hashPrefix = hashPrefix;
  }

  public String getHashPrefix() {
    return hashPrefix;
  }

  public void setHashPrefix(String hashPrefix) {
    this.hashPrefix = hashPrefix;
  }

  public Set<String> getNodes() {
    return responses.keySet();
  }

  public Map<String, HashArgumentResponseTrack> getResponses() {
    return responses;
  }

  public void setResponses(Map<String, HashArgumentResponseTrack> responses) {
    this.responses = responses;
  }

  public void addNode(String nodeUniqueId){
    responses.put(nodeUniqueId, new HashArgumentResponseTrack());
  }

  public ScheduledFuture getExecutor() {
    return executor;
  }

  public void setExecutor(ScheduledFuture executor) {
    this.executor = executor;
  }
}
