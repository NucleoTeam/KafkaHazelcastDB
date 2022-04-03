package com.nucleocore.nucleodb.negotiator.decision.hash;

import com.nucleocore.nucleodb.NucleoDBNode;
import com.nucleocore.nucleodb.negotiator.decision.support.ArgumentMessageData;
import com.nucleocore.nucleodb.negotiator.decision.hash.responses.ConsensusResponse;
import com.nucleocore.nucleodb.negotiator.decision.hash.responses.ReasonResponse;
import org.apache.commons.collections4.map.HashedMap;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HashMeta extends ArgumentMessageData<HashProcess> {
  private String node;
  private String hashPrefix;
  private Map<String, Object> objects = new HashedMap<>();
  private int replicas;
  private int retry = 0;

  public HashMeta(String node, String hashPrefix, int replicas, int retry) {
    super(HashProcess.class);
    this.node = node;
    this.hashPrefix = hashPrefix;
    this.replicas = replicas;
    this.retry = retry;
  }
  public HashMeta(String node, String hashPrefix, Map<String, Object> objects, int replicas, int retry) {
    super(HashProcess.class);
    this.node = node;
    this.hashPrefix = hashPrefix;
    this.objects = objects;
    this.replicas = replicas;
    this.retry = retry;
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public String getHashPrefix() {
    return hashPrefix;
  }

  public void setHashPrefix(String hashPrefix) {
    this.hashPrefix = hashPrefix;
  }

  public Map<String, Object> getObjects() {
    return objects;
  }

  public void setObjects(Map<String, Object> objects) {
    this.objects = objects;
  }

  public int getReplicas() {
    return replicas;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public void setNodeStatus(NucleoDBNode node){
    node.setData(this.getObjects());
  }
  public ReasonResponse getReason() {
    return new ReasonResponse(
      (long) this.getObjects().getOrDefault("slots", (long)0),
      (long) this.getObjects().getOrDefault("hits", (long)0),
      (double[]) this.getObjects().getOrDefault("load", new double[3]),
      (ReasonResponse.CPUPercent[]) this.getObjects().getOrDefault("cpu", null),
      (ReasonResponse.Memory) this.getObjects().getOrDefault("memory", null)
    );
  }
  public ConsensusResponse getConsensus() {
    Object obj = getObjects().getOrDefault("consensus", null);
    if(obj!=null && obj instanceof ConsensusResponse){
      return (ConsensusResponse) obj;
    }
    return null;
  }

  public Set<String> getVoteResult() {
    Object obj = getObjects().getOrDefault("vote_result", null);
    if(obj!=null && obj instanceof Map){
      return ((Map<String, HashProcess.NodeVote>) obj).entrySet().stream().sorted(Comparator.comparingInt(a -> a.getValue().votes)).limit(replicas).collect(Collectors.toList()).stream().map(c->c.getValue().node).collect(Collectors.toSet());
    }
    return null;
  }
  public <T> T get(String key){
    return (T) objects.get(key);
  }

  public int getRetry() {
    return retry;
  }

  public void setRetry(int retry) {
    this.retry = retry;
  }
}
