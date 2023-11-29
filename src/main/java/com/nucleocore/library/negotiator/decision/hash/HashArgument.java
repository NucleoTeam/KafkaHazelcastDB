package com.nucleocore.library.negotiator.decision.hash;

import com.nucleocore.library.negotiator.decision.hash.responses.HashArgumentResponseTrack;
import org.apache.commons.collections4.map.HashedMap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class HashArgument {
  String hashPrefix;
  private int retry = 0;

  Map<String, HashArgumentResponseTrack> responses = new HashedMap<>();

  ScheduledFuture nodeExecutor = null;
  ScheduledFuture consensusExector = null;
  ScheduledFuture reasonExecutor = null;

  AtomicBoolean nodeExecuted = new AtomicBoolean(false);
  AtomicBoolean consensusExecuted = new AtomicBoolean(false);
  AtomicBoolean reasonExecuted = new AtomicBoolean(false);

  public HashArgument(String hashPrefix, int retry) {
    this.hashPrefix = hashPrefix;
    this.retry = retry;
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

  public ScheduledFuture getNodeExecutor() {
    return nodeExecutor;
  }

  public void setNodeExecutor(ScheduledFuture nodeExecutor) {
    this.nodeExecutor = nodeExecutor;
  }

  public ScheduledFuture getConsensusExector() {
    return consensusExector;
  }

  public void setConsensusExector(ScheduledFuture consensusExector) {
    this.consensusExector = consensusExector;
  }

  public ScheduledFuture getReasonExecutor() {
    return reasonExecutor;
  }

  public void setReasonExecutor(ScheduledFuture reasonExecutor) {
    this.reasonExecutor = reasonExecutor;
  }

  public AtomicBoolean getNodeExecuted() {
    return nodeExecuted;
  }

  public AtomicBoolean getConsensusExecuted() {
    return consensusExecuted;
  }

  public AtomicBoolean getReasonExecuted() {
    return reasonExecuted;
  }

  public int getRetry() {
    return retry;
  }

  public void setRetry(int retry) {
    this.retry = retry;
  }
}
