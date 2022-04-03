package com.nucleocore.nucleodb.negotiator.decision.hash.responses;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class HashArgumentResponseTrack implements Serializable  {
  ReasonResponse reasonResponse;
  ConsensusResponse consensusResponse;
  Set<String> nodes = null;


  public HashArgumentResponseTrack() {

  }

  public ReasonResponse getReasonResponse() {
    return reasonResponse;
  }

  public void setReasonResponse(ReasonResponse reasonResponse) {
    this.reasonResponse = reasonResponse;
  }

  public ConsensusResponse getConsensusResponse() {
    return consensusResponse;
  }

  public void setConsensusResponse(ConsensusResponse consensusResponse) {
    this.consensusResponse = consensusResponse;
  }

  public Set<String> getNodes() {
    return nodes;
  }

  public void setNodes(Set<String> nodes) {
    this.nodes = nodes;
  }
}