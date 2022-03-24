package com.nucleocore.nucleodb.negotiator.decision.hash.responses;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class ConsensusResponse implements Serializable {
  Set<String> nodes = Sets.newHashSet();
  List<NodeSortWeight> weights = Lists.newLinkedList();

  public ConsensusResponse(Set<String> nodes, List<NodeSortWeight> weights) {
    this.nodes = nodes;
    this.weights = weights;
  }

  public Set<String> getNodes() {
    return nodes;
  }

  public void setNodes(Set<String> nodes) {
    this.nodes = nodes;
  }

  public List<NodeSortWeight> getWeights() {
    return weights;
  }

  public void setWeights(List<NodeSortWeight> weights) {
    this.weights = weights;
  }
}
