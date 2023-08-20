package com.nucleocore.library.negotiator.decision.hash.responses;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeSortWeight implements Serializable {
  public int weight;
  public String node;

  public NodeSortWeight(String node) {
    this.node = node;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

}