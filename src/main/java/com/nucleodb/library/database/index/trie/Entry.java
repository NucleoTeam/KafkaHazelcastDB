package com.nucleodb.library.database.index.trie;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Stack;

public class Entry<T>{
  T data;

  Stack<Node> lastNodes;

  public Entry(T data, Stack<Node> nodes) {
    this.data = data;
    this.lastNodes = nodes;
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }

  @JsonIgnore
  public Stack<Node> getLastNodes() {
    return lastNodes;
  }

  public void setLastNodes(Stack<Node> lastNodes) {
    this.lastNodes = lastNodes;
  }
}