package com.nucleodb.library.database.index.trie;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.TreeMap;

public class Node{
  int character;
  Node parent;
  TreeMap<Integer, Node> nodes = new TreeMap<>();
  List<Entry> entries = Lists.newLinkedList();
  List<Entry> partialEntries = Lists.newLinkedList();

  public Node(int character) {
    this.character = character;
  }

  public int getCharacter() {
    return character;
  }

  public TreeMap<Integer, Node> getNodes() {
    return nodes;
  }

  @JsonIgnore
  public Node getParent() {
    return parent;
  }

  public void setParent(Node parent) {
    this.parent = parent;
  }

  public List<Entry> getEntries() {
    return entries;
  }

  @JsonIgnore
  public List<Entry> getPartialEntries() {
    return partialEntries;
  }

}