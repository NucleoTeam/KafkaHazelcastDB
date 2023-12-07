package com.nucleodb.library.database.index.trie;

import com.google.common.collect.Lists;

import java.util.List;

public class Root<T> {
    List<Node> roots = Lists.newLinkedList();

    public Root() {
    }

    public void add(Node n){
      roots.add(n);
    }
    public void remove(Node n){
      roots.remove(n);
    }
    public List<Node> getRoots() {
      return roots;
    }

    public void setRoots(List<Node> roots) {
      this.roots = roots;
    }
  }