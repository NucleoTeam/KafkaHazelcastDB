package com.nucleocore.db.database.utils;

import java.util.*;
import java.util.stream.Collectors;

public class Trie {
  public TrieNode root = new TrieNode();
  public void add(Object index, DataEntry de){
    root.add(String.valueOf(index), de);
  }
  public Set<DataEntry> search(Object indexCheck){
    return root.search(String.valueOf(indexCheck));
  }
  public boolean remove(Object left, DataEntry de) {
    return root.remove(String.valueOf(left), de);
  }
}
