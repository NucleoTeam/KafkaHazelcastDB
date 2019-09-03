package com.nucleocore.db.database.utils;

import java.util.*;
import java.util.stream.Collectors;

public class Trie {
  public TrieNode root = new TrieNode();
  public void add(Object index, String de){
    root.add(String.valueOf(index), de);
  }
  public List<String> search(Object indexCheck){
    return root.search(String.valueOf(indexCheck));
  }
  public boolean remove(Object left, String de) {
    return root.remove(String.valueOf(left), de);
  }
}
