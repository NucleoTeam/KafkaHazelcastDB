package com.nucleocore.db.database.index.trie;

import com.nucleocore.db.database.index.IndexTemplate;

import java.util.*;

public class TrieIndex extends IndexTemplate {
  public TrieNode root = new TrieNode();
  public void add(Object index, String de){
    root.add(String.valueOf(index), de);
  }
  public List<Object> search(Object indexCheck){
    return root.search(String.valueOf(indexCheck));
  }
  public boolean remove(Object left, String de) {
    return root.remove(String.valueOf(left), de);
  }
}
