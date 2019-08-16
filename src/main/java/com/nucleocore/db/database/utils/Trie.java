package com.nucleocore.db.database.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Trie {
  public Trie(){

  }
  public class Node{
    public List<Node> path = new ArrayList<>();
    public Set<DataEntry> entries = null;
    public String entry = "";
    public Node(){

    }
    public void add(String string, DataEntry de){
      if(string==null){
        if(entries==null)
          entries = new HashSet<>();
        entries.add(de);
        return;
      }
      char s = string.charAt(0);
      if(path.get(s)==null){

      }
      Node n = new Node();
      path.set(s, n);

    }
  }
}
