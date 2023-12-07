package com.nucleodb.library.database.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.nucleodb.library.database.index.trie.Entry;
import com.nucleodb.library.database.index.trie.Node;
import com.nucleodb.library.database.index.trie.Root;
import com.nucleodb.library.database.tables.connection.Connection;
import com.nucleodb.library.database.tables.table.DataEntry;

import java.util.LinkedList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TrieIndex<T> extends IndexWrapper<T>{
  Root[] rootPartialNodes = new Root[256];
  Node root = new Node(0);

  TreeMap<String, Entry> entries = new TreeMap<>();

  public TrieIndex(String indexedKey) {
    super(indexedKey);
  }

  void addRoot(Node n) {
    if (rootPartialNodes[n.getCharacter()] == null) {
      rootPartialNodes[n.getCharacter()] = new Root();
    }
    rootPartialNodes[n.getCharacter()].add(n);
  }

  Node getNodeFromIntBuffer(TreeMap<Integer, Node> treeMap, int character) {
    try {
      return treeMap.get(character);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void add(T obj) throws JsonProcessingException {
    for (Object o : getIndexValue(obj)) {
      if(o instanceof String){
        insert(obj, (String) o);
      }
    }
  }

  @Override
  public void modify(T obj) throws JsonProcessingException {
    String key = getKey(obj);
    Entry entry = entries.get(key);
    if(entry!=null) {
      delete(entry);
      for (Object o : getIndexValue(obj)) {
        if (o instanceof String) {
          insert(obj, (String) o);
        }
      }
    }
  }

  @Override
  public Set<T> get(Object search) {
    if(search instanceof String) {
      return searchData((String)search).stream().collect(Collectors.toSet());
    }
    return (Set<T>) Sets.newTreeSet();
  }

  @Override
  public Set<T> contains(Object searchStr) {
    if(searchStr instanceof String) {
      return partialData((String) searchStr);
    }
    return (Set<T>) Sets.newTreeSet();
  }

  String getKey(T obj){
    String key = null;
    if(obj instanceof DataEntry){
      key = ((DataEntry<?>) obj).getKey();
    }else if(obj instanceof Connection<?,?>){
      key = ((Connection<?, ?>) obj).getUuid();
    }
    return key;
  }
  public void update(Entry entry, String newString){
    delete(entry);
    insert(entry, newString);
  }
  public void delete(T obj){
    String key = getKey(obj);
    if(key!=null){
      Entry entry = entries.get(key);
      if(entry!=null) delete(entry);
    }
  }
  public void delete(Entry entry){
    if(entry!=null){
      Stack<Node> stack = entry.getLastNodes();
      Node tmp;
      while(!stack.isEmpty()){
        Node leaf = stack.lastElement();
        tmp = leaf;
        while(tmp!=null) {
          tmp.getEntries().remove(entry);
          tmp.getPartialEntries().remove(entry);
          if (tmp.getPartialEntries().isEmpty() && tmp.getEntries().isEmpty()) {
            if (tmp.getParent() != null) {
              tmp.getParent().getNodes().remove(tmp.getCharacter());
            }
          }
          tmp = tmp.getParent();
        }
        stack.remove(leaf);
      }
      entries.remove(getKey((T)entry.getData()));
    }
  }

  public void insert(T obj, String val) {
    Entry entry = new Entry(obj, new Stack<>());
    insert(entry, val);
  }

  public void insert(Entry entry, String val) {
    String key = getKey((T)entry.getData());
    PrimitiveIterator.OfInt iterator = val.chars().iterator();
    Node tmp = this.root;
    Stack<Node> nodePath = entry.getLastNodes();
    while (iterator.hasNext()) {
      int character = iterator.next();
      Node nodeTraversal = getNodeFromIntBuffer(tmp.getNodes(), character);
      if (nodeTraversal == null) {
        nodeTraversal = new Node(character);
        nodeTraversal.setParent(tmp);
        tmp.getNodes().put(character, nodeTraversal);

        this.addRoot(nodeTraversal);
      }
      tmp = nodeTraversal;
      tmp.getPartialEntries().add(entry);
    }
    nodePath.add(tmp); // add leaf
    entries.put(key, entry);
    tmp.getEntries().add(entry);
  }

  public Set<T> searchData(String findString) {
    return search(findString).stream().map(e->(T)e.getData()).collect(Collectors.toSet());
  }
  public List<Entry> search(String findString) {
    PrimitiveIterator.OfInt iterator = findString.chars().iterator();
    Node tmp = this.root;
    while (iterator.hasNext()) {
      int character = iterator.next();
      Node nodeTmp = getNodeFromIntBuffer(tmp.getNodes(), character);
      if (tmp.getNodes() != null && nodeTmp == null) {
        tmp = null;
        break;
      }
      tmp = nodeTmp;
    }
    if (tmp == null) {
      return Lists.newLinkedList();
    }
    return tmp.getEntries().stream().collect(Collectors.toList());
  }

  public Set<T> partialData(String findString) {
    return partial(findString).stream().map(e->(T)e.getData()).collect(Collectors.toSet());
  }
  public List<Entry> partial(String findString) {
    LinkedList<Entry> objects = Lists.newLinkedList();
    Root partialRoot = null;
    int start = findString.charAt(0);
    partialRoot = this.rootPartialNodes[start];
    if (partialRoot == null)
      return objects;
    char[] charArray = findString.toCharArray();
    partialRoot.getRoots().stream().forEach(r -> {
      Node tmp = (Node) r;
      for (int i = 1; i < charArray.length; i++) {
        int character = charArray[i];
        Node nodeTmp = getNodeFromIntBuffer(tmp.getNodes(), character);
        if (nodeTmp == null) {
          tmp = null;
          break;
        }
        tmp = nodeTmp;
      }
      if (tmp == null) return;
      objects.addAll(tmp.getPartialEntries());
    });
    return objects.stream().distinct().collect(Collectors.toList());
  }

  public List<Entry> endsWith(String findString) {
    LinkedList<Entry> objects = Lists.newLinkedList();
    Root partialRoot = null;
    int start = findString.charAt(0);
    partialRoot = this.rootPartialNodes[start];
    if (partialRoot == null)
      return objects;
    char[] charArray = findString.toCharArray();
    partialRoot.getRoots().stream().forEach(r -> {
      Node tmp = (Node) r;
      for (int i = 1; i < charArray.length; i++) {
        int character = charArray[i];
        Node nodeTmp = getNodeFromIntBuffer(tmp.getNodes(), character);
        if (nodeTmp == null) {
          tmp = null;
          break;
        }
        tmp = nodeTmp;
      }
      if (tmp == null) return;
      objects.addAll(tmp.getEntries());
    });
    return objects.stream().distinct().collect(Collectors.toList());
  }

  public List<Entry> startsWith(String findString) {
    char[] charArray = findString.toCharArray();
    Node tmp = this.root;
    for (int i = 0; i < charArray.length; i++) {
      int character = charArray[i];
      Node nodeTmp = getNodeFromIntBuffer(tmp.getNodes(), character);
      if (nodeTmp == null) {
        tmp = null;
        break;
      }
      tmp = nodeTmp;
    }
    if (tmp == null) return new LinkedList<>();
    return tmp.getPartialEntries().stream().collect(Collectors.toList());
  }
}