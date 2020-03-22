package com.nucleocore.db.database.index.trie;

import com.google.common.collect.Lists;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TrieNode {
    public List<NodeInner> path = new ArrayList<>();
    public List<Object> entries = null;
    int get(int c){
        int i = Collections.binarySearch(path, new NodeInner(c, null), Comparator.comparingInt(NodeInner::getItem));
        //System.out.println("search result: "+i);
        return i;
    }
    void set(int c, TrieNode node){
        path.add(new NodeInner(c, node));
        Collections.sort(path, Comparator.comparingInt(NodeInner::getItem));
    }
    public void add(String string, String key) {
        if (string.length() == 0) {
            if (entries != null) {
                entries = Lists.newArrayList();
            } else {
                entries = new ArrayList<>();
            }
            synchronized (entries) {
                entries.add(key);
            }
            return;
        }
        char s = string.charAt(0);
        TrieNode n = null;
        int nInt;
        if ((nInt = get(s)) > -1) {
            n = path.get(nInt).getNode();
            synchronized (n) {
                n.add(string.substring(1), key);
            }
        }else{
            n = new TrieNode();
            synchronized (n) {
                n.add(string.substring(1), key);
            }
            set(s, n);
        }
    }
    public List<Object> search(String left){
        if(left.length()==0){
            return entries;
        }
        TrieNode n = null;
        int nInt;
        char s = left.charAt(0);
        if((nInt=get(s))>-1){
            n = path.get(nInt).getNode();
            return n.search(left.substring(1));
        }
        return null;
    }
    public void deleteFromArray(int pos){
        path.remove(pos);
    }
    public boolean remove(String left, String key){
        if(left.length()==0){
            if(entries!=null) {
                Stream<Object> stream = entries.stream();
                List<Object> nodes = stream.filter(i ->!i.equals(key)).collect(Collectors.toList());
                stream.close();
                if (nodes.size() == 0){
                    entries=null;
                    return true;
                }
                entries = nodes;
            }
            return false;
        }
        int nInt = 0;
        TrieNode n;
        char s = left.charAt(0);
        if((nInt=get(s))>-1){
            n = path.get(nInt).getNode();
            if(n.remove(left.substring(1), key)){
                if(n.path.size()==0 && (entries==null || entries.size()==0)){
                    deleteFromArray(nInt);
                    return true;
                }
            }
        }
        return false;
    }
    public class NodeInner{
        public int item=-1;
        public TrieNode node = null;

        public NodeInner(int item, TrieNode node) {
            this.item = item;
            this.node = node;
        }

        public int getItem() {
            return item;
        }

        public void setItem(int item) {
            this.item = item;
        }

        public TrieNode getNode() {
            return node;
        }

        public void setNode(TrieNode node) {
            this.node = node;
        }
    }
}