package com.nucleocore.db.database.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.stream.Collectors;

public class TrieNode {
    public NodeInner[] path = {};
    public List<String> entries = null;
    int get(int c){
        int i = Arrays.binarySearch(path, new NodeInner(c, null), Comparator.comparingInt(NodeInner::getItem));
        //System.out.println("search result: "+i);
        return i;
    }
    void set(int c, TrieNode node){
        int length = path.length;
        if(length==0){
            path = new NodeInner[]{new NodeInner(c, node)};
            return;
        }
        NodeInner[] tmpPath = new NodeInner[length+1];
        System.arraycopy(path, 0, tmpPath, 0, length);
        System.arraycopy(new NodeInner[]{new NodeInner(c, node)}, 0, tmpPath, length, 1);
        /*try {
            System.out.println(new ObjectMapper().writeValueAsString(tmpPath));
        }catch (JsonProcessingException e){
            e.printStackTrace();
        }*/
        Arrays.sort(tmpPath, Comparator.comparingInt(NodeInner::getItem));
        path = tmpPath;
    }
    public void add(String string, String de) {
        if (string.length() == 0) {
            if (entries == null)
                entries = new ArrayList<>();
            entries.add(de);
            return;
        }
        char s = string.charAt(0);
        TrieNode n = null;
        int nInt;
        if ((nInt = get(s)) > -1) {
            n = path[nInt].getNode();
            n.add(string.substring(1), de);
        }else{
            n = new TrieNode();
            n.add(string.substring(1), de);
            set(s, n);
        }
    }
    public List<String> search(String left){
        if(left.length()==0){
            return entries;
        }
        TrieNode n = null;
        int nInt;
        char s = left.charAt(0);
        if((nInt=get(s))>-1){
            n = path[nInt].getNode();
            return n.search(left.substring(1));
        }
        return null;
    }
    public void deleteFromArray(int pos){
        int length = path.length;

        NodeInner[] tmpPath = new NodeInner[length-1];

        if(pos>0)
            System.arraycopy(path, 0, tmpPath, 0, pos);
        /*try {
            System.out.println(new ObjectMapper().writeValueAsString(path));
            System.out.println(pos + 1);
            System.out.println(new ObjectMapper().writeValueAsString(tmpPath));
            System.out.println(pos);
            System.out.println(length - pos);
        }catch (Exception e){
            e.printStackTrace();
        }*/
        if(pos+1<length)
            System.arraycopy(path, pos+1, tmpPath, pos, length-pos-1);
        try {
            System.out.println(new ObjectMapper().writeValueAsString(tmpPath));
        }catch (Exception e){
            e.printStackTrace();
        }
        //System.out.println("=============================================================");
        Arrays.sort(tmpPath, Comparator.comparingInt(NodeInner::getItem));

        path = tmpPath;
    }
    public boolean remove(String left, String key){
        if(left.length()==0){
            if(entries!=null) {
                Set<String> nodes = entries.parallelStream().filter(i -> i.equals(key)).collect(Collectors.toSet());
                for (String nodeToDelete : nodes) {
                    entries.remove(nodeToDelete);
                }
                if (entries.size() == 0){
                    System.gc();
                    return true;
                }

            }
            return false;
        }
        int nInt = 0;
        TrieNode n;
        char s = left.charAt(0);
        if((nInt=get(s))>-1){
            n = path[nInt].getNode();
            if(n.remove(left.substring(1), key)){
                if(n.path.length==0 && (entries==null || entries.size()==0)){
                    deleteFromArray(nInt);
                    System.gc();
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
