package com.nucleodb.library.database.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.index.helpers.TestClass;
import com.nucleodb.library.database.index.trie.Node;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.utils.Serializer;
import org.junit.jupiter.api.Test;

import java.util.Stack;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;


class TrieIndexTest{

  @Test
  void add() throws JsonProcessingException {
    TrieIndex index = new TrieIndex<TestClass>("name");
    index.add(new DataEntry(new TestClass("taco test")));
    assertEquals(1, index.search("taco").size());
  }

  @Test
  void modify() throws JsonProcessingException {
    DataEntry<TestClass> tacoTest = new DataEntry(new TestClass("taco test"));
    TrieIndex index = new TrieIndex<TestClass>("name");
    index.add(tacoTest);
    tacoTest.getData().setName("burrito");
    index.modify(tacoTest);
    assertEquals(0, index.search("taco").size());
    Stack<Node> nodes = new Stack<Node>();
    nodes.add(index.root);
    int x = 0;
    while(!nodes.isEmpty()){
      Node n = nodes.pop();
      nodes.addAll(n.getNodes().values());
      x++;
    }
    assertEquals(8, x); // characters + 1(root)
    assertEquals(1, index.search("burrito").size());
  }

  @Test
  void get() throws JsonProcessingException {
    String name = "taco test";
    DataEntry<TestClass> tacoTest = new DataEntry(new TestClass(name));
    TrieIndex index = new TrieIndex<TestClass>("name");
    index.add(tacoTest);
    assertEquals(1, index.get(name).size());
    Stack<Node> nodes = new Stack<>();
    nodes.add(index.root);
    int x = 0;
    while(!nodes.isEmpty()){
      Node n = nodes.pop();
      nodes.addAll(n.getNodes().values());
      x++;
    }
    assertEquals(name.length()+1, x); // characters + 1(root)
  }

  @Test
  void contains() throws JsonProcessingException {
    String name = "vestibulum rhoncus est pellentesque elit ullamcorper dignissim cras tincidunt lobortis feugiat vivamus at augue eget arcu dictum varius duis at consectetur lorem donec massa sapien";
    DataEntry<TestClass> tacoTest = new DataEntry(new TestClass(name));
    TrieIndex index = new TrieIndex<TestClass>("name");
    index.add(tacoTest);
    assertEquals(1, index.search("rho").size());
    Stack<Node> nodes = new Stack<>();
    nodes.add(index.root);
    int x = 0;
    while(!nodes.isEmpty()){
      Node n = nodes.pop();
      nodes.addAll(n.getNodes().values());
      x++;
    }
    assertEquals(name.length()+1, x); // characters + 1(root)
    assertEquals(1, index.entries.size());
  }

  @Test
  void delete() throws JsonProcessingException {
    String name = "taco test";
    DataEntry<TestClass> tacoTest = new DataEntry(new TestClass(name));
    TrieIndex index = new TrieIndex<TestClass>("name");
    index.add(tacoTest);
    assertEquals(1, index.get(name).size());
    index.delete(tacoTest);
    Stack<Node> nodes = new Stack<>();
    nodes.add(index.root);
    int x = 0;
    while(!nodes.isEmpty()){
      Node n = nodes.pop();
      nodes.addAll(n.getNodes().values());
      x++;
    }
    assertEquals(1, x); // characters + 1(root)
    assertEquals(0, index.entries.size());
  }
}