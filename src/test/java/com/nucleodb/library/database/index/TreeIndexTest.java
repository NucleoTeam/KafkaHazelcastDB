package com.nucleodb.library.database.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.index.helpers.TestClass;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.utils.Serializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TreeIndexTest{

  @Test
  void add() throws JsonProcessingException {
    TreeIndex index = new TreeIndex<TestClass>("name");
    index.add(new DataEntry(new TestClass("taco test")));
    assertEquals(1, index.contains("taco").size());
  }

  @Test
  void delete() throws JsonProcessingException {
    DataEntry tacoTest = new DataEntry(new TestClass("taco test"));
    TreeIndex index = new TreeIndex<TestClass>("name");
    index.add(tacoTest);
    assertEquals(1, index.contains("taco").size());
    assertEquals(1, index.getIndex().size());
    assertEquals(1, index.getReverseMap().size());
    index.delete(tacoTest);
    assertEquals(0, index.contains("taco").size());
    assertEquals(0, index.getIndex().size());
    assertEquals(0, index.getReverseMap().size());
  }

  @Test
  void modify() throws JsonProcessingException {
    DataEntry<TestClass> tacoTest = new DataEntry(new TestClass("taco test"));
    TreeIndex index = new TreeIndex<TestClass>("name");
    index.add(tacoTest);
    assertEquals(1, index.contains("taco").size());
    assertEquals(1, index.getIndex().size());
    assertEquals(1, index.getReverseMap().size());
    tacoTest.getData().setName("just test");
    index.modify(tacoTest);
    assertEquals(1, index.contains("just").size());
    assertEquals(1, index.getIndex().size());
    assertEquals(1, index.getReverseMap().size());
  }

  @Test
  void get() {
  }

  @Test
  void contains() {
  }
}