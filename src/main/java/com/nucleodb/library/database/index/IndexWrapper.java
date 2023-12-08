package com.nucleodb.library.database.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nucleodb.library.database.index.trie.Entry;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.exceptions.InvalidIndexTypeException;
import org.jetbrains.annotations.NotNull;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public abstract class IndexWrapper<T> implements Serializable, Comparable{
  private static final long serialVersionUID = 1;
  String indexedKeyStr;
  public IndexWrapper(String indexedKey) {
    this.indexedKeyStr = indexedKey;
  }

  public List<Object> getIndexValue(T object) throws JsonProcessingException {
    Object obj;
    if(object instanceof DataEntry<?>){
      obj = ((DataEntry<?>) object).getData();
    }else{
      obj = object;
    }
    return getValues(Queues.newConcurrentLinkedQueue(Arrays.asList(this.indexedKeyStr.split("\\."))), obj);
    /*String json = dataEntry.getReference().toString();
    try (JsonReader reader = Json.createReader(new StringReader(json))) {
      System.out.println(json);
      System.out.println(indexedKeyStr);

      JsonValue value = indexedKey.getValue(reader.read());
      switch (value.getValueType()) {
        case ARRAY:
          return value.asJsonArray().stream().map(val -> {
            try {
              return Serializer.getObjectMapper().getOm().readValue(val.toString(), String.class);
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          }).collect(Collectors.toList());
        case STRING:
          return Arrays.asList(((JsonString)value).getString());
        case NUMBER:
          return Arrays.asList(((JsonNumber)value).numberValue().toString());
      }
    }
    return Arrays.asList();*/
  }

  public List<Object> getValues(Queue<String> pointer, Object start) throws JsonProcessingException {
    if(start== null){
      return new LinkedList<>();
    }
    Object current = start;
    if(pointer.isEmpty()){
      return Arrays.asList(current);
    }
    try {
      String name = pointer.poll();
      current = new PropertyDescriptor(name, current.getClass()).getReadMethod().invoke(current);
    } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
      //e.printStackTrace();
      return new LinkedList<>();
    }
    if(current instanceof Collection){
      return ((Collection<?>) current).stream().map(c-> {
        try {
          return getValues(Queues.newConcurrentLinkedQueue(pointer), c);
        } catch (JsonProcessingException e) {
          return new LinkedList();
        }
      }).reduce(new LinkedList(), (a, b)->{
        boolean b1 = a.addAll(b);
        return a;
      });
    }else if(current instanceof Object){
      return getValues(pointer, current);
    }
    return new LinkedList<>();
  }


  public void add(T dataEntry) throws JsonProcessingException, InvalidIndexTypeException {
    throw new InvalidIndexTypeException("index selected does not implement add.");
  }

  public void delete(T dataEntry) throws InvalidIndexTypeException {
    throw new InvalidIndexTypeException("index selected does not implement delete.");
  }

  public void modify(T dataEntry) throws JsonProcessingException, InvalidIndexTypeException {
    throw new InvalidIndexTypeException("index selected does not implement modify.");
  }

  public Set<T> get(Object search) {
    return (Set<T>) Sets.newTreeSet();
  }

  public Set<T> contains(Object searchObj) {
    return (Set<T>) Sets.newTreeSet();
  }

  public String getIndexedKey() {
    return indexedKeyStr;
  }

  public Set<T> endsWith(String findString)  {
    return (Set<T>) Sets.newTreeSet();
  }

  public Set<T> startsWith(String findString) {
    return (Set<T>) Sets.newTreeSet();
  }

  public Set<T> lessThan(Object searchObj) {
    return (Set<T>) Sets.newTreeSet();
  }

  public Set<T> lessThanEqual(Object searchObj) {
    return (Set<T>) Sets.newTreeSet();
  }

  public Set<T> greaterThan(Object searchObj)  {
    return (Set<T>) Sets.newTreeSet();
  }

  public Set<T> greaterThanEqual(Object searchObj)  {
    return (Set<T>) Sets.newTreeSet();
  }

  @Override
  public int compareTo(@NotNull Object o) {
    if(o instanceof IndexWrapper) {
      return indexedKeyStr.compareTo(((IndexWrapper<?>) o).getIndexedKey());
    }
    return 0;
  }
}
