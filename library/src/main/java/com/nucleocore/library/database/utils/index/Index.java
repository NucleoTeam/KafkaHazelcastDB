package com.nucleocore.library.database.utils.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Queues;
import com.nucleocore.library.database.utils.DataEntry;

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

public abstract class Index implements Serializable{
  private static final long serialVersionUID = 1;
  String indexedKeyStr;
  public Index(String indexedKey) {
    this.indexedKeyStr = indexedKey;
  }



  public List<Object> getIndexValue(DataEntry dataEntry) throws JsonProcessingException {
    return getValues(Queues.newLinkedBlockingDeque(Arrays.asList(this.indexedKeyStr.split("\\."))), dataEntry.getData());
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
      //Serializer.log(name);
      //Serializer.log(current.getClass().getName());
      current = new PropertyDescriptor(name, current.getClass()).getReadMethod().invoke(current);
    } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
      //e.printStackTrace();
      return new LinkedList<>();
    }
    if(current instanceof Collection){
      //System.out.println(current.getClass().getName());
      return ((Collection<?>) current).stream().map(c-> {
        try {
          return getValues(Queues.newLinkedBlockingDeque(pointer), c);
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


  public void add(DataEntry dataEntry) throws JsonProcessingException {
    System.out.println("Add ERROR");
  }

  public void delete(DataEntry dataEntry) {
    System.out.println("Delete ERROR");
  }

  public void modify(DataEntry dataEntry) throws JsonProcessingException {
    System.out.println("Modify ERROR");
  }

  public Set<DataEntry> get(Object search){
    return null;
  }

  public Set<DataEntry> getNotEqual(Object notEqualVal) {
    return null;
  }


  public Set<DataEntry> search(Object searchObj) {
    return null;
  }

  public String getIndexedKey() {
    return indexedKeyStr;
  }

}
