package com.nucleocore.nucleodb.database.utils.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.Serializer;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonPointer;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonReader;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serial;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public abstract class Index implements Serializable{
  private static final long serialVersionUID = 1;
  String indexedKeyStr;
  public Index(String indexedKey) {
    this.indexedKeyStr = indexedKey;
  }



  public List<String> getIndexValue(DataEntry dataEntry) throws JsonProcessingException {
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

  public List<String> getValues(Queue<String> pointer, Object start) throws JsonProcessingException {
    Object current = start;
    if(pointer.isEmpty()){
      if(current instanceof String){
        System.out.println(current);
        return Arrays.asList((String)current);
      }else if(current instanceof Integer){
        return Arrays.asList(((Integer)current).toString());
      }else if(current instanceof Float) {
        return Arrays.asList(((Float) current).toString());
      }
      return new LinkedList<>();
    }
    try {
      String name = pointer.poll();
      System.out.println(name);
      current = new PropertyDescriptor(name, current.getClass()).getReadMethod().invoke(current);
    } catch (IntrospectionException | InvocationTargetException | IllegalAccessException e) {
      e.printStackTrace();
    }
    if(current instanceof Collection){
      System.out.println(current.getClass().getName());
      return ((Collection<?>) current).stream().map(c-> {
        try {
          return getValues(Queues.newLinkedBlockingDeque(pointer), c);
        } catch (JsonProcessingException e) {
          return new LinkedList<String>();
        }
      }).reduce(new LinkedList<>(), (a, b)->{
        a.addAll(b);
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

  public Set<DataEntry> get(String getByStr) {
    return null;
  }

  public Set<DataEntry> getNotEqual(String notEqualVal) {
    return null;
  }


  public Set<DataEntry> search(String search) {
    return null;
  }

  public String getIndexedKey() {
    return indexedKeyStr;
  }

}
