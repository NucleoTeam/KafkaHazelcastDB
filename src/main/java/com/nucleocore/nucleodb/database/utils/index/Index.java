package com.nucleocore.nucleodb.database.utils.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.Serializer;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonPointer;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonReader;
import java.io.Serial;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class Index implements Serializable{
  private static final long serialVersionUID = 1;
  JsonPointer indexedKey;
  String indexedKeyStr;
  public Index(String indexedKey) {
    this.indexedKey = Json.createPointer(indexedKey);
    this.indexedKeyStr = indexedKey;
  }



  public List<String> getIndexValue(DataEntry dataEntry) throws JsonProcessingException {
    String json = dataEntry.getReference().toString();
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
    return Arrays.asList();
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
