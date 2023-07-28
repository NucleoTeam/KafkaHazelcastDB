package com.nucleocore.nucleodb.database.utils.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.database.utils.DataEntry;

import javax.json.Json;
import javax.json.JsonPointer;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class Index{
  JsonPointer indexedKey;
  String indexedKeyStr;
  public Index(String indexedKey) {
    this.indexedKey = Json.createPointer(indexedKey);
    this.indexedKeyStr = indexedKey;
  }



  public List<String> getIndexValue(DataEntry dataEntry) throws JsonProcessingException {
    String json = dataEntry.getReference().toString();
    System.out.println(json);
    try (JsonReader reader = Json.createReader(new StringReader(json))) {
      JsonValue value = indexedKey.getValue(reader.read());
      switch (value.getValueType()) {
        case ARRAY:
          return value.asJsonArray().stream().map(val -> val.toString()).collect(Collectors.toList());
        case STRING:
        case NUMBER:
          return Arrays.asList(value.toString());
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

  public Set<DataEntry> search(String search) {
    return null;
  }

  public String getIndexedKey() {
    return indexedKeyStr;
  }

}
