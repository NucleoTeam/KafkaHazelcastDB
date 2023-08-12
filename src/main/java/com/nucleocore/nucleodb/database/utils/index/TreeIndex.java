package com.nucleocore.nucleodb.database.utils.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.Serializer;
import com.nucleocore.nucleodb.database.utils.TreeSetExt;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

public class TreeIndex extends Index implements Serializable{
  private static final long serialVersionUID = 1;
  public TreeIndex() {
    super(null);
  }



  private boolean unique;
  private Map<DataEntry, Set<Set<DataEntry>>> reverseMap = new TreeMap<>();
  private Map<Object, Set<DataEntry>> index = new TreeMap<>();


  public TreeIndex(String indexedKey) {
    super(indexedKey);

  }



  @Override
  public void add(DataEntry dataEntry) throws JsonProcessingException {
    List<Object> values = getIndexValue(dataEntry);
    System.out.println(Serializer.getObjectMapper().getOm().writeValueAsString(values));
    values.forEach(val->{
      Set<DataEntry> entries;
      synchronized (index) {
        entries = index.get(val);
        if (entries == null) {
          entries = new TreeSetExt<>();
          index.put(val, entries);
        }
      }
      entries.add(dataEntry);
      Set<Set<DataEntry>> rMap;
      synchronized (reverseMap) {
        rMap = reverseMap.get(dataEntry);
        if (rMap == null) {
          rMap = new TreeSetExt<>();
          reverseMap.put(dataEntry, rMap);
        }
      }
      rMap.add(entries);
      //System.out.println("Add, "+ this.getIndexedKey() + " = " +val);

    });
  }

  @Override
  public void delete(DataEntry dataEntry) {
    //System.out.println("Delete "+dataEntry);
    Set<Set<DataEntry>> i = reverseMap.get(dataEntry);
    if(i!=null)
      i.forEach(c -> c.remove(dataEntry));
    reverseMap.remove(dataEntry);
    //System.out.println(reverseMap.get(dataEntry));
  }

  @Override
  public void modify(DataEntry dataEntry) throws JsonProcessingException {
    //System.out.println("Modify, "+ this.getIndexedKey() + " = " +dataEntry);
    delete(dataEntry);
    add(dataEntry);
  }

  @Override
  public Set<DataEntry> get(Object search) {
    Optional<Object> optionalO = index.keySet().stream().findFirst();
    if(optionalO.isPresent()) {
      Object o = optionalO.get();
      if (o.getClass() == Float.class || o.getClass() == float.class) {
        if (search.getClass() == Double.class)
          return index.get(Double.valueOf((Double) search).floatValue());
        if (search.getClass() == Long.class)
          return index.get(Long.valueOf((Long) search).floatValue());
      } else if (o.getClass() == Integer.class || o.getClass() == int.class) {
        if (search.getClass() == Double.class)
          return index.get(Double.valueOf((Double) search).intValue());
        if (search.getClass() == Long.class)
          return index.get(Long.valueOf((Long) search).intValue());
      } else {
        return index.get(search);
      }
    }
    return new TreeSetExt<>();
  }



  @Override
  public Set<DataEntry> search(Object searchObj) {
    return index.keySet().stream().filter(key->{
      if(key instanceof String && searchObj instanceof String){
        return ((String) key).contains((String)searchObj);
      }else{
        return key.equals(searchObj);
      }
    }).map(key->index.get(key)).filter(i->i!=null).reduce(new TreeSet<>(), (a,b)->{
      a.addAll(b);
      return a;
    });
  }

  public Map<DataEntry, Set<Set<DataEntry>>> getReverseMap() {
    return reverseMap;
  }

  public void setReverseMap(Map<DataEntry, Set<Set<DataEntry>>> reverseMap) {
    this.reverseMap = reverseMap;
  }

  public Map<Object, Set<DataEntry>> getIndex() {
    return index;
  }

  public void setIndex(Map<Object, Set<DataEntry>> index) {
    this.index = index;
  }

  public boolean isUnique() {
    return unique;
  }

  public void setUnique(boolean unique) {
    this.unique = unique;
  }
}
