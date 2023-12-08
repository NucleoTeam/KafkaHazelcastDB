package com.nucleodb.library.database.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.nucleodb.library.database.utils.TreeSetExt;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class TreeIndex<T> extends IndexWrapper<T>{
  private static final long serialVersionUID = 1;
  public TreeIndex() {
    super(null);
  }



  private boolean unique;
  private TreeMap<T, List<Object>> reverseMap = new TreeMap<>();
  private TreeMap<Object, Set<T>> index = new TreeMap<>();


  public TreeIndex(String indexedKey) {
    super(indexedKey);

  }



  @Override
  public void add(T dataEntry) throws JsonProcessingException {
    List<Object> values = getIndexValue(dataEntry);
    //System.out.println(Serializer.getObjectMapper().getOm().writeValueAsString(values));
    values.forEach(val->{
      synchronized (reverseMap) {
        List<Object> rMap = reverseMap.get(dataEntry);
        if (rMap == null) {
          rMap = Lists.newLinkedList();
          reverseMap.put(dataEntry, rMap);
        }
        rMap.add(val);
      }
      synchronized (index) {
        Set<T> entries;
        entries = index.get(val);
        if (entries == null) {
          entries = new TreeSetExt<>();
          index.put(val, entries);

        }
        entries.add(dataEntry);
      }

      //System.out.println("Add, "+ this.getIndexedKey() + " = " +val);

    });
  }

  @Override
  public void delete(T dataEntry) {
    //System.out.println("Delete "+dataEntry);
    List<Object> i = reverseMap.get(dataEntry);
    if(i!=null)
      i.forEach(c -> {
        Set<T> ts = index.get(c);
        if(ts!=null) {
          ts.remove(dataEntry);
          if(ts.isEmpty()){
            index.remove(c);
          }
        }
      });

    reverseMap.remove(dataEntry);
    //System.out.println(reverseMap.get(dataEntry));
  }

  @Override
  public void modify(T dataEntry) throws JsonProcessingException {
    //System.out.println("Modify, "+ this.getIndexedKey() + " = " +dataEntry);
    delete(dataEntry);
    add(dataEntry);
  }

  @Override
  public Set<T> get(Object search) {
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
  public Set<T> contains(Object searchObj) {
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
  Set<T> reduce(SortedMap<Object, Set<T>> objectSetSortedMap){
    return objectSetSortedMap.entrySet()
      .stream()
      .map(c->c.getValue()).reduce(new TreeSetExt<>(), (a,b)->{
        a.addAll(b);
        return a;
      });
  }
  @Override
  public Set<T> lessThan(Object searchObj) {
    return reduce(index.headMap(searchObj));
  }
  @Override
  public Set<T> lessThanEqual(Object searchObj) {
    return reduce(index.headMap(searchObj, true));
  }
  @Override
  public Set<T> greaterThan(Object searchObj) {
    return reduce(index.tailMap(searchObj));
  }
  @Override
  public Set<T> greaterThanEqual(Object searchObj) {
    return reduce(index.tailMap(searchObj, true));
  }


  public TreeMap<T, List<Object>> getReverseMap() {
    return reverseMap;
  }

  public void setReverseMap(TreeMap<T, List<Object>> reverseMap) {
    this.reverseMap = reverseMap;
  }

  public TreeMap<Object, Set<T>> getIndex() {
    return index;
  }

  public void setIndex(TreeMap<Object, Set<T>> index) {
    this.index = index;
  }

  public boolean isUnique() {
    return unique;
  }

  public void setUnique(boolean unique) {
    this.unique = unique;
  }

}
