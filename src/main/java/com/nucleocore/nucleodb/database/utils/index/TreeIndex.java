package com.nucleocore.nucleodb.database.utils.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

public class TreeIndex extends Index{

  class TreeSetExt<V> extends TreeSet<V> implements Comparable<TreeSetExt> {
    String uuid;
    public TreeSetExt() {
      uuid = UUID.randomUUID().toString();
    }

    public TreeSetExt(Comparator<? super V> comparator) {
      super(comparator);
      uuid = UUID.randomUUID().toString();
    }

    public TreeSetExt(@NotNull Collection<? extends V> c) {
      super(c);
      uuid = UUID.randomUUID().toString();
    }

    public TreeSetExt(SortedSet<V> s) {
      super(s);
      uuid = UUID.randomUUID().toString();
    }

    @Override
    public int compareTo(@NotNull TreeSetExt o) {
      return o.toString().compareTo(this.toString());
    }

    @Override
    public boolean equals(Object o) {
      if(o instanceof TreeSetExt){
        return ((TreeSetExt<?>) o).uuid.equals(this.uuid);
      }
      return super.equals(o);
    }
  }

  Map<DataEntry, Set<Set<DataEntry>>> reverseMap = new TreeMap<>();
  private Map<String, Set<DataEntry>> index = new TreeMap<>();


  public TreeIndex(String indexedKey) {
    super(indexedKey);

  }



  @Override
  public void add(DataEntry dataEntry) throws JsonProcessingException {
    List<String> values = getIndexValue(dataEntry);
    values.forEach(val->{
      Set<DataEntry> entries = index.get(val);
      if(entries==null){
        entries = new TreeSetExt<>();
        index.put(val, entries);
      }
      entries.add(dataEntry);
      Set<Set<DataEntry>> rMap = reverseMap.get(dataEntry);
      if(rMap==null){
        rMap = new TreeSetExt<>();
        reverseMap.put(dataEntry, rMap);
      }
      System.out.println("Add, "+ this.getIndexedKey() + " = " +val);
      rMap.add(entries);
    });
  }

  @Override
  public void delete(DataEntry dataEntry) {
    System.out.println("Delete "+dataEntry);
    reverseMap.get(dataEntry).forEach(c -> c.remove(dataEntry));
    reverseMap.remove(dataEntry);
    System.out.println(reverseMap.get(dataEntry));
  }

  @Override
  public void modify(DataEntry dataEntry) throws JsonProcessingException {
    System.out.println("Modify, "+ this.getIndexedKey() + " = " +dataEntry);
    delete(dataEntry);
    add(dataEntry);
  }

  @Override
  public Set<DataEntry> get(String search) {
    return index.get(search);
  }

  @Override
  public Set<DataEntry> search(String search) {
    return index.keySet().stream().filter(key->key.contains(search)).map(key->index.get(key)).filter(i->i!=null).reduce(new TreeSet<>(), (a,b)->{
      a.addAll(b);
      return a;
    });
  }

}
