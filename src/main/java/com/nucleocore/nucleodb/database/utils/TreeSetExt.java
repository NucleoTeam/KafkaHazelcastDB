package com.nucleocore.nucleodb.database.utils;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

public class TreeSetExt<V> extends TreeSet<V> implements Comparable<TreeSetExt>, Serializable{
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
      return this.uuid.compareTo(o.uuid);
    }

    @Override
    public boolean equals(Object o) {
      if(o instanceof TreeSetExt){
        return ((TreeSetExt<?>) o).uuid.equals(this.uuid);
      }
      return this.equals(o);
    }
  }