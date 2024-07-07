package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.database.utils.Pagination;

import java.util.Comparator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionProjection<C extends Connection> {
  Pagination pagination = null;

  Predicate<C> filter = null;

  Comparator<C> sort = null;
  boolean write = false;
  boolean lock = false;

  public ConnectionProjection(Pagination pagination, Predicate<C> filter) {
    this.pagination = pagination;
    this.filter = filter;
  }

  public ConnectionProjection(Pagination pagination, Predicate<C> filter, Comparator<C> sort) {
    this.pagination = pagination;
    this.filter = filter;
    this.sort = sort;
  }

  public ConnectionProjection(Pagination pagination) {
    this.pagination = pagination;
  }

  public ConnectionProjection(Predicate<C> filter) {
    this.filter = filter;
  }

  public ConnectionProjection() {
  }

  public ConnectionProjection(Pagination pagination, Predicate<C> filter, boolean write) {
    this.pagination = pagination;
    this.filter = filter;
    this.write = write;
  }

  public ConnectionProjection(Pagination pagination, boolean write) {
    this.pagination = pagination;
    this.write = write;
  }

  public ConnectionProjection(Predicate<C> filter, boolean write) {
    this.filter = filter;
    this.write = write;
  }

  public ConnectionProjection(Predicate<C> filter, boolean write, boolean lock) {
    this.filter = filter;
    this.write = write;
    this.lock = lock;
  }

  public Set<Connection> process(Stream<C> connectionStream, Class<? extends Connection> clazz){
    Stream<C> connectionStreamTmp = connectionStream;
    if(this.filter!=null){
      connectionStreamTmp = connectionStreamTmp.filter(this.filter);
    }
    if(this.sort!=null){
      connectionStreamTmp = connectionStreamTmp.sorted(this.sort);
    }
    if(this.pagination!=null){
      connectionStreamTmp = connectionStreamTmp.skip(this.pagination.getSkip()).limit(this.pagination.getLimit());
    }
    if(this.write){
      connectionStreamTmp = connectionStreamTmp.map(c->(C)c.copy(clazz, lock));
    }
    return connectionStreamTmp.collect(Collectors.toSet());
  }
  public void setPagination(Pagination pagination) {
    this.pagination = pagination;
  }
  public void setFilter(Predicate<C> filter) {
    this.filter = filter;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

  public Comparator<C> getSort() {
    return sort;
  }

  public void setSort(Comparator<C> sort) {
    this.sort = sort;
  }

  public Pagination getPagination() {
    return pagination;
  }

  public Predicate<C> getFilter() {
    return filter;
  }

  public boolean isWrite() {
    return write;
  }

  public boolean isLock() {
    return lock;
  }

  public void setLock(boolean lock) {
    this.lock = lock;
  }
}
