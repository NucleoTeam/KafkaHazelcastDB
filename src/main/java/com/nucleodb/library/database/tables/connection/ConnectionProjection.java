package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.database.utils.Pagination;

import java.util.Comparator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionProjection {
  Pagination pagination = null;

  Predicate<Connection> filter = null;

  Comparator<Connection> sort = null;
  boolean write = false;

  public ConnectionProjection(Pagination pagination, Predicate<Connection> filter) {
    this.pagination = pagination;
    this.filter = filter;
  }

  public ConnectionProjection(Pagination pagination, Predicate<Connection> filter, Comparator<Connection> sort) {
    this.pagination = pagination;
    this.filter = filter;
    this.sort = sort;
  }

  public ConnectionProjection(Pagination pagination) {
    this.pagination = pagination;
  }

  public ConnectionProjection(Predicate<Connection> filter) {
    this.filter = filter;
  }

  public ConnectionProjection() {
  }

  public ConnectionProjection(Pagination pagination, Predicate<Connection> filter, boolean write) {
    this.pagination = pagination;
    this.filter = filter;
    this.write = write;
  }

  public ConnectionProjection(Pagination pagination, boolean write) {
    this.pagination = pagination;
    this.write = write;
  }

  public ConnectionProjection(Predicate<Connection> filter, boolean write) {
    this.filter = filter;
    this.write = write;
  }

  public Set<Connection> process(Stream<Connection> connectionStream, Class<? extends Connection> clazz){
    Stream<Connection> connectionStreamTmp = connectionStream;
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
      connectionStreamTmp = connectionStreamTmp.map(c->(Connection)c.copy(clazz));
    }
    return connectionStreamTmp.collect(Collectors.toSet());
  }
  public void setPagination(Pagination pagination) {
    this.pagination = pagination;
  }
  public void setFilter(Predicate<Connection> filter) {
    this.filter = filter;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

  public Comparator<Connection> getSort() {
    return sort;
  }

  public void setSort(Comparator<Connection> sort) {
    this.sort = sort;
  }

  public Pagination getPagination() {
    return pagination;
  }

  public Predicate<Connection> getFilter() {
    return filter;
  }
}
