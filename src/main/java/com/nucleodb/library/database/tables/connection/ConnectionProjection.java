package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.database.utils.Pagination;

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class ConnectionProjection<T extends Connection> {
  Pagination pagination = null;

  Predicate<T> filter = null;

  Comparator<T> sort = null;
  boolean write = false;

  public ConnectionProjection(Pagination pagination, Predicate<T> filter) {
    this.pagination = pagination;
    this.filter = filter;
  }

  public ConnectionProjection(Pagination pagination, Predicate<T> filter, Comparator<T> sort) {
    this.pagination = pagination;
    this.filter = filter;
    this.sort = sort;
  }

  public ConnectionProjection(Pagination pagination) {
    this.pagination = pagination;
  }

  public ConnectionProjection(Predicate<T> filter) {
    this.filter = filter;
  }

  public ConnectionProjection() {
  }

  public ConnectionProjection(Pagination pagination, Predicate<T> filter, boolean write) {
    this.pagination = pagination;
    this.filter = filter;
    this.write = write;
  }

  public ConnectionProjection(Pagination pagination, boolean write) {
    this.pagination = pagination;
    this.write = write;
  }

  public ConnectionProjection(Predicate<T> filter, boolean write) {
    this.filter = filter;
    this.write = write;
  }

  public Stream<T> process(Stream<T> connectionStream){
    Stream<T> connectionStreamTmp = connectionStream;
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
      connectionStreamTmp = connectionStreamTmp.skip(this.pagination.getSkip()).limit(this.pagination.getLimit());
    }
    return connectionStreamTmp;
  }
  public void setPagination(Pagination pagination) {
    this.pagination = pagination;
  }
  public void setFilter(Predicate<T> filter) {
    this.filter = filter;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

  public Comparator<T> getSort() {
    return sort;
  }

  public void setSort(Comparator<T> sort) {
    this.sort = sort;
  }

  public Pagination getPagination() {
    return pagination;
  }

  public Predicate<T> getFilter() {
    return filter;
  }
}
