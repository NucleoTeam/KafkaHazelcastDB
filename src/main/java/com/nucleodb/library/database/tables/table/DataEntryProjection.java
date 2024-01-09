package com.nucleodb.library.database.tables.table;

import com.nucleodb.library.database.utils.Pagination;

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class DataEntryProjection<T extends DataEntry>{
  Pagination pagination = null;

  Predicate<T> filter = null;

  Comparator<T> sort = null;

  boolean writable = true;

  public DataEntryProjection(Pagination pagination, Predicate<T> filter) {
    this.pagination = pagination;
    this.filter = filter;
  }

  public DataEntryProjection(Pagination pagination, Predicate<T> filter, Comparator<T> sort) {
    this.pagination = pagination;
    this.filter = filter;
    this.sort = sort;
  }

  public DataEntryProjection(Pagination pagination, Comparator<T> sort) {
    this.pagination = pagination;
    this.sort = sort;
  }

  public DataEntryProjection(Pagination pagination) {
    this.pagination = pagination;
  }

  public DataEntryProjection(Predicate<T> filter) {
    this.filter = filter;
  }

  public DataEntryProjection() {
  }

  public Stream<T> process(Stream<T> DataEntryStream){
    Stream<T> DataEntryStreamTmp = DataEntryStream;
    if(this.filter!=null){
      DataEntryStreamTmp = DataEntryStreamTmp.filter(this.filter);
    }
    if(this.sort!=null){
      DataEntryStreamTmp = DataEntryStreamTmp.sorted(this.sort);
    }
    if(this.pagination!=null){
      DataEntryStreamTmp = DataEntryStreamTmp.skip(this.pagination.getSkip()).limit(this.pagination.getLimit());
    }
    return DataEntryStreamTmp;
  }
  public void setPagination(Pagination pagination) {
    this.pagination = pagination;
  }
  public void setFilter(Predicate<T> filter) {
    this.filter = filter;
  }

  public Pagination getPagination() {
    return pagination;
  }

  public Predicate<T> getFilter() {
    return filter;
  }

  public boolean isWritable() {
    return writable;
  }

  public void setWritable(boolean writable) {
    this.writable = writable;
  }

  public Comparator<T> getSort() {
    return sort;
  }

  public void setSort(Comparator<T> sort) {
    this.sort = sort;
  }
}
