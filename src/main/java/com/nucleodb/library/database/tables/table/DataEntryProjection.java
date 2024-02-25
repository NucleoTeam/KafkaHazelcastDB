package com.nucleodb.library.database.tables.table;

import com.nucleodb.library.database.utils.Pagination;

import javax.xml.crypto.Data;
import java.util.Comparator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataEntryProjection<T extends DataEntry>{
  Pagination pagination = null;

  Predicate<T> filter = null;

  Comparator<T> sort = null;

  boolean writable = false;
  boolean lockUntilWrite = false;

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

  public DataEntryProjection(Pagination pagination, boolean writable, boolean lockUntilWrite) {
    this.pagination = pagination;
    this.writable = writable;
    this.lockUntilWrite = lockUntilWrite;
  }

  public DataEntryProjection(Predicate<T> filter, boolean writable, boolean lockUntilWrite) {
    this.filter = filter;
    this.writable = writable;
    this.lockUntilWrite = lockUntilWrite;
  }

  public DataEntryProjection(Pagination pagination) {
    this.pagination = pagination;
  }

  public DataEntryProjection(Predicate<T> filter) {
    this.filter = filter;
  }

  public DataEntryProjection() {
  }

  public Set<T> process(Stream<T> DataEntryStream, Class<? extends DataEntry> clazz){
    Stream<T> dataEntryStream = DataEntryStream;
    if(this.filter!=null){
      dataEntryStream = dataEntryStream.filter(this.filter);
    }
    if(this.sort!=null){
      dataEntryStream = dataEntryStream.sorted(this.sort);
    }
    if(this.pagination!=null){
      dataEntryStream = dataEntryStream.skip(this.pagination.getSkip()).limit(this.pagination.getLimit());
    }
    if(isWritable()){
      dataEntryStream = dataEntryStream.map(de->(T) de.copy(clazz, lockUntilWrite));
    }
    return dataEntryStream.collect(Collectors.toSet());
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

  public boolean isLockUntilWrite() {
    return lockUntilWrite;
  }

  public void setLockUntilWrite(boolean lockUntilWrite) {
    this.lockUntilWrite = lockUntilWrite;
  }
}
