package com.nucleocore.library.database.tables.table;

import com.nucleocore.library.database.utils.Pagination;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class DataEntryProjection{
  Pagination pagination = null;

  Predicate<DataEntry> filter = null;

  public DataEntryProjection(Pagination pagination, Predicate<DataEntry> filter) {
    this.pagination = pagination;
    this.filter = filter;
  }

  public DataEntryProjection(Pagination pagination) {
    this.pagination = pagination;
  }

  public DataEntryProjection(Predicate<DataEntry> filter) {
    this.filter = filter;
  }

  public DataEntryProjection() {
  }

  public Stream<DataEntry> process(Stream<DataEntry> DataEntryStream){
    Stream<DataEntry> DataEntryStreamTmp = DataEntryStream;
    if(this.filter!=null){
      DataEntryStreamTmp = DataEntryStreamTmp.filter(this.filter);
    }
    if(this.pagination!=null){
      DataEntryStreamTmp = DataEntryStreamTmp.skip(this.pagination.getSkip()).limit(this.pagination.getLimit());
    }
    return DataEntryStreamTmp;
  }
  public void setPagination(Pagination pagination) {
    this.pagination = pagination;
  }
  public void setFilter(Predicate<DataEntry> filter) {
    this.filter = filter;
  }
}
