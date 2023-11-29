package com.nucleodb.library.database.utils;

public class Pagination {
  long skip = 0;
  long limit = 50;
  public Pagination() {
  }
  public Pagination(long skip, long limit) {
    this.skip = skip;
    this.limit = limit;
  }

  public long getSkip() {
    return skip;
  }

  public void setSkip(long skip) {
    this.skip = skip;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }
}
