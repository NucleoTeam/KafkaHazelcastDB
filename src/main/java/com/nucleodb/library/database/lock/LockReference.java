package com.nucleodb.library.database.lock;

import java.time.Instant;
import java.util.UUID;

public class LockReference{
  private String tableName;
  private String key;
  private String owner;
  private String request = UUID.randomUUID().toString();
  private Instant time = Instant.now();

  private boolean lock;

  public LockReference() {
  }

  public LockReference(String tableName, String key, String owner, boolean lock) {
    this.tableName = tableName;
    this.key = key;
    this.owner = owner;
    this.lock = lock;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public Instant getTime() {
    return time;
  }

  public void setTime(Instant time) {
    this.time = time;
  }

  public boolean isLock() {
    return lock;
  }

  public void setLock(boolean lock) {
    this.lock = lock;
  }

  public String getRequest() {
    return request;
  }

  public void setRequest(String request) {
    this.request = request;
  }
}