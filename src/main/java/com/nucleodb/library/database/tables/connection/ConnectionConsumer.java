package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.database.tables.table.DataEntry;

import java.lang.reflect.Type;

public class ConnectionConsumer{
  Type to;
  Type from;
  ConnectionConfig connectionConfig;

  public ConnectionConsumer(Type to, Type from, ConnectionConfig connectionConfig) {
    this.to = to;
    this.from = from;
    this.connectionConfig = connectionConfig;
  }

  public Type getTo() {
    return to;
  }

  public void setTo(Type to) {
    this.to = to;
  }

  public Type getFrom() {
    return from;
  }

  public void setFrom(Type from) {
    this.from = from;
  }

  public ConnectionConfig getConnectionConfig() {
    return connectionConfig;
  }

  public void setConnectionConfig(ConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
  }
}