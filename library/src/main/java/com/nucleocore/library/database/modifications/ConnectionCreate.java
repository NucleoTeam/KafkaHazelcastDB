package com.nucleocore.library.database.modifications;

import com.nucleocore.library.database.tables.Connection;
import com.nucleocore.library.database.utils.DataEntry;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

public class ConnectionCreate extends Modify {
  public Connection connection;

  public long version;
  public Instant time;
  public String changeUUID = UUID.randomUUID().toString();

  public ConnectionCreate() {
    this.time = Instant.now();
  }

  public ConnectionCreate(Connection connection) {
    this.connection = connection;
    this.time = Instant.now();
  }

  public ConnectionCreate(String changeUUID, Connection connection) {
    this.connection = connection;
    this.changeUUID = changeUUID;
    this.time = Instant.now();
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public Instant getTime() {
    return time;
  }

  public void setTime(Instant time) {
    this.time = time;
  }

  public String getChangeUUID() {
    return changeUUID;
  }

  public void setChangeUUID(String changeUUID) {
    this.changeUUID = changeUUID;
  }
}
