package com.nucleodb.library.database.modifications;

import com.nucleodb.library.database.tables.connection.Connection;

import java.time.Instant;
import java.util.UUID;

public class ConnectionDelete extends Modify {
  public long version;
  public Instant time;
  public String uuid;
  public String changeUUID = UUID.randomUUID().toString();

  public ConnectionDelete() {
    this.time = Instant.now();
  }

  public ConnectionDelete(Connection connection) {
    this.uuid = connection.getUuid();
    this.version = connection.getVersion();
    this.time = Instant.now();
  }
  public ConnectionDelete(String changeUUID, Connection connection)  {
    this.uuid = connection.getUuid();
    this.changeUUID = changeUUID;
    this.version = connection.getVersion();
    this.time = Instant.now();
  }

  public ConnectionDelete(String uuid)  {
    this.uuid = uuid;
    this.time = Instant.now();
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

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getChangeUUID() {
    return changeUUID;
  }

  public void setChangeUUID(String changeUUID) {
    this.changeUUID = changeUUID;
  }
}
