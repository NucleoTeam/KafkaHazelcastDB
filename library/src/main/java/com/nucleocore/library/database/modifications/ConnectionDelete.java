package com.nucleocore.library.database.modifications;

import com.nucleocore.library.database.tables.Connection;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

public class ConnectionDelete extends Modify {
  public long version;
  public Instant time;
  public String uuid;
  public String changeUUID = UUID.randomUUID().toString();

  public ConnectionDelete() {
  }

  public ConnectionDelete(Connection connection) throws IOException {
    this.uuid = connection.getUuid();
  }
  public ConnectionDelete(String changeUUID, Connection connection) throws IOException {
    this.uuid = connection.getUuid();
    this.changeUUID = changeUUID;
  }

  public ConnectionDelete(String uuid) throws IOException {
    this.uuid = uuid;
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
