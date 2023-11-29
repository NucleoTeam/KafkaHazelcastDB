package com.nucleodb.library.database.modifications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.tables.connection.Connection;
import com.nucleodb.library.database.utils.Serializer;

import java.time.Instant;
import java.util.UUID;

public class ConnectionCreate extends Modify {

  private String connectionData;
  private String uuid;
  private Instant date;

  public long version;
  public String changeUUID = UUID.randomUUID().toString();

  public ConnectionCreate() {
    this.date = Instant.now();
  }

  public ConnectionCreate(Connection connection) {
    this.date = connection.getDate();
    this.uuid = connection.getUuid();
    this.date = connection.getDate();
    this.version = connection.getVersion();
    try {
      this.connectionData = Serializer.getObjectMapper().getOm().writeValueAsString(connection);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public ConnectionCreate(String changeUUID, Connection connection) {
    this.changeUUID = changeUUID;
    this.uuid = connection.getUuid();
    this.date = connection.getDate();
    this.version = connection.getVersion();
    try {
      this.connectionData = Serializer.getObjectMapper().getOm().writeValueAsString(connection);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public String getChangeUUID() {
    return changeUUID;
  }

  public void setChangeUUID(String changeUUID) {
    this.changeUUID = changeUUID;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public Instant getDate() {
    return date;
  }

  public void setDate(Instant date) {
    this.date = date;
  }

  public String getConnectionData() {
    return connectionData;
  }

  public void setConnectionData(String connectionData) {
    this.connectionData = connectionData;
  }
}
