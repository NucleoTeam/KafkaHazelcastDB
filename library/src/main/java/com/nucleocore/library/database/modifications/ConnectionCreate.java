package com.nucleocore.library.database.modifications;

import com.nucleocore.library.database.tables.connection.Connection;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class ConnectionCreate extends Modify {

  private String uuid;
  private String fromKey;
  private String toKey;
  private Instant date;
  private Map<String, String> metadata = new TreeMap<>();

  public long version;
  public String changeUUID = UUID.randomUUID().toString();

  public ConnectionCreate() {
    this.date = Instant.now();
  }

  public ConnectionCreate(Connection connection) {
    this.date = connection.getDate();
    this.fromKey = connection.getFromKey();
    this.toKey = connection.getToKey();
    this.uuid = connection.getUuid();
    this.date = connection.getDate();
    this.metadata = connection.getMetadata();
    this.version = connection.getVersion();
  }

  public ConnectionCreate(String changeUUID, Connection connection) {
    this.changeUUID = changeUUID;
    this.fromKey = connection.getFromKey();
    this.toKey = connection.getToKey();
    this.uuid = connection.getUuid();
    this.date = connection.getDate();
    this.metadata = connection.getMetadata();
    this.version = connection.getVersion();
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

  public String getFromKey() {
    return fromKey;
  }

  public void setFromKey(String fromKey) {
    this.fromKey = fromKey;
  }

  public String getToKey() {
    return toKey;
  }

  public void setToKey(String toKey) {
    this.toKey = toKey;
  }

  public Instant getDate() {
    return date;
  }

  public void setDate(Instant date) {
    this.date = date;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }
}
