package com.nucleodb.library.database.modifications;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.fge.jsonpatch.JsonPatch;
import com.nucleodb.library.database.utils.JsonOperations;
import com.nucleodb.library.database.utils.Serializer;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class ConnectionUpdate extends Modify{
  public long version;
  public Instant time;
  public String changes;
  public String uuid;
  public String request;
  public String changeUUID = UUID.randomUUID().toString();

  public ConnectionUpdate() throws IOException {
    this.time = Instant.now();

  }

  public ConnectionUpdate(long version, String changes, String changeUUID, String uuid, String request) {
    this.version = version;
    this.changes = changes;
    this.changeUUID = changeUUID;
    this.uuid = uuid;
    this.time = Instant.now();
    this.request = request;
  }

  public ConnectionUpdate(long version, Instant time, String changes, String uuid, String request) {
    this.version = version;
    this.time = time;
    this.changes = changes;
    this.uuid = uuid;
    this.request = request;
  }

  @JsonIgnore
  public JsonPatch getChangesPatch() {
    try {
      return Serializer.getObjectMapper().getOmNonType().readValue(changes, JsonPatch.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @JsonIgnore
  public List<JsonOperations> getOperations() {
    try {
      return Serializer.getObjectMapper().getOmNonType().readValue(changes, new TypeReference<List<JsonOperations>>(){});
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

  public Instant getTime() {
    return time;
  }

  public void setTime(Instant time) {
    this.time = time;
  }

  public String getChanges() {
    return changes;
  }

  public void setChanges(String changes) {
    this.changes = changes;
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

  public String getRequest() {
    return request;
  }

  public void setRequest(String request) {
    this.request = request;
  }
}
