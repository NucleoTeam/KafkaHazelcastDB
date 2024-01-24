package com.nucleodb.library.database.tables.connection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.library.database.lock.LockReference;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.SkipCopy;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

public class Connection<T extends DataEntry, F extends DataEntry> implements Serializable, Comparable<Connection>{
  @SkipCopy
  private static final long serialVersionUID = 1;

  private String uuid;
  private String fromKey;
  private String toKey;

  private String request;
  private Instant date;
  private Instant modified;
  public long version = 0;
  private Map<String, String> metadata = new TreeMap<>();

  @JsonIgnore
  public transient ConnectionHandler connectionHandler;



  public Connection() {
    this.uuid = UUID.randomUUID().toString();
    this.date = Instant.now();
    this.modified = Instant.now();
  }

  public Connection(F from, T to) {
    this.uuid = UUID.randomUUID().toString();
    this.fromKey = from.getKey();
    this.toKey = to.getKey();
    this.date = Instant.now();
    this.modified = Instant.now();
  }

  public <T extends Connection> T copy(Class<T> clazz, boolean lock) {
    if (lock){
      // get lock
      try {
        LockReference lockReference = this.connectionHandler.getNucleoDB().getLockManager().waitForLock(
            this.connectionHandler.getConfig().getLabel(),
            uuid
        );
        System.out.println("unlocked "+lockReference.getRequest());
        try {
          T obj =  Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(this), clazz);
          obj.setRequest(lockReference.getRequest());
          ((Connection)obj).connectionHandler = this.connectionHandler;
          return obj;
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }else{
      try {
        T obj =  Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(this), clazz);
        ((Connection)obj).connectionHandler = this.connectionHandler;
        return obj;
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
    }

    return null;
  }

  public Connection(F from, T to, Map<String, String> metadata) {
    this.uuid = UUID.randomUUID().toString();
    this.fromKey = from.getKey();
    this.toKey = to.getKey();
    this.metadata = metadata;
    this.date = Instant.now();
    this.modified = Instant.now();
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

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public void versionIncrease(){
    version+=1;
    this.modified = Instant.now();
  }

  public Instant getDate() {
    return date;
  }

  public void setDate(Instant date) {
    this.date = date;
  }

  public Instant getModified() {
    return modified;
  }

  public void setModified(Instant modified) {
    this.modified = modified;
  }

  @Override
  protected Connection clone() {
    Connection clonedConnection = new Connection();
    clonedConnection.uuid = this.uuid;
    clonedConnection.fromKey = this.fromKey;
    clonedConnection.toKey = this.toKey;
    clonedConnection.date = this.date;
    clonedConnection.modified = this.modified;
    clonedConnection.connectionHandler = this.connectionHandler;
    clonedConnection.version = this.version;
    clonedConnection.metadata = new TreeMap<>(this.metadata); // Create a copy of the metadata map

    return clonedConnection;
  }

  @Override
  public int compareTo(@NotNull Connection o) {
    return this.getUuid().compareTo(o.getUuid());
  }

  @JsonIgnore
  public T toEntry(){
    if(this.connectionHandler!=null) {
      DataTable table = this.connectionHandler.getNucleoDB().getTable(this.connectionHandler.getConfig().getToTable());
      Set<DataEntry> tmp = table.get("id", this.getToKey(), null);
      if (tmp != null) {
        Optional<DataEntry> tmpOp = tmp.stream().findFirst().map(c->c.copy(table.getConfig().getDataEntryClass(), false)).map(DataEntry.class::cast);
        if (tmpOp.isPresent()) {
          return (T)tmpOp.get();
        }
      }
    }
    return null;
  }
  @JsonIgnore
  public F fromEntry(){
    if(this.connectionHandler!=null) {
      DataTable table = this.connectionHandler.getNucleoDB().getTable(this.connectionHandler.getConfig().getFromTable());
      Set<DataEntry> tmp = table.get("id", this.getFromKey(), null);
      if (tmp != null) {
        Optional<DataEntry> tmpOp = tmp.stream().findFirst().map(c->c.copy(table.getConfig().getDataEntryClass(), false)).map(DataEntry.class::cast);
        if (tmpOp.isPresent()) {
          return (F)tmpOp.get();
        }
      }
    }
    return null;
  }

  public String getRequest() {
    return request;
  }

  public void setRequest(String request) {
    this.request = request;
  }
}
