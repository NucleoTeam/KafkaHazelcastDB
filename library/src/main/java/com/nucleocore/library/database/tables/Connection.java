package com.nucleocore.library.database.tables;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.SkipCopy;
import org.jetbrains.annotations.NotNull;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

public class Connection implements Serializable, Comparable<Connection>{
  private static final long serialVersionUID = 1;

  private String uuid;

  private String fromKey;
  private String fromTable;
  private String toKey;
  private String toTable;
  private String label;
  private Instant date;

  private Instant modified;

  public long version = 0;

  @JsonIgnore
  public transient ConnectionHandler connectionHandler;

  private Map<String, String> metadata = new TreeMap<>();

  public Connection() {
    this.uuid = UUID.randomUUID().toString();
    this.date = Instant.now();
    this.modified = Instant.now();
  }

  public Connection(DataEntry from, String label, DataEntry to) {
    this.uuid = UUID.randomUUID().toString();
    this.fromKey = from.getKey();
    this.toKey = to.getKey();
    this.label = label;
    this.toTable = to.getTableName();
    this.fromTable = from.getTableName();
    this.date = Instant.now();
    this.modified = Instant.now();
  }

  public Connection(Connection toCopy) {
    try {
      for (Field field : this.getClass().getDeclaredFields()) {
        if(field.isAnnotationPresent(SkipCopy.class)) continue;
        field.set(this, field.get(toCopy));
      }
    }catch (Exception e){
      e.printStackTrace();
    }
  }

  public Connection(DataEntry from, String label, DataEntry to, Map<String, String> metadata) {
    this.uuid = UUID.randomUUID().toString();
    this.fromKey = from.getKey();
    this.toKey = to.getKey();
    this.label = label;
    this.metadata = metadata;
    this.toTable = to.getTableName();
    this.fromTable = from.getTableName();
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

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getFromTable() {
    return fromTable;
  }

  public void setFromTable(String fromTable) {
    this.fromTable = fromTable;
  }

  public String getToTable() {
    return toTable;
  }

  public void setToTable(String toTable) {
    this.toTable = toTable;
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
    clonedConnection.fromTable = this.fromTable;
    clonedConnection.toKey = this.toKey;
    clonedConnection.toTable = this.toTable;
    clonedConnection.label = this.label;
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
  public DataEntry toEntry(){
    if(this.connectionHandler!=null) {
      Set<DataEntry> tmp = this.connectionHandler.getNucleoDB().getTable(this.getToTable()).get("id", this.getToKey());
      if (tmp != null) {
        Optional<DataEntry> tmpOp = tmp.stream().findFirst();
        if (tmpOp.isPresent()) {
          return tmpOp.get();
        }
      }
    }
    return null;
  }
  @JsonIgnore
  public DataEntry fromEntry(){
    if(this.connectionHandler!=null) {
      Set<DataEntry> tmp = this.connectionHandler.getNucleoDB().getTable(this.getFromTable()).get("id", this.getFromKey());
      if (tmp != null) {
        Optional<DataEntry> tmpOp = tmp.stream().findFirst();
        if (tmpOp.isPresent()) {
          return tmpOp.get();
        }
      }
    }
    return null;
  }
}
