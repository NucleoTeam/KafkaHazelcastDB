package com.nucleocore.library.database.tables.connection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.database.modifications.ConnectionCreate;
import com.nucleocore.library.database.tables.annotation.Conn;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.utils.SkipCopy;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;


@Conn(name = "connection", to = DataEntry.class, from = DataEntry.class)
public class Connection<T extends DataEntry, F extends DataEntry> implements Serializable, Comparable<Connection>{
  @SkipCopy
  private static final long serialVersionUID = 1;

  private String uuid;
  private String fromKey;
  private String toKey;
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

  public Connection(DataEntry from, DataEntry to) {
    this.uuid = UUID.randomUUID().toString();
    this.fromKey = from.getKey();
    this.toKey = to.getKey();
    this.date = Instant.now();
    this.modified = Instant.now();
  }

  public Connection(ConnectionCreate connectionCreate) {
    this.uuid = connectionCreate.getUuid();
    this.fromKey = connectionCreate.getFromKey();
    this.toKey = connectionCreate.getToKey();;
    this.date = connectionCreate.getDate();
    this.version = connectionCreate.getVersion();
    this.metadata = new TreeMap<>(connectionCreate.getMetadata());

  }

  public <T> T copy(Class<T> clazz)  {
    ObjectMapper om = new ObjectMapper()
        .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
        .findAndRegisterModules();
    try {
      T obj =  om.readValue(om.writeValueAsString(this), clazz);
      ((Connection)obj).connectionHandler = this.connectionHandler;
      return obj;
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
  }

  public Connection(DataEntry from, DataEntry to, Map<String, String> metadata) {
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
        Optional<DataEntry> tmpOp = tmp.stream().findFirst().map(c->c.copy(table.getConfig().getDataEntryClass())).map(DataEntry.class::cast);
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
        Optional<DataEntry> tmpOp = tmp.stream().findFirst().map(c->c.copy(table.getConfig().getDataEntryClass())).map(DataEntry.class::cast);
        if (tmpOp.isPresent()) {
          return (F)tmpOp.get();
        }
      }
    }
    return null;
  }
}
