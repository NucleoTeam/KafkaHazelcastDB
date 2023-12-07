package com.nucleodb.library.database.tables.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nucleodb.library.database.index.IndexWrapper;
import com.nucleodb.library.database.utils.StartupRun;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class DataTableConfig implements Serializable{
  private static final long serialVersionUID = 4416983891804575837L;

  public static class IndexConfig implements Comparable {
    String name;
    Class<? extends IndexWrapper> indexType;

    public IndexConfig(String name, Class<? extends IndexWrapper> indexType) {
      this.name = name;
      this.indexType = indexType;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Class<? extends IndexWrapper> getIndexType() {
      return indexType;
    }

    public void setIndexType(Class<? extends IndexWrapper> indexType) {
      this.indexType = indexType;
    }

    @Override
    public int compareTo(@NotNull Object o) {
      if(o instanceof IndexConfig){
        return this.getName().compareTo(((IndexConfig) o).getName());
      }
      return 0;
    }
  }
  String bootstrap = "127.0.0.1:19092";
  String table;
  @JsonIgnore
  transient Class clazz;
  boolean saveChanges = true;
  boolean loadSave = true;
  boolean jsonExport = false;
  Instant readToTime = null;
  Class dataEntryClass;
  boolean read = true;
  boolean write = true;
  List<IndexConfig> indexes = new LinkedList<>();
  @JsonIgnore
  private transient StartupRun startupRun = null;


  String tableFileName;
  public DataTableConfig() {
  }

  public String getBootstrap() {
    return bootstrap;
  }

  public String getTable() {

    return table;
  }

  public Class getClazz() {
    return clazz;
  }

  public boolean isSaveChanges() {
    return saveChanges;
  }

  public boolean isLoadSave() {
    return loadSave;
  }

  public Instant getReadToTime() {
    return readToTime;
  }

  public StartupRun getStartupRun() {
    return startupRun;
  }

  public boolean isRead() {
    return read;
  }

  public boolean isWrite() {
    return write;
  }

  public List<IndexConfig> getIndexes() {
    return indexes;
  }

  public void setBootstrap(String bootstrap) {
    this.bootstrap = bootstrap;
  }

  public void setTable(String table) {
    this.tableFileName = "./data/" + table + ".dat";
    this.table = table;
  }

  public void setTableFileName(String tableFileName) {
    this.tableFileName = tableFileName;
  }

  public void setClazz(Class clazz) {
    this.clazz = clazz;
  }

  public void setSaveChanges(boolean saveChanges) {
    this.saveChanges = saveChanges;
  }

  public void setLoadSave(boolean loadSave) {
    this.loadSave = loadSave;
  }

  public void setReadToTime(Instant readToTime) {
    this.readToTime = readToTime;
  }

  public void setRead(boolean read) {
    this.read = read;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

  public void setIndexes(List<IndexConfig> indexes) {
    this.indexes = indexes;
  }

  public void setStartupRun(StartupRun startupRun) {
    this.startupRun = startupRun;
  }

  public void merge(DataTableConfig config) {
    this.indexes = config.indexes;
  }
  public String getTableFileName() {
    return tableFileName;
  }

  public boolean isJsonExport() {
    return jsonExport;
  }

  public void setJsonExport(boolean jsonExport) {
    this.jsonExport = jsonExport;
  }

  public Class getDataEntryClass() {
    return dataEntryClass;
  }

  public void setDataEntryClass(Class dataEntryClass) {
    this.dataEntryClass = dataEntryClass;
  }
}
