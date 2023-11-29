package com.nucleocore.library.database.tables.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nucleocore.library.database.utils.StartupRun;

import java.io.Serializable;
import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;

public class DataTableConfig implements Serializable{
  private static final long serialVersionUID = 4416983891804575837L;

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
  Set<String> indexes = new TreeSet<>();
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

  public Set<String> getIndexes() {
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

  public void setIndexes(Set<String> indexes) {
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
