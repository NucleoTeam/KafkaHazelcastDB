package com.nucleocore.library.database.tables.connection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nucleocore.library.database.utils.StartupRun;

import java.io.Serializable;
import java.time.Instant;

public class ConnectionConfig implements Serializable{
  private static final long serialVersionUID = 1;
  Instant readToTime = null;
  boolean write = true;
  boolean read = true;
  boolean loadSaved = true;
  boolean jsonExport = false;
  boolean saveChanges = true;
  String topic;
  String label;
  String bootstrap = "127.0.0.1:19092";

  Class connectionClass;
  Class toTable;
  Class fromTable;
  @JsonIgnore
  private transient StartupRun startupRun = null;

  public ConnectionConfig() {
  }

  public Instant getReadToTime() {
    return readToTime;
  }

  public void setReadToTime(Instant readToTime) {
    this.readToTime = readToTime;
  }

  public boolean isWrite() {
    return write;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

  public String getBootstrap() {
    return bootstrap;
  }

  public void setBootstrap(String bootstrap) {
    this.bootstrap = bootstrap;
  }

  public StartupRun getStartupRun() {
    return startupRun;
  }

  public void setStartupRun(StartupRun startupRun) {
    this.startupRun = startupRun;
  }

  public boolean isRead() {
    return read;
  }

  public void setRead(boolean read) {
    this.read = read;
  }

  public boolean isSaveChanges() {
    return saveChanges;
  }

  public void setSaveChanges(boolean saveChanges) {
    this.saveChanges = saveChanges;
  }

  public boolean isLoadSaved() {
    return loadSaved;
  }

  public void setLoadSaved(boolean loadSaved) {
    this.loadSaved = loadSaved;
  }

  public boolean isJsonExport() {
    return jsonExport;
  }

  public void setJsonExport(boolean jsonExport) {
    this.jsonExport = jsonExport;
  }

  public Class getConnectionClass() {
    return connectionClass;
  }

  public void setConnectionClass(Class connectionClass) {
    this.connectionClass = connectionClass;
  }

  public Class getToTable() {
    return toTable;
  }

  public void setToTable(Class toTable) {
    this.toTable = toTable;
  }

  public Class getFromTable() {
    return fromTable;
  }

  public void setFromTable(Class fromTable) {
    this.fromTable = fromTable;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }
}
