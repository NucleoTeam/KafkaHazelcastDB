package com.nucleocore.nucleodb.database.tables;

import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.utils.DataTableConfig;
import com.nucleocore.nucleodb.database.utils.StartupRun;

import java.time.Instant;

public class DataTableBuilder{
  DataTableConfig config = new DataTableConfig();
  NucleoDB db = null;
  public static DataTableBuilder createReadOnly(String bootstrap, String table, Class clazz){
    return new DataTableBuilder(){{
      this.config.setTable(table);
      this.config.setBootstrap(bootstrap);
      this.config.setRead(true);
      this.config.setWrite(false);
      this.config.setLoadSave(true);
      this.config.setSaveChanges(true);
      this.config.setClazz(clazz);
    }};
  }
  public static DataTableBuilder createWriteOnly(String bootstrap, String table, Class clazz){
    return new DataTableBuilder(){{
      this.config.setTable(table);
      this.config.setBootstrap(bootstrap);
      this.config.setRead(false);
      this.config.setWrite(true);
      this.config.setLoadSave(true);
      this.config.setSaveChanges(false);
      this.config.setClazz(clazz);
    }};
  }
  public static DataTableBuilder create(String bootstrap, String table, Class clazz){
    return new DataTableBuilder(){{
      this.config.setTable(table);
      this.config.setBootstrap(bootstrap);
      this.config.setClazz(clazz);
    }};
  }
  public DataTableBuilder setBootstrap(String bootstrap) {
    config.setBootstrap(bootstrap);
    return this;
  }

  public DataTableBuilder setTable(String table) {
    this.config.setTable(table);
    return this;
  }

  public DataTableBuilder setClazz(Class clazz) {
    this.config.setClazz(clazz);
    return this;
  }

  public DataTableBuilder setSaveChanges(boolean saveChanges) {
    this.setSaveChanges(saveChanges);
    return this;
  }

  public DataTableBuilder setLoadSave(boolean loadSave) {
    this.config.setLoadSave(loadSave);
    return this;
  }

  public DataTableBuilder setReadToTime(Instant readToTime) {
    this.config.setReadToTime(readToTime);
    return this;
  }

  public DataTableBuilder setRead(boolean read) {
    this.config.setRead(read);
    return this;
  }

  public DataTableBuilder setWrite(boolean write) {
    this.config.setWrite(write);
    return this;
  }

  public DataTableBuilder setTableFileName(String tableFileName) {
    this.config.setTableFileName(tableFileName);
    return this;
  }

  public DataTableBuilder setIndexes(String... indexes) {
    this.config.setIndexes(indexes);
    return this;
  }
  public DataTableBuilder setStartupRun(StartupRun startupRun) {
    this.config.setStartupRun(startupRun);
    return this;
  }
  public DataTableBuilder setReadWrite(){
    this.config.setRead(true);
    this.config.setWrite(true);
    return this;
  }

  public DataTable build(){
    DataTable table = new DataTable(this.config);
    if(db != null){
      db.getTables().put(config.getTable(), table);
    }
    return table;
  }

  public NucleoDB getDb() {
    return db;
  }

  public DataTableBuilder setDb(NucleoDB db) {
    this.db = db;
    return this;
  }
}
