package com.nucleocore.library.database.tables.table;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.StartupRun;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

public class DataTableBuilder implements Comparable{
  private static final long serialVersionUID = 1;
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

  public DataTableBuilder setJSONExport(boolean jsonExport){
    this.config.setJsonExport(jsonExport);
    return this;
  }

  public DataTableBuilder setClazz(Class clazz) {
    this.config.setClazz(clazz);
    return this;
  }

  public DataTableBuilder setSaveChanges(boolean saveChanges) {
    this.config.setSaveChanges(saveChanges);
    return this;
  }

  public DataTableBuilder setLoadSave(boolean loadSave) {
    this.config.setLoadSave(loadSave);
    return this;
  }

  public DataTableBuilder setDataEntryClass(Class dataEntryClass) {
    this.config.setDataEntryClass(dataEntryClass);
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
    int inLen = indexes.length;
    int oldLen = this.config.getIndexes().length;
    String[] newIndexArray = new String[inLen+oldLen];

    for (int i = 0; i < inLen; i++) {
      newIndexArray[i] = indexes[i];
    }
    for (int i = 0; i < oldLen; i++) {
      newIndexArray[inLen+i] = this.config.getIndexes()[i];
    }
    this.config.setIndexes(newIndexArray);
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

  public DataTableConfig getConfig() {
    return config;
  }

  @Override
  public int compareTo(@NotNull Object o) {
    return this.getConfig().getTable().compareTo(((DataTableBuilder) o).getConfig().getTable());
  }
}
