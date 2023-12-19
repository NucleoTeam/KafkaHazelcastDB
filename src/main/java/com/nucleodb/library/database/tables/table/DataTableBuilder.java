package com.nucleodb.library.database.tables.table;

import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.utils.StartupRun;
import com.nucleodb.library.mqs.config.MQSConfiguration;
import org.jetbrains.annotations.NotNull;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Set;

public class DataTableBuilder implements Comparable{
  private static final long serialVersionUID = 1;
  DataTableConfig config = new DataTableConfig();
  NucleoDB db = null;
  public static DataTableBuilder createReadOnly(String table, Class clazz){
    return new DataTableBuilder(){{
      this.config.setTable(table);
      this.config.setRead(true);
      this.config.setWrite(false);
      this.config.setLoadSave(true);
      this.config.setSaveChanges(true);
      this.config.setClazz(clazz);
    }};
  }
  public static DataTableBuilder createWriteOnly(String table, Class clazz){
    return new DataTableBuilder(){{
      this.config.setTable(table);
      this.config.setRead(false);
      this.config.setWrite(true);
      this.config.setLoadSave(true);
      this.config.setSaveChanges(false);
      this.config.setClazz(clazz);
    }};
  }
  public static DataTableBuilder create(String table, Class clazz){
    return new DataTableBuilder(){{
      this.config.setTable(table);
      this.config.setClazz(clazz);
    }};
  }
  public DataTableBuilder setBootstrap() {
    return this;
  }

  public DataTableBuilder setTable(String table) {
    this.config.setTable(table);
    return this;
  }

  public DataTableBuilder putSetting(String key, Object value){
    this.config.getSettingsMap().put(key, value);
    return this;
  }

  public DataTableBuilder setMQSConfiguration(Class<? extends MQSConfiguration> clazz){
    try {
      this.config.setMqsConfiguration(clazz.getDeclaredConstructor().newInstance());
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
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

  public DataTableBuilder addIndexes(Set<DataTableConfig.IndexConfig> indexes) {
    this.config.getIndexes().addAll(indexes);
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

  public DataTable build() throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
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
