package com.nucleocore.nucleodb.database.tables;

import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.Modification;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class H2Table implements TableTemplate{
  private String tableName;
  private Set<String> hashes = new HashSet<>();
  public H2Table(String tableName) {

  }

  @Override
  public void modify(Modification mod, Object modification) {

  }

  @Override
  public boolean save(DataEntry oldEntry, DataEntry newEntry, Consumer<DataEntry> consumer) {
    return false;
  }

  @Override
  public boolean save(DataEntry oldEntry, DataEntry newEntry) {
    return false;
  }

  @Override
  public void multiImport(DataEntry newEntry) {

  }

  @Override
  public int getSize() {
    return 0;
  }

  @Override
  public void startup() {

  }

  @Override
  public void setBuildIndex(boolean buildIndex) {

  }

  @Override
  public void setUnsavedIndexModifications(boolean unsavedIndexModifications) {

  }

  @Override
  public void consume() {

  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
