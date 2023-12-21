package com.nucleodb.library.event;

import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Update;
import com.nucleodb.library.database.tables.table.DataEntry;

public abstract class DataTableEventListener<T> {
  public void update(Update update, T entry){
  }
  public void delete(Delete delete, T entry){
  }
  public void create(Create create, T entry){
  }
}
