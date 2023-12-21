package com.nucleodb.library.event;

import com.nucleodb.library.database.modifications.ConnectionCreate;
import com.nucleodb.library.database.modifications.ConnectionDelete;
import com.nucleodb.library.database.modifications.ConnectionUpdate;

public abstract class ConnectionEventListener<T> {
  public void update(ConnectionUpdate update, T entry){
  }
  public void delete(ConnectionDelete delete, T entry){
  }
  public void create(ConnectionCreate create, T entry){
  }
}