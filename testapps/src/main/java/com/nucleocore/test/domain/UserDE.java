package com.nucleocore.test.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.tables.table.DataEntry;

public class UserDE extends DataEntry<User>{
  public UserDE(User obj) {
    super(obj);
  }

  public UserDE(Create create) throws ClassNotFoundException, JsonProcessingException {
    super(create);
  }

  public UserDE() {
  }

  public UserDE(String key) {
    super(key);
  }
}
