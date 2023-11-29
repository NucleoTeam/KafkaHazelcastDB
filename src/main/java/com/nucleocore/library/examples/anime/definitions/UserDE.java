package com.nucleocore.library.examples.anime.definitions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.examples.anime.tables.User;

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
