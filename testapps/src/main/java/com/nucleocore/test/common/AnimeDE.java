package com.nucleocore.test.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.tables.table.DataEntry;

public class AnimeDE extends DataEntry{
  public AnimeDE(Anime obj) {
    super(obj);
  }

  public AnimeDE(Create create) throws ClassNotFoundException, JsonProcessingException {
    super(create);
  }

  public AnimeDE() {
  }

  public AnimeDE(String key) {
    super(key);
  }

  @Override
  public Anime getData() {
    return (Anime) data;
  }

  public void setData(Anime data) {
    this.data = data;
  }
}
