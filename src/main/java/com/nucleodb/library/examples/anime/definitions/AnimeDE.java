package com.nucleodb.library.examples.anime.definitions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.examples.anime.tables.Anime;

public class AnimeDE extends DataEntry<Anime>{
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
