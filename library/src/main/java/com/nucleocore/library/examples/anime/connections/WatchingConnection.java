package com.nucleocore.library.examples.anime.connections;

import com.nucleocore.library.database.modifications.ConnectionCreate;
import com.nucleocore.library.database.tables.annotation.Conn;
import com.nucleocore.library.database.tables.connection.Connection;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.utils.SkipCopy;
import com.nucleocore.library.examples.anime.definitions.AnimeDE;
import com.nucleocore.library.examples.anime.definitions.UserDE;
import com.nucleocore.library.examples.anime.tables.Anime;
import com.nucleocore.library.examples.anime.tables.User;

import java.util.Map;

@Conn("WATCHING")
public class WatchingConnection extends Connection<AnimeDE, UserDE>{
  @SkipCopy
  private static final long serialVersionUID = 1;
  public WatchingConnection() {
  }

  public WatchingConnection(UserDE from, AnimeDE to) {
    super(from, to);
  }

  public WatchingConnection(UserDE from, AnimeDE to, Map<String, String> metadata) {
    super(from, to, metadata);
  }

  private float time = 0f;

  public float getTime() {
    return time;
  }

  public void setTime(float time) {
    this.time = time;
  }
}
