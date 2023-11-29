package com.nucleodb.library.examples.anime.connections;

import com.nucleodb.library.database.tables.annotation.Conn;
import com.nucleodb.library.database.tables.connection.Connection;
import com.nucleodb.library.database.utils.SkipCopy;
import com.nucleodb.library.examples.anime.definitions.AnimeDE;
import com.nucleodb.library.examples.anime.definitions.UserDE;

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
