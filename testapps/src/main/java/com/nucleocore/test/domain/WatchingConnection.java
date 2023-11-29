package com.nucleocore.test.domain;

import com.nucleocore.library.database.tables.annotation.Conn;
import com.nucleocore.library.database.tables.connection.Connection;

import java.util.Map;

@Conn("WATCHING")
public class WatchingConnection extends Connection<AnimeDE, UserDE>{
  public WatchingConnection() {
  }

  public WatchingConnection(UserDE from, AnimeDE to) {
    super(from, to);
  }

  public WatchingConnection(UserDE from, AnimeDE to, Map<String, String> metadata) {
    super(from, to, metadata);
  }
}
