package com.nucleocore.test.common;

import com.nucleocore.library.database.modifications.ConnectionCreate;
import com.nucleocore.library.database.tables.annotation.Conn;
import com.nucleocore.library.database.tables.connection.Connection;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.test.common.Anime;
import com.nucleocore.test.common.AnimeDE;
import com.nucleocore.test.common.User;

import java.util.Map;

@Conn(name = "WATCHING", to = Anime.class, from = User.class)
public class WatchingConnection<AnimeDE, UserDE> extends Connection{
  public WatchingConnection() {
  }

  public WatchingConnection(DataEntry from, DataEntry to) {
    super(from, to);
  }

  public WatchingConnection(ConnectionCreate connectionCreate) {
    super(connectionCreate);
  }

  public WatchingConnection(DataEntry from, DataEntry to, Map metadata) {
    super(from, to, metadata);
  }

}
