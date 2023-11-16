package com.nucleocore.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.connection.Connection;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.utils.InvalidConnectionException;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class AnimeTest{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());
  static ObjectMapper om = new ObjectMapper().findAndRegisterModules();
  public static void main(String[] args) throws IOException, InterruptedException {

    NucleoDB nucleoDB = new NucleoDB("127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092", "com.nucleocore.test", NucleoDB.DBType.NO_LOCAL);
    logger.info(nucleoDB.getTables().keySet().stream().collect(Collectors.joining(", ")));
    logger.info("animes: "+nucleoDB.getTable("anime").getEntries().size());
    logger.info("users: "+nucleoDB.getTable("user").getEntries().size());
    if(nucleoDB.getTables().size()==0){
      System.exit(1);
    }
    String userName = UUID.randomUUID().toString();
    String animeName = UUID.randomUUID().toString();
    int x=0;
    while(x<100000) {
      x++;
      logger.info(om.writeValueAsString(nucleoDB.getTable("anime").getEntries().size()));
      logger.info("running "+x);
      Anime a = new Anime();
      a.setName(animeName);
      a.setOwner("firestar");

      nucleoDB.getTable("anime").saveSync(new DataEntry(a));
      nucleoDB.getTable("user").saveSync(new DataEntry(new User(userName, "me")));

      Set<DataEntry> anime = nucleoDB.getTable("anime").get("name", animeName);
      Set<DataEntry> user = nucleoDB.getTable("user").get("name", userName);

      if (anime.size() > 0) {
        logger.info("do not expect, anime from connection "+om.writeValueAsString(nucleoDB.getConnectionHandler().getByFromAndLabel(anime.stream().findFirst().get(), "ADMIN_USER")));
      } else {
        logger.info("ERROR");
        System.exit(1);
        return;
      }
      if (user.size() > 0) {
        try {
          if (anime.size() > 0) {
            nucleoDB.getConnectionHandler().saveSync(new Connection(user.stream().findFirst().get(), "WATCHING", anime.stream().findFirst().get(), new TreeMap<>(){{
              put("time", "2.0402042");
            }}));
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (InvalidConnectionException e) {
          throw new RuntimeException(e);
        }

        Set<Connection> connectionOptional = nucleoDB.getConnectionHandler().getByFromAndLabel(user.stream().findFirst().get(), "WATCHING");
        if (connectionOptional.size() > 0) {
          nucleoDB.getConnectionHandler().deleteSync(connectionOptional.stream().findFirst().get());
        }
        connectionOptional = nucleoDB.getConnectionHandler().getByFromAndLabel(user.stream().findFirst().get(), "WATCHING");
        if (connectionOptional.size() > 0) {
          logger.info("connection failed to delete.");
          logger.info("expect connection"+om.writeValueAsString(connectionOptional.stream().findFirst().get()));
        } else {
          logger.info("connection not found, successfully deleted connection.");
        }
        anime.stream().collect(Collectors.toSet()).stream().forEach(an -> {
          try {
            nucleoDB.getTable("anime").deleteSync(an);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
        user.stream().collect(Collectors.toSet()).stream().forEach(us -> {
          try {
            nucleoDB.getTable("user").deleteSync(us);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
      }else{
        logger.info("ERROR");
        System.exit(1);
      }
      Thread.sleep(2);
    }

//    Set<DataEntry> entries = nucleoDB.getTable("anime").get("name", "Zoku Owarimonogatari");
//    nucleoDB.getConnectionHandler().save(new Connection());
//    logger.info(entries);
//
//    entries.retainAll(nucleoDB.getRelated(new DataEntry(), Anime.class));
//    logger.info(entries);
    //});
  }
}
