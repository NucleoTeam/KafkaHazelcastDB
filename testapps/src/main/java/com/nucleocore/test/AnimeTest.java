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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class AnimeTest{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());
  static ObjectMapper om = new ObjectMapper().findAndRegisterModules();
  public static void main(String[] args) throws IOException, InterruptedException {
    NucleoDB nucleoDB = new NucleoDB("127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092", "com.nucleocore.test", NucleoDB.DBType.NO_LOCAL);
    logger.info(nucleoDB.getTables().keySet().stream().collect(Collectors.joining(", ")));
    DataTable userTable = nucleoDB.getTable(User.class);
    DataTable animeTable = nucleoDB.getTable(Anime.class);
    logger.info("animes: "+animeTable.getEntries().size());
    logger.info("users: "+userTable.getEntries().size());
    if(nucleoDB.getTables().size()==0){
      System.exit(1);
    }
    String userName = UUID.randomUUID().toString();
    String animeName = UUID.randomUUID().toString();
    int x=0;
    while(x<100000) {
      x++;
      logger.info(om.writeValueAsString(animeTable.getEntries().size()));
      logger.info("running "+x);

      Anime a = new Anime();
      a.setName(animeName);
      a.setOwner("firestar");

      AtomicReference<DataEntry> animeReference = new AtomicReference<>();
      animeTable.saveAsync(a, dataEntry -> {
        animeReference.set(dataEntry);
        synchronized (animeReference) {
          animeReference.notify();
        }
      });
      try {
        synchronized (animeReference) {
          animeReference.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      AtomicReference<DataEntry> userReference = new AtomicReference<>();
      userTable.saveAsync(new User(userName, "me"), (dataEntry -> {
        userReference.set(dataEntry);
        synchronized (userReference) {
          userReference.notify();
        }
      }));
      try {
        synchronized (userReference) {
          userReference.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      if (animeReference.get()!=null) {
        logger.info("do not expect, anime from connection "+om.writeValueAsString(nucleoDB.getConnectionHandler().getByFromAndLabel(animeReference.get(), "ADMIN_USER")));
      } else {
        logger.info("ERROR");
        System.exit(1);
        return;
      }
      if (userReference.get()!=null) {
        try {
          if (animeReference.get()!=null) {
            nucleoDB.getConnectionHandler().saveSync(new Connection(userReference.get(), "WATCHING", animeReference.get(), new TreeMap<>(){{
              put("time", "2.0402042");
            }}));
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (InvalidConnectionException e) {
          e.printStackTrace();
        }

        Set<Connection> connectionOptional = nucleoDB.getConnectionHandler().getByFromAndLabel(userReference.get(), "WATCHING");
        if (connectionOptional.size() > 0) {
          nucleoDB.getConnectionHandler().deleteSync(connectionOptional.stream().findFirst().get());
        }
        connectionOptional = nucleoDB.getConnectionHandler().getByFromAndLabel(userReference.get(), "WATCHING");
        if (connectionOptional.size() > 0) {
          logger.info("connection failed to delete.");
          logger.info("expect connection"+om.writeValueAsString(connectionOptional.stream().findFirst().get()));
        } else {
          logger.info("connection not found, successfully deleted connection.");
        }
        try {
          animeTable.deleteSync(animeReference.get());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        try {
          userTable.deleteSync(userReference.get());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }else{
        logger.info("ERROR");
        System.exit(1);
      }
      Thread.sleep(1000);
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
