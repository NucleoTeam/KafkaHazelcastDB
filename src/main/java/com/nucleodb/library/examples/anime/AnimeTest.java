package com.nucleodb.library.examples.anime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.tables.connection.Connection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.InvalidConnectionException;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.examples.anime.connections.WatchingConnection;
import com.nucleodb.library.examples.anime.definitions.UserDE;
import com.nucleodb.library.examples.anime.definitions.AnimeDE;
import com.nucleodb.library.examples.anime.tables.Anime;
import com.nucleodb.library.examples.anime.tables.User;
import com.nucleodb.library.examples.anime.tables.nested.VoiceActor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class AnimeTest{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());
  static ObjectMapper om = new ObjectMapper().findAndRegisterModules();
  public static void main(String[] args) throws IOException, InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException, IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
    NucleoDB nucleoDB = new NucleoDB(
        "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        NucleoDB.DBType.ALL,
        "com.nucleocore.library.negotiator.examples.anime",
        "com.nucleocore.library.examples.anime.tables",
        "com.nucleocore.library.examples.anime.connections"
    );
    logger.info(String.format("indexes: %s",Serializer.getObjectMapper().getOm().writeValueAsString(nucleoDB.getTable(Anime.class).getIndexes())));
    logger.info(String.format("tables: %s", nucleoDB.getTables().keySet().stream().collect(Collectors.joining(", "))));
    logger.info(String.format("connections: %s", nucleoDB.getConnections().keySet().stream().collect(Collectors.joining(", "))));
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
      a.getActors().add(new VoiceActor("Maaya Sakamoto"));
      a.setOwner("firestar");

      AtomicReference<AnimeDE> animeReference = new AtomicReference<>();
      animeTable.saveAsync(a, dataEntry -> {
        if(dataEntry instanceof AnimeDE) {
          animeReference.set((AnimeDE)dataEntry);
          synchronized (animeReference) {
            animeReference.notify();
          }
        }
      });
      try {
        synchronized (animeReference) {
          animeReference.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      //logger.info(String.format("indexes: %s",Serializer.getObjectMapper().getOm().writeValueAsString(nucleoDB.getTable(Anime.class).getIndexes())));


      AtomicReference<UserDE> userReference = new AtomicReference<>();
      userTable.saveAsync(new User(userName, "me"), (dataEntry -> {
        userReference.set((UserDE) dataEntry);
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
      try {
        String user = ((UserDE) userTable.getEntries().stream().findFirst().get()).getData().getUser();
      }catch (Exception e){
        e.printStackTrace();
      }
      logger.info("returned anime class "+nucleoDB
          .getTable(Anime.class)
          .get("name", animeName, null)
          .stream().map(AnimeDE.class::cast).findFirst().get().getData().getName());

      if (userReference.get()!=null) {
        try {
          if (animeReference.get()!=null) {
            nucleoDB.getConnectionHandler(WatchingConnection.class).saveSync(new WatchingConnection(userReference.get(), animeReference.get(), new TreeMap<>(){{
              put("time", "2.0402042");
            }}));
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (InvalidConnectionException e) {
          e.printStackTrace();
        }

        Set<Connection> connectionOptional = nucleoDB.getConnectionHandler(WatchingConnection.class).getByFrom(userReference.get(), null);
        if (connectionOptional.size() > 0) {
          WatchingConnection connection = (WatchingConnection)connectionOptional.stream().findFirst().get();
          logger.info("connection found type is "+connection.getClass().getName());
          logger.info("anime name is "+connection.toEntry().getData().getName());
          connection.setTime(555.222f);
          try {
            nucleoDB.getConnectionHandler(WatchingConnection.class).saveSync(connection);
          } catch (InvalidConnectionException e) {
            throw new RuntimeException(e);
          }
          nucleoDB.getConnectionHandler(WatchingConnection.class).deleteSync(connectionOptional.stream().findFirst().get());
        }

        connectionOptional = nucleoDB.getConnectionHandler(WatchingConnection.class).getByFrom(userReference.get(), null);
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
