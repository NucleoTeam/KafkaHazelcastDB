package com.nucleocore.test;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.Connection;
import com.nucleocore.library.database.tables.ConnectionHandler;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.test.sql.Anime;
import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public class AnimeTest{
  public static void main(String[] args) throws IOException, InterruptedException {
    NucleoDB nucleoDB = new NucleoDB("127.0.0.1:29092", "com.nucleocore.test");
    Serializer.log(nucleoDB.getTables().keySet());

//    Anime a = new Anime();
//    a.setName("Kizumonogatari: Koyomi Vamp");
//    a.setOwner("firestar");
//
//    nucleoDB.getTable("anime").insertDataEntrySync(new DataEntry(a));
//    nucleoDB.getTable("user").insertDataEntrySync(new DataEntry(new User("dave", "me")));

    Optional<DataEntry> anime = nucleoDB.getTable("anime").get("name", "Kizumonogatari: Koyomi Vamp").stream().findFirst();
    Optional<DataEntry> user =  nucleoDB.getTable("user").get("name", "dave").stream().findFirst();
    if(anime.isPresent()) {
      Serializer.log(nucleoDB.getConnectionHandler().getByLabel(anime.get(), "ADMIN_USER"));
    }
    if(user.isPresent()) {
      try {
        if(anime.isPresent()) {
          nucleoDB.getConnectionHandler().saveSync(new Connection(user.get(), "WATCHING", anime.get(), new TreeMap<>(){{
            put("time", "2.0402042");
          }}));
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      Serializer.log(nucleoDB.getConnectionHandler().getByLabelStream(user.get(), "WATCHING").findFirst().get());
      Optional<Connection> connectionOptional = nucleoDB.getConnectionHandler().getByLabelStream(user.get(), "WATCHING").findFirst();
      if(connectionOptional.isPresent()) {
        nucleoDB.getConnectionHandler().deleteSync(connectionOptional.get());
      }
      connectionOptional = nucleoDB.getConnectionHandler().getByLabelStream(user.get(), "WATCHING").findFirst();
      if(connectionOptional.isPresent()) {
        Serializer.log("connection failed to delete.");
        Serializer.log(connectionOptional.get());
      }else{
        Serializer.log("connection not found, successfully deleted connection.");
      }
    }

//    Set<DataEntry> entries = nucleoDB.getTable("anime").get("name", "Zoku Owarimonogatari");
//    nucleoDB.getConnectionHandler().save(new Connection());
//    Serializer.log(entries);
//
//    entries.retainAll(nucleoDB.getRelated(new DataEntry(), Anime.class));
//    Serializer.log(entries);
    //});
  }
}
