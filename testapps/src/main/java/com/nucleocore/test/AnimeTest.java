package com.nucleocore.test;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.Connection;
import com.nucleocore.library.database.tables.ConnectionHandler;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.test.sql.Anime;
import net.sf.jsqlparser.JSQLParserException;

import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public class AnimeTest{
  public static void main(String[] args) {
    NucleoDB nucleoDB = new NucleoDB("127.0.0.1:29092", "com.nucleocore.test");
    //Serializer.log(nucleoDB.getTables().keySet());

//    try {
//      nucleoDB.sql("INSERT INTO anime SET owner='dave', name='Zoku Owarimonogatari'");
//    } catch (JSQLParserException e) {
//      throw new RuntimeException(e);
//    }
//    Anime a = new Anime();
//    a.setName("Kizumonogatari: Koyomi Vamp");
//    a.setOwner("firestar");
//    nucleoDB.getTable("anime").insert(a, (dataEntry)->{
    //nucleoDB.getTable("user").insert(new User("dave", "me"));
    Optional<DataEntry> anime = nucleoDB.getTable("anime").get("name", "Zoku Owarimonogatari").stream().findFirst();
    Optional<DataEntry> user =  nucleoDB.getTable("user").get("name", "dave").stream().findFirst();
    Serializer.log(nucleoDB.getConnectionHandler().getByLabel(anime.get(), "ADMIN_USER"));
//    try {
//      nucleoDB.getConnectionHandler().saveSync(new Connection(user.get(), "WATCHING", anime.get(), new TreeMap<>(){{
//        put("time", "2.0402042");
//      }}));
//    } catch (InterruptedException e) {
//      throw new RuntimeException(e);
//    }
    Serializer.log(nucleoDB.getConnectionHandler().getByLabelStream(user.get(), "WATCHING").findFirst().get());

//    Set<DataEntry> entries = nucleoDB.getTable("anime").get("name", "Zoku Owarimonogatari");
//    nucleoDB.getConnectionHandler().save(new Connection());
//    Serializer.log(entries);
//
//    entries.retainAll(nucleoDB.getRelated(new DataEntry(), Anime.class));
//    Serializer.log(entries);
    //});
  }
}
