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
    //Optional<DataEntry> user =  nucleoDB.getTable("user").get("name", "dave").stream().findFirst();
    Serializer.log(nucleoDB.getConnectionHandler().getByLabel(anime.get(), "ADMIN_USER"));
//    nucleoDB.getConnectionHandler().save(new Connection(anime.get(), "ADMIN_USER", user.get()), (c)->{
//      Serializer.log(c);
//      Serializer.log(nucleoDB.getConnectionHandler().get(anime.get()));
//    });

//    Set<DataEntry> entries = nucleoDB.getTable("anime").get("name", "Zoku Owarimonogatari");
//    nucleoDB.getConnectionHandler().save(new Connection());
//    Serializer.log(entries);
//
//    entries.retainAll(nucleoDB.getRelated(new DataEntry(), Anime.class));
//    Serializer.log(entries);
    //});
  }
}
