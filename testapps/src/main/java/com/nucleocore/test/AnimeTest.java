package com.nucleocore.test;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.test.sql.Anime;
import net.sf.jsqlparser.JSQLParserException;

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
    Set<DataEntry> entries = nucleoDB.getTable("anime").get("name", "Zoku Owarimonogatari");
    Serializer.log(entries);
    entries.retainAll(nucleoDB.getRelatedRemote(new DataEntry(new User("dave", "me")), Anime.class, "owner"));
    Serializer.log(entries);
    //});
  }
}
