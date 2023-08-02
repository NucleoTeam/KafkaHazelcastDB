package com.nucleocore.nucleodb.sqltest;

import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.utils.SQLHandler;
import com.nucleocore.nucleodb.database.utils.StartupRun;
import com.nucleocore.nucleodb.usertest.User;
import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;
import java.util.Arrays;

public class SqlSample{
  public static void main(String[] args) {
    NucleoDB db = new NucleoDB();
    db.launchNucleoTable(null, "anime", Anime.class, new StartupRun(){
      public void run() {
        System.out.println("STARTUP COMPLETE");
      }
    }, "name", "tags", "actors.name");
    try {
      //db.getTable("anime").setSave(false);

      //populateDB(db);

      db.sql("SELECT * FROM anime WHERE actors.name='Maaya Sakamoto' LIMIT 20;", AnimeDTO.class);
      db.sql("SELECT * FROM anime WHERE actors.name='Megumi Toyoguchi' and tags in ('Fantasy') LIMIT 20;", AnimeDTO.class);
      db.sql("SELECT * FROM anime WHERE actors.name='Megumi Toyoguchi' and tags='Action' or tags='Fantasy' LIMIT 20;", AnimeDTO.class);
    } catch (JSQLParserException e) {
      throw new RuntimeException(e);
    }
  }

  static void populateDB(NucleoDB db) {
    db.getTable("anime").insert(new Anime("Bleach", Arrays.asList("Adventure", "Action", "Fantasy")));
    db.getTable("anime").insert(new Anime("Full Metal Panic? Fumoffu", Arrays.asList("Comedy", "Action")));
    db.getTable("anime").insert(new Anime(
        "Ouran Koukou Host Club",
        Arrays.asList("Comedy", "Romance"),
        Arrays.asList(new VoiceActor("Maaya Sakamoto", "Haruhi Fujioka"))
    ));
    db.getTable("anime").insert(new Anime(
        "Arakawa Under the Bridge x Bridge",
        Arrays.asList("Comedy", "Romance"),
        Arrays.asList(new VoiceActor("Maaya Sakamoto", "Nino"))
    ));
    db.getTable("anime").insert(new Anime(
        ".hack//Sign",
        Arrays.asList("Adventure,Fantasy,Mystery".split(",")),
        Arrays.asList(
            new VoiceActor("Maaya Sakamoto", "Aura"),
            new VoiceActor("Kaori Nazuka", "Subaru"),
            new VoiceActor("Megumi Toyoguchi", "Mimiru")
        )
    ));
    db.getTable("anime").insert(new Anime(
        "Black Lagoon",
        Arrays.asList("Action".split(",")),
        Arrays.asList(
            new VoiceActor("Megumi Toyoguchi", "Revy")
        )
    ));
  }
}
