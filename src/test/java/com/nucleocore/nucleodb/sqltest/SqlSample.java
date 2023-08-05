package com.nucleocore.nucleodb.sqltest;

import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.utils.Serializer;
import com.nucleocore.nucleodb.database.utils.StartupRun;
import net.sf.jsqlparser.JSQLParserException;

import java.util.Arrays;
import java.util.Set;

public class SqlSample{
  public static void main(String[] args) {
    NucleoDB db = new NucleoDB();
    db.launchNucleoTable(null, "anime", Anime.class, new StartupRun(){
      public void run() {
        System.out.println("STARTUP COMPLETE");
      }
    }, "name", "tags", "actors.name", "votes", "actors.tags");
    try {
      //db.getTable("anime").setSave(false);

      //populateDB(db);
      Set<AnimeDTO> response = db.select("SELECT * FROM anime WHERE actors.name='Maaya Sakamoto' LIMIT 1;", AnimeDTO.class);
      Serializer.log(response);
      response = db.select("SELECT * FROM anime WHERE actors.name='Megumi Toyoguchi' and tags in ('Fantasy') LIMIT 1;", AnimeDTO.class);
      Serializer.log(response);
      response = db.select("SELECT * FROM anime WHERE actors.name='Megumi Toyoguchi' and (tags='Action' or tags='Fantasy') LIMIT 1;", AnimeDTO.class);
      Serializer.log(response);
      response = db.select("SELECT * FROM anime WHERE votes in (5.5) LIMIT 1;", AnimeDTO.class);
      Serializer.log(response);
      //DataEntry de = db.insert("INSERT INTO anime SET name='Woot', rating=2.5, votes=(1.2,5.5,2.4,3.4), actors=((name='Megumi Toyoguchi', character='Witch', tags=('works?')), (name='Maaya Sakamoto')), tags=('Action','Fantasy')");
      //Serializer.log(de);
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
