package com.nucleocore.nucleodb.sqltest;

import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.utils.Serializer;
import com.nucleocore.nucleodb.database.utils.StartupRun;
import net.sf.jsqlparser.JSQLParserException;

import java.io.Serial;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class SqlSample{
  public static void main(String[] args) {
    NucleoDB db = new NucleoDB();
    db.launchNucleoTable(null, "anime", Anime.class, new StartupRun(){
      public void run() {
        System.out.println("STARTUP COMPLETE");
        try {
          //db.getTable("anime").setSave(false);

            long time = System.currentTimeMillis();
            List<AnimeDTO> response = db.select("SELECT * FROM anime WHERE actors.name='Maaya Sakamoto' LIMIT 1;", AnimeDTO.class);
            Serializer.log(response);
            Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
            time = System.currentTimeMillis();
            response = db.select("SELECT * FROM anime WHERE tags in ('Comedy') ORDER BY name ASC, rating DESC LIMIT 0, 3", AnimeDTO.class);
            Serializer.log(response);
            Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
            time = System.currentTimeMillis();
            response = db.select("SELECT * FROM anime WHERE name='Bleach' ORDER BY name ASC, rating DESC LIMIT 1", AnimeDTO.class);
            Serializer.log(response);
            Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
            time = System.currentTimeMillis();
            response = db.select("SELECT * FROM anime WHERE name='Bleach' ORDER BY name ASC, rating DESC LIMIT 1, 1", AnimeDTO.class);
            Serializer.log(response);
            Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
            time = System.currentTimeMillis();
            response = db.select("SELECT * FROM anime WHERE actors.name='Megumi Toyoguchi' and (tags='Action' or tags='Fantasy') LIMIT 1;", AnimeDTO.class);
            Serializer.log(response);
            Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
            time = System.currentTimeMillis();
            response = db.select("SELECT * FROM anime WHERE votes in (5.5) LIMIT 1;", AnimeDTO.class);
            Serializer.log(response);
            Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
            time = System.currentTimeMillis();
            response = db.select("SELECT * FROM anime WHERE name='Bleach'", AnimeDTO.class);
            Serializer.log(response);
            Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
          time = System.currentTimeMillis();
          response = db.select("SELECT * FROM anime WHERE tags in ('Horror');", AnimeDTO.class);
          Serializer.log(response);
          Serializer.log((System.currentTimeMillis() - time) + "ms to fetch");
          //DataEntry de = db.insert("INSERT INTO anime SET name='Woot', rating=2.5, votes=(1.2,5.5,2.4,3.4), actors=((name='Megumi Toyoguchi', character='Witch', tags=('works?')), (name='Maaya Sakamoto')), tags=('Action','Fantasy')");
          //Serializer.log(de);
          //Serializer.log(db.update("UPDATE anime SET name='.Hack//Sign', actors.i0.name='test', actors.new=(name='actor1', tags=('tagOne','tagTwo')) WHERE id='a0f5fd74-18b4-40d0-8d7b-a5bbef2c6182'"));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, "name", "tags", "actors.name", "votes", "actors.tags");
    //populateDB(db);
    /*try {
      populateDBTwo(db);
    } catch (JSQLParserException e) {
      e.printStackTrace();
    }*/
  }

  static void populateDBTwo(NucleoDB db) throws JSQLParserException {
    Serializer.log(db.sql("INSERT INTO anime SET name='Hellsing Ultimate', tags=('Action','Horror','Supernatural'), actors=((name='Maaya Sakamoto',character='Rip Van Winkle'),(name='Fumiko Orikasa', character='Seras Victoria'))"));
    Serializer.log(db.sql("DELETE FROM anime WHERE name='Bleach'"));
    Serializer.log(db.sql("INSERT INTO anime SET name='Bleach', tags=('Action','Adventure','Fantasy'), actors=((name='Fumiko Orikasa', character='Rukia Kuchiki'))"));
  }

  static void populateDB(NucleoDB db) {
    db.getTable("anime").insert(new Anime("Bleach", Arrays.asList("Adventure", "Action", "Fantasy"), Float.valueOf((float) 1.2)));
    db.getTable("anime").insert(new Anime("Bleach", Arrays.asList("Adventure", "Action", "Fantasy"), Float.valueOf((float) 2.2)));
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
