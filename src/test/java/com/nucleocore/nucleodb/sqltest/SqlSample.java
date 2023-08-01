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
    }, "/name", "/tags");
    try {
      db.getTable("anime").setSave(false);
      //db.getTable("anime").insert(new Anime("Bleach", Arrays.asList("Adventure", "Action", "Fantasy")));
      //db.getTable("anime").insert(new Anime("Full Metal Panic? Fumoffu", Arrays.asList("Comedy", "Action")));
      db.sql("SELECT * FROM anime WHERE name='Bleach' AND tags in ('Adventure', 'Action') OR name like 'Full Metal' LIMIT 20;", AnimeDTO.class);
    } catch (JSQLParserException e) {
      throw new RuntimeException(e);
    }
  }
}
