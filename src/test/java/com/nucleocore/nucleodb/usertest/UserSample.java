package com.nucleocore.nucleodb.usertest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.StartupRun;

import java.time.Instant;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class UserSample{
  public static void main(String... args) {

    System.out.println("Running database server");

    runTest();
  }
  public static void runTest(){
    NucleoDB db = new NucleoDB();
    db.launchNucleoTable(null, "playerlist", User.class, new StartupRun(){
      public void run() {
        System.out.println("STARTUP COMPLETE");
      }
    }, "/user");

    db.launchNucleoTable(null, "usertest", User.class, Instant.ofEpochSecond(1690785144,702348000), new StartupRun(){
      public void run() {
        System.out.println("STARTUP COMPLETE");
      }
    }, "/user");
    //db.getTable("test4").addListener(Modification.DELETE, (d)->System.out.println("Deleted "+d.getClass().getName()));
    // db.getTable("userDataTest").addListener(Modification.CREATE, (d)->System.out.println("Created "+d.getClass().getName()));
    ObjectMapper om = new ObjectMapper();
    Scanner sc = new Scanner(System.in);
    User test;
    int i = -1;
    while ((i = sc.nextInt()) != -1) {
      System.out.println("Selected: " + i);
      long time;
      User user;
      switch (i) {
        case 0:
          System.gc();
          break;
        case 1:
          user = new User();
          user.setName("1");
          user.setUser("Firestarthe");
          db.getTable("usertest").insert(user, d -> {
            try{
              System.out.println("[" + om.writeValueAsString(d) + "] Finished save");
            }catch(Exception e){
              e.printStackTrace();
            }
          });
          break;
        case 2:
          System.out.println(db.getTable("usertest").size());
          break;
        case 3:
          for (int j = 0; j < 75; j++) {
            time = System.currentTimeMillis();
            Set<DataEntry> dataIndex = db.getTable("usertest").search("/user", "Firestarthe");
            if (dataIndex != null) {
              System.out.println("returned: " + dataIndex.size());
            }
            System.out.println(System.currentTimeMillis() - time);
          }
          break;
        case 4:
          time = System.currentTimeMillis();
          db.getTable("usertest").flush();
          System.out.println(System.currentTimeMillis() - time);
          break;
        case 5:
          time = System.currentTimeMillis();
          try {
            System.out.println(om.writeValueAsString(db.getTable("usertest").search("/user", "Firestarthe")));
          } catch (JsonProcessingException e) {
            e.printStackTrace();
          }
          System.out.println(System.currentTimeMillis() - time);
          break;
        case 6:
          db.getTable("usertest").get("/user", "Firestarthe");
          break;
        case 7:
          DataEntry de = db.getTable("usertest").searchOne("/user", "Firestarthe");
          ((User)de.getData()).setUser("changedUser");
          db.getTable("usertest").save(de);
          break;
        case 8:
          time = System.currentTimeMillis();
          try {
            System.out.println(new ObjectMapper().writeValueAsString(db.getTable("usertest").getEntries()));
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          try {
            System.out.println(new ObjectMapper().writeValueAsString(db.getTable("usertest").getIndexes()));
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          System.out.println(System.currentTimeMillis() - time);
          break;
        case 9:
          for (int j = 0; j < 75; j++) {
            time = System.currentTimeMillis();
            Set<DataEntry> dataIndex = db.getTable("usertest").get("/user", "Firestarthe");
            if (dataIndex != null) {
              System.out.println("returned: " + dataIndex.size());
            }
            System.out.println(System.currentTimeMillis() - time);
          }
          break;
      }
    }
  }
}
