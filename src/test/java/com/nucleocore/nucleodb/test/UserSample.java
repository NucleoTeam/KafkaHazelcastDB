package com.nucleocore.nucleodb.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatchException;
import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.utils.ChangeHandler;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.StartupRun;

import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.stream.Collectors;

public class UserSample{
  public static void main(String... args) {

    System.out.println("Running database server");
    User user = new User();
    User user2 = new User();
    user2.setUser("OtherUsername");
    user2.setName("David");
    user.setName("Nathaniel");
    user.setUser("Firestarthe");
    user.getTestingNested().get(0).setNestedValue("yikes");
    user.getTestingNested().add(new UserNested());
    ObjectMapper om = new ObjectMapper();
    try {
      ChangeHandler changeHandler = new ChangeHandler(User.class).getOperations(user2, user);
      User val = changeHandler.applyChanges(user2);
      System.out.println(changeHandler.getPatchJSON());
      System.out.println(om.writeValueAsString(val));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } catch (JsonPatchException e) {
      throw new RuntimeException(e);
    }

    runTest();
  }
  public static void runTest(){
    NucleoDB db = new NucleoDB();
    db.launchNucleoTable(null, "playerlist", User.class, new StartupRun(){
      public void run() {
        System.out.println("STARTUP COMPLETE");
      }
    }, "/user");

    db.launchNucleoTable(null, "usertest", User.class, new StartupRun(){
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
          time = System.currentTimeMillis();
          List<DataEntry> dataIndex = db.getTable("usertest").search("/user", "Firestarthe");
          if (dataIndex != null) {
            System.out.println("returned: " + dataIndex.size());
            try {
              System.out.println(new ObjectMapper().writeValueAsString(dataIndex.stream().map(de->de.getData()).collect(Collectors.toList())));
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          }
          System.out.println(System.currentTimeMillis() - time);
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
          db.getTable("usertest");
          break;
        case 7:
          DataEntry de = db.getTable("usertest").searchOne("/user", "Firestarthe");
          ((User)de.getData()).setUser("changedUser");
          db.getTable("usertest").save(de);
          break;
      }
    }
  }
}
