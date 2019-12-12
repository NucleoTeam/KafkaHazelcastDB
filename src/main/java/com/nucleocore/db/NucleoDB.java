package com.nucleocore.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.LargeDataTable;
import com.nucleocore.db.database.TableTemplate;
import com.nucleocore.db.database.DataTable;
import com.nucleocore.db.database.utils.Importer;
import com.nucleocore.db.database.utils.Test;
import org.supercsv.cellprocessor.*;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import java.io.IOException;
import java.util.*;

public class NucleoDB {
    private TreeMap<String, TableTemplate> tables = new TreeMap<>();
    static String latestSave = "";
    public static void main(String... args) {
        /*
        NucleoDB db = new NucleoDB();
        db.launchLargeTable("192.168.1.7:9092,192.168.1.6:9092,192.168.1.8:9092,192.168.1.5:9092", "playerlist", Player.class, ()->{
            System.out.println("STARTUP COMPLETE");
        });
        db.launchLargeTable("192.168.1.7:9092,192.168.1.6:9092,192.168.1.8:9092,192.168.1.5:9092", "usertest", User.class, ()->{
            System.out.println("STARTUP COMPLETE");
        });
        //db.getTable("test4").addListener(Modification.DELETE, (d)->System.out.println("Deleted "+d.getClass().getName()));
       // db.getTable("userDataTest").addListener(Modification.CREATE, (d)->System.out.println("Created "+d.getClass().getName()));

            ObjectMapper om = new ObjectMapper();
            Scanner sc = new Scanner(System.in);
            int i = -1;
            while((i = sc.nextInt())!=-1){
                System.out.println("Selected: "+i);
                long time;
                User user;
                switch (i){
                    case 0:
                        System.gc();
                        break;
                    case 1:
                        user = new User();
                        user.setEmail("nathaniel.davidson@gmail.com");
                        user.setPlayer(1);
                        user.setUser("firestar");
                        db.getLargeTable("usertest").save(null, user, d->{
                            System.out.println("["+d.getKey()+"] Finished save");
                            latestSave = d.getKey();
                        });
                        break;
                    case 2:
                        time = System.currentTimeMillis();
                        user = (User) db.getLargeTable("usertest").searchOne("user", "firestar");
                        if(user!=null){
                            User changed = new User(user);
                            changed.setUser("test");
                            db.getLargeTable("usertest").save(user, changed, d->{
                                System.out.println("["+d.getKey()+"] Finished save");
                                latestSave = d.getKey();
                            });
                        }

                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 3:
                        user = (User) db.getLargeTable("usertest").searchOne("user", "firestar");
                        try {
                            System.out.println(om.writeValueAsString(user));
                        }catch (IOException ex){
                            ex.printStackTrace();
                        }
                        break;
                    case 4:
                        user = (User) db.getLargeTable("usertest").searchOne("user", "test");
                        try {
                            System.out.println(om.writeValueAsString(user));
                        }catch (IOException ex){
                            ex.printStackTrace();
                        }
                        break;
                    case 5:
                        System.out.println(((DataTable)db.getTable("test4")).size());
                        break;
                    case 6:
                        time = System.currentTimeMillis();
                        List<Test> dataIndex = db.getTable("test4").search("user", "Thot", Test.class);
                        if(dataIndex!=null){
                            System.out.println("returned: "+dataIndex.size());
                            dataIndex.stream().forEach(t->System.out.println(t.getKey()));
                        }
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 7:
                        time = System.currentTimeMillis();
                        ((DataTable)db.getTable("test4")).flush();
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 8:
                        time = System.currentTimeMillis();
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("test4").search("user", "Thot", Test.class)));
                            //System.out.println(om.writeValueAsString(((LargeDataTable)db.getTable("test4")).entries));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 9:
                        time = System.currentTimeMillis();
                        try {
                            System.out.println(om.writeValueAsString(((DataTable)db.getTable("test4")).trieIndex));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 10:
                        int x = new Importer()
                            .addMap(new ParseLong()) // id
                            .addMap(new NotNull()) // user
                            .addMap("pass", null, new Optional()) // pass
                            .addMap(new Optional()) // email
                            .addMap("api_key","apiKey", new NotNull())
                            .addMap(new ParseDate("yyyy-MM-dd"))  //joined
                            .addMap(new ParseLong()) // player
                            .addMap(new ParseBool()) // mod
                            .addMap(new Optional()) // ip
                            .addMap(new Optional())
                            .addMap(new Optional())
                            .addMap(new Optional())
                            .addMap(new ParseBool()) // premium
                            .addMap(new ParseInt()) // level
                            .addMap(new Optional()) // position
                            .addMap(new Optional()) // registerip
                            .addMap(new Optional())
                            .addMap(new Optional())
                            .addMap(new ParseInt()) // disputeclosurecount
                            .addMap("staff_flags","staffFlags", new NotNull()) // staffFlags
                            .addMap(new ParseBool()) // recentiplock
                            .addMap(new ParseBool()) // iplock
                            .addMap("pass2","pass", new Optional()) // pass
                            .addMap(new ParseLong()) // lastAction
                            .addMap(new Optional())
                            .addMap(new Optional()) // reports
                            .addMap(new Optional())
                            .addMap(new Optional())
                            .readIntoStream("G:/users.csv", db.getTable("userDataTest"), User.class);
                        db.getTable("userDataTest").consume();
                        System.out.println("Created "+x+" users.");
                        break;
                    case 11:
                        time = System.currentTimeMillis();
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("userDataTest").search("user", "Firestar", User.class)));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 12:
                        time = System.currentTimeMillis();
                        System.out.println("READING IN DATA");
                        db.getTable("test4").save(null, new Player(1,"firestarthe","piou4t3o78thgiudnsjkvn7824h"));
                        for(int y=2;y<10;y++){
                            db.getTable("test4").save(null, new Player(
                              y,
                              UUID.randomUUID().toString().replaceAll("-","").substring(0, (int)(Math.random()*27)+4),
                              UUID.randomUUID().toString().replaceAll("-","").substring(0, (int)(Math.random()*27)+4)
                            ));
                        }

                        int y = new Importer()
                            .addMap("player_id", "playerId", new ParseLong()) // pass
                            .addMap(new Optional()) // name
                            .addMap(new Optional()) // id
                            .readIntoStream("G:/players.csv", db.getTable("players"), Player.class);
                        db.getTable("players").consume();
                        //db.getTable("test4").stopImportThreads();
                        //System.out.println("Created "+y+" players.");
                        System.out.println("Exec Time: "+(System.currentTimeMillis()-time));
                        break;
                    case 13:
                        time = System.currentTimeMillis();
                        String name = "firestarthe";
                        System.out.println("searching for player "+name);
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("players").search("name", name, Player.class)));
                            //System.out.println(om.writeValueAsString(((LargeDataTable)db.getTable("test4")).sortedIndex));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println("time: "+(System.currentTimeMillis()-time));
                        break;
                    case 14:
                        time = System.currentTimeMillis();
                        try {
                            System.out.println(om.writeValueAsString(db.getLargeTable("playerlist").search("name", "firestarthe")));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        try {
                            System.out.println(om.writeValueAsString(db.getLargeTable("playerlist").search("name", "brookeelisia")));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println("time: "+(System.currentTimeMillis()-time));
                        break;
                    case 16:
                        db.getLargeTable("playerlist").resetIndex();
                        break;
                }
            }
            */
    }
    public DataTable getTable(String table){
        return (DataTable)tables.get(table);
    }
    public LargeDataTable getLargeTable(String table){
        return (LargeDataTable)tables.get(table);
    }
    public TableTemplate launchTable(String bootstrap, String table, Class clazz){
        DataTable t = new DataTable(bootstrap, table, clazz);
        t.consume();
        tables.put(table, t);
        return t;
    }
    public TableTemplate launchLargeTable(String bootstrap, String table, Class clazz, Runnable runnable){
        LargeDataTable t = new LargeDataTable(bootstrap, table, clazz, runnable);
        t.consume();
        tables.put(table, t);
        return t;
    }
}
