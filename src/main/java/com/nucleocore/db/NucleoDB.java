package com.nucleocore.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.LargeDataTable;
import com.nucleocore.db.database.TableTemplate;
import com.nucleocore.db.database.utils.*;
import com.nucleocore.db.database.DataTable;
import org.supercsv.cellprocessor.*;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import java.io.IOException;
import java.util.*;

public class NucleoDB {
    private TreeMap<String, TableTemplate> tables = new TreeMap<>();
    static String latestSave = "";
    public static void main(String... args) {
        NucleoDB db = new NucleoDB();
        db.launchLargeTable("192.168.1.7:9092,192.168.1.6:9092", "test4");
        db.launchTable(null, "userDataTest");
        //db.getTable("test4").addListener(Modification.DELETE, (d)->System.out.println("Deleted "+d.getClass().getName()));
       // db.getTable("userDataTest").addListener(Modification.CREATE, (d)->System.out.println("Created "+d.getClass().getName()));

            ObjectMapper om = new ObjectMapper();
            Scanner sc = new Scanner(System.in);
            int i = 0;
            while((i = sc.nextInt())!=0){
                System.out.println("Selected: "+i);
                long time;
                switch (i){
                    case 1:
                        db.getTable("test4").save(null, new Test(UUID.randomUUID().toString(), "This", "Thot"), d->{
                            System.out.println("["+d.getKey()+"] Finished save");
                            latestSave = d.getKey();
                        });
                        break;
                    case 2:
                        time = System.currentTimeMillis();
                        ((DataTable)db.getTable("test4"))
                            .filterMap(x->{
                                Test t = ((Test)x.getValue());
                                return t.getUser().equals("Thot");
                            })
                            .forEach((entry)->{
                                System.out.println(((Test)entry.getValue()).getUser());
                            });
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 3:
                        DataEntry de = ((DataTable)db.getTable("test4")).get(latestSave);
                        try {
                            System.out.println(om.writeValueAsString(de));
                        }catch (IOException ex){
                            ex.printStackTrace();
                        }
                        db.getTable("test4").save(de, null);
                        break;
                    case 4:
                        try {
                            time = System.currentTimeMillis();
                            Test data = ((DataTable)db.getTable("test4")).get(latestSave);
                            if(data!=null) {
                                Test data2 = new Test(data);
                                data2.setName(data2.getName() + ".");
                                db.getTable("test4").save(data, data2);
                            }else{
                                System.out.println("null");
                            }
                            System.out.println(System.currentTimeMillis()-time);
                        }catch (Exception ex){
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
                            .readIntoStream("G:/test.csv", (DataTable)db.getTable("userDataTest"), User.class);
                        System.out.println("Created "+x+" users.");
                        break;
                    case 11:
                        time = System.currentTimeMillis();
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("userDataTest").search("user", "Firestar", Player.class)));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 12:
                        time = System.currentTimeMillis();
                        System.out.println("READING IN DATA");
                        /*db.getTable("test4").save(null, new Player(1,"firestarthe","piou4t3o78thgiudnsjkvn7824h"));
                        for(int y=2;y<10;y++){
                            db.getTable("test4").save(null, new Player(
                              y,
                              UUID.randomUUID().toString().replaceAll("-","").substring(0, (int)(Math.random()*27)+4),
                              UUID.randomUUID().toString().replaceAll("-","").substring(0, (int)(Math.random()*27)+4)
                            ));
                        }*/

                        db.getTable("test4").startImportThreads();
                        int y = new Importer()
                            .addMap("player_id", "playerId", new ParseLong()) // pass
                            .addMap(new Optional()) // name
                            .addMap(new Optional()) // id
                            .readIntoStream("G:/players.csv", db.getTable("test4"), Player.class);

                        //db.getTable("test4").stopImportThreads();
                        //System.out.println("Created "+y+" players.");
                        System.out.println("Exec Time: "+(System.currentTimeMillis()-time));
                        break;
                    case 13:
                        time = System.currentTimeMillis();
                        String name = "firestarthe";
                        System.out.println("searching for player "+name);
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("test4").search("name", name, Player.class)));
                            //System.out.println(om.writeValueAsString(((LargeDataTable)db.getTable("test4")).sortedIndex));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println("time: "+(System.currentTimeMillis()-time));
                        break;
                    case 14:
                        time = System.currentTimeMillis();
                        name = "darkshadow8891";
                        System.out.println("searching for player "+name);
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("test4").search("name", name, Player.class)));
                            //System.out.println(om.writeValueAsString(db.getTable("test4").trieIndex));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        System.out.println("time: "+(System.currentTimeMillis()-time));
                        break;
                }
            }
    }
    public TableTemplate getTable(String table){
        return tables.get(table);
    }
    public TableTemplate launchTable(String bootstrap, String table){
        DataTable t = new DataTable(bootstrap, table);
        tables.put(table, t);
        return t;
    }
    public TableTemplate launchLargeTable(String bootstrap, String table){
        LargeDataTable t = new LargeDataTable(bootstrap, table);
        tables.put(table, t);
        return t;
    }
}
