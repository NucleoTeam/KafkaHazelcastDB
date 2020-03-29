package com.nucleocore.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.DataTable;
import com.nucleocore.db.database.TableTemplate;
import com.nucleocore.db.database.utils.StartupRun;
import com.nucleocore.db.database.utils.Test;

import java.util.*;

public class NucleoDB {
    private TreeMap<String, TableTemplate> tables = new TreeMap<>();
    static String latestSave = "";
    public static void main(String... args) {

        NucleoDB db = new NucleoDB();
        db.launchNucleoTable(null, "playerlist", Test.class, new StartupRun(){
            public void run() {
                System.out.println("STARTUP COMPLETE");
            }
        }, false);
        db.launchNucleoTable(null, "usertest", Test.class, new StartupRun(){
            public void run() {
                System.out.println("STARTUP COMPLETE");
            }
        }, false);
        //db.getTable("test4").addListener(Modification.DELETE, (d)->System.out.println("Deleted "+d.getClass().getName()));
       // db.getTable("userDataTest").addListener(Modification.CREATE, (d)->System.out.println("Created "+d.getClass().getName()));
        ObjectMapper om = new ObjectMapper();
        Scanner sc = new Scanner(System.in);
        Test test;
        int i = -1;
        while ((i = sc.nextInt()) != -1) {
            System.out.println("Selected: " + i);
            long time;
            Test user;
            switch (i) {
                case 0:
                    System.gc();
                    break;
                case 1:
                    user = new Test();
                    user.setKey("nathaniel.davidson@gmail.com");
                    user.setName("1");
                    user.setUser("firestar");
                    db.getTable("usertest").save(null, user, d -> {
                        try{
                            System.out.println("[" + om.writeValueAsString(d) + "] Finished save");
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                        latestSave = d.getKey();
                    });
                    break;
                case 2:
                    System.out.println(db.getTable("usertest").size());
                    break;
                case 3:
                    time = System.currentTimeMillis();
                    test = new Test();
                    test.setUser("firestar");
                    List<Test> dataIndex = db.getTable("usertest").search("user", test);
                    if (dataIndex != null) {
                        System.out.println("returned: " + dataIndex.size());
                        dataIndex.stream().forEach(t -> System.out.println(t.getKey()));
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
                        test = new Test();
                        test.setUser("firestar");
                        System.out.println(om.writeValueAsString(db.getTable("usertest").search("user", test)));
                        System.out.println(om.writeValueAsString(db.getTable("usertest").getIndexes()));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    System.out.println(System.currentTimeMillis() - time);
                    break;
                case 6:
                    db.getTable("usertest").resetIndex();
                    break;
            }
        }
    }
    public DataTable getTable(String table){
        return (DataTable)tables.get(table);
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, boolean startupConsume){
        StartupRun startup = new StartupRun(){
            public void run(DataTable table) {
                table.consume();
            }
        };
        DataTable t = new DataTable(bootstrap, table, clazz, startup, startupConsume);
        tables.put(table, t);
        return t;
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, StartupRun runnable, boolean startupConsume){
        DataTable t = new DataTable(bootstrap, table, clazz, runnable, startupConsume);
        tables.put(table, t);
        return t;
    }
}
