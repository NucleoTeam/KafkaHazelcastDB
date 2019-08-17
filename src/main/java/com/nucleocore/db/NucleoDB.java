package com.nucleocore.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.Modification;
import com.nucleocore.db.database.Table;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Test;

import java.io.IOException;
import java.util.*;

public class NucleoDB {
    private TreeMap<String, Table> tables = new TreeMap<>();
    static String latestSave = "";
    static String getSaltString() {
        String SALTCHARS = "HELLO";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < 5) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }
    public static void main(String... args) {
        NucleoDB db = new NucleoDB();
        db.launchTable(null, "test4");
        db.getTable("test4").addListener(Modification.DELETE, (d)->System.out.println("Deleted "+d.getClass().getName()));
        new Thread(()->{
            ObjectMapper om = new ObjectMapper();
            Scanner sc = new Scanner(System.in);
            int i = 0;
            while((i = sc.nextInt())!=0){
                System.out.println("Selected: "+i);
                long time;
                switch (i){
                    case 1:
                        db.getTable("test4").save(null, new Test(UUID.randomUUID().toString(), "This",getSaltString()), d->{
                            System.out.println("["+d.getKey()+"] Finished save");
                            latestSave = d.getKey();
                        });
                        break;
                    case 2:
                        time = System.currentTimeMillis();
                        db.getTable("test4")
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
                        DataEntry de = db.getTable("test4").get(latestSave);
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
                            Test data = db.getTable("test4").get(latestSave);
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
                        System.out.println(db.getTable("test4").size());
                        break;
                    case 6:
                        time = System.currentTimeMillis();
                        Set<Test> dataIndex = db.getTable("test4").indexSearch("user", "Thot");
                        if(dataIndex!=null){
                            System.out.println("returned: "+dataIndex.size());
                            dataIndex.parallelStream().forEach(t->System.out.println(t.getKey()));
                        }
                        System.out.println(System.currentTimeMillis()-time);
                        break;
                    case 7:
                        db.getTable("test4").flush();
                        break;
                    case 8:
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("test4").trieIndexSearch("user", "Thot")));
                            //System.out.println(om.writeValueAsString(db.getTable("test4").trieIndex));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        break;
                    case 9:
                        try {
                            System.out.println(om.writeValueAsString(db.getTable("test4").trieIndex));
                        }catch (JsonProcessingException e){
                            e.printStackTrace();
                        }
                        break;
                }
            }
        }).start();
    }
    public Table getTable(String table){
        return tables.get(table);
    }
    public Table launchTable(String bootstrap, String table){
        Table t = new Table(bootstrap, table);
        tables.put(table, t);
        return t;
    }
}
