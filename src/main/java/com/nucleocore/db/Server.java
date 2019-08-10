package com.nucleocore.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.nucleocore.db.database.Database;
import com.nucleocore.db.database.Modification;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Test;

import java.io.IOException;
import java.util.*;

public class Server {
    static TreeMap<String, Database> databases = new TreeMap<>();
    public static void main(String... args) {
        String tableString = System.getenv("tables");
        if (tableString != null){
            for(String table : tableString.split(",")){
                databases.put(table, new Database(table));
            }
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                ObjectMapper om = new ObjectMapper();
                Scanner sc = new Scanner(System.in);
                int i = 0;
                while((i = sc.nextInt())!=0){
                    System.out.println("Selected: "+i);
                    switch (i){
                        case 1:
                            getTable("test4").save(null, new Test(UUID.randomUUID().toString(), "This","That"));
                            break;
                        case 2:
                            EntryObject e = new PredicateBuilder().getEntryObject();
                            Predicate sqlQuery = e.get("user").equal("That");
                            Collection<DataEntry> entries = getTable("test4").getMap().values( sqlQuery );
                            for(DataEntry de : entries){
                                System.out.println(((Test)de).getName());
                            }
                        break;
                        case 3:
                            DataEntry de = getTable("test4").getMap().get("44bae15f-4532-4492-87d2-a35799d0ae92");
                            try {
                                System.out.println(om.writeValueAsString(de));
                            }catch (IOException ex){
                                ex.printStackTrace();
                            }
                            getTable("test4").save(de, null);
                        break;
                        case 4:
                            try {
                                Test data = (Test) getTable("test4").getMap().get("3b9e56c4-5567-451c-8841-892d001bf88e");
                                Test data2 = new Test(data);
                                data2.setName(data2.getName()+".");
                                getTable("test4").save(data, data2);
                            }catch (Exception ex){
                                ex.printStackTrace();
                            }
                        break;
                    }
                }
            }
        }).start();
    }
    public static Database getTable(String table){
        return databases.get(table);
    }
}
