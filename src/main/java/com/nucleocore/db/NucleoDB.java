package com.nucleocore.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.nucleocore.db.database.Table;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Test;

import java.io.IOException;
import java.util.*;

public class NucleoDB {
    static TreeMap<String, Table> tables = new TreeMap<>();
    public static void main(String... args) {
        launchTable("192.169.1.16:9093,192.169.1.19:9093,192.169.1.17:9093", "test4");
        new Thread(()->{
            ObjectMapper om = new ObjectMapper();
            Scanner sc = new Scanner(System.in);
            int i = 0;
            while((i = sc.nextInt())!=0){
                System.out.println("Selected: "+i);
                long time;
                switch (i){
                    case 1:
                        getTable("test4").save(null, new Test(UUID.randomUUID().toString(), "This","That"));
                        break;
                    case 2:
                        time = System.currentTimeMillis();
                        EntryObject e = new PredicateBuilder().getEntryObject();
                        Predicate sqlQuery = e.get("user").equal("That");
                        Collection<DataEntry> entries = getTable("test4").getMap().values( sqlQuery );
                        System.out.println(System.currentTimeMillis()-time);
                        for(DataEntry de : entries){
                            System.out.println(((Test)de).getName());
                        }
                    break;
                    case 3:
                        DataEntry de = getTable("test4").getMap().get("");
                        try {
                            System.out.println(om.writeValueAsString(de));
                        }catch (IOException ex){
                            ex.printStackTrace();
                        }
                        getTable("test4").save(de, null);
                    break;
                    case 4:
                        try {
                            time = System.currentTimeMillis();
                            Test data = (Test) getTable("test4").getMap().get("53241121-b828-4e7c-848e-a83d93a98ddb");
                            Test data2 = new Test(data);
                            data2.setName(data2.getName()+".");
                            getTable("test4").save(data, data2);
                            System.out.println(System.currentTimeMillis()-time);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                    break;
                }
            }
        }).start();
    }
    public static Table getTable(String table){
        return tables.get(table);
    }
    public static Table launchTable(String bootstrap, String table){
        Table t = new Table(bootstrap, table);
        tables.put(table, t);
        return t;
    }
}
