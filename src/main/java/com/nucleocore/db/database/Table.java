package com.nucleocore.db.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.nucleocore.db.database.modifications.Create;
import com.nucleocore.db.database.modifications.Delete;
import com.nucleocore.db.database.modifications.Update;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.UUID;

public class Table {
    private ProducerHandler producer;
    private ConsumerHandler consumer;
    private static HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    private IMap<String, DataEntry> map;
    private ObjectMapper om = new ObjectMapper();
    public Table(String table, String bootstrap){
        new Thread(new Runnable() {
            @Override
            public void run() {
                map = hz.getMap(table);
            }
        }).start();

        producer = new ProducerHandler(bootstrap, table);
        consumer = new ConsumerHandler(bootstrap, UUID.randomUUID().toString(), this, table);
    }

    public IMap<String, DataEntry> getMap() {
        return map;
    }

    public boolean compare(Object a, Object b){
        if(a.getClass()==String.class && b.getClass()==String.class){
            return ((String)a).equals((String)b);
        }else if(a.getClass()==Integer.class && b.getClass()==Integer.class){
            return ((Integer)a) == ((Integer)b);
        }else if(a.getClass()==Long.class && b.getClass()==Long.class){
            return ((Long)a) == ((Long)b);
        }
        return false;
    }

    public synchronized boolean save(DataEntry oldEntry, DataEntry newEntry){
        if(oldEntry == null && newEntry != null){
            System.out.println("CREATE CHECKS");
            try {
                Create createEntry = new Create(newEntry.getKey(), newEntry);
                producer.save(createEntry);
            }catch (IOException e){
                e.printStackTrace();
            }
        }else if(newEntry == null && oldEntry != null){
            System.out.println("DELETE CHECKS");
            Delete deleteEntry = new Delete(oldEntry.getKey());
            producer.save(deleteEntry);
        }else if(newEntry != null && oldEntry != null){
            System.out.println("UPDATE CHECKS");
            Update updateEntry = new Update();
            try {
                updateEntry.setKey(newEntry.getKey());
                boolean changed = false;
                updateEntry.setMasterClass(newEntry.getClass().getName());
                for (Field f : newEntry.getClass().getDeclaredFields()) {
                    if (!compare(f.get(newEntry), f.get(oldEntry))) {
                        updateEntry.getChange().put(f.getName(), f.get(newEntry));
                        changed = true;
                    }
                }
                if(changed) {
                    System.out.println("Changed");
                    producer.save(updateEntry);
                    return true;
                }
                System.out.println("Nothing changed");
                return false;
            }catch (Exception e){
                e.printStackTrace();
                return false;
            }
        } else
            return false;
        return true;
    }

    public void modify(Modification mod, Object modification){
        switch(mod){
            case CREATE:
                Create c = (Create) modification;
                System.out.println("Create statement called");
                if(c!=null){
                    try {
                        map.set(c.getKey(), c.getValue());
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            break;
            case DELETE:
                Delete d = (Delete) modification;
                System.out.println("Delete statement called");
                if(d!=null){
                    map.delete(d.getKey());
                }
            break;
            case UPDATE:
                Update u = (Update) modification;
                System.out.println("Update statement called");
                if(u!=null){
                    try {
                        Class clazz = Class.forName(u.getMasterClass());
                        Object obj = clazz.cast(map.get(u.getKey()));
                        u.getChange().forEach((String key, Object val)->{
                            try {
                                clazz.getDeclaredField(key).set(obj, val);
                            } catch (Exception e){

                            }
                        });
                        map.set(u.getKey(), (DataEntry) obj);
                    } catch (Exception e){

                    }
                }
            break;
        }
    }
    public void setMap(IMap<String, DataEntry> map) {
        this.map = map;
    }
}
