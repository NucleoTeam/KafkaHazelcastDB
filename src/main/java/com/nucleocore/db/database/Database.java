package com.nucleocore.db.database;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.nucleocore.db.database.modifications.Create;
import com.nucleocore.db.database.modifications.Delete;
import com.nucleocore.db.database.modifications.Update;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;
import com.nucleocore.db.database.DataEntry;

import java.util.UUID;

public class Database {
    private ProducerHandler producer;
    private ConsumerHandler consumer;
    private static HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    private IMap<String, DataEntry> map;
    public Database(String name){
        map = hz.getMap(name);
        producer = new ProducerHandler("192.169.1.16:9092,192.169.1.19:9092,192.169.1.17:9092");
        consumer = new ConsumerHandler("192.169.1.16:9092,192.169.1.19:9092,192.169.1.17:9092", UUID.randomUUID().toString(), this);
    }

    public IMap<String, DataEntry> getMap() {
        return map;
    }

    public void modify(Modification mod, Object modification){
        switch(mod){
            case CREATE:
                Create c = (Create) modification;
                if(c!=null){
                    map.set(c.getKey(), c.getData());
                }
            break;
            case DELETE:
                Delete d = (Delete) modification;
                if(d!=null){
                    map.delete(d.getKey());
                }
            break;
            case UPDATE:
                Update u = (Update) modification;
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
