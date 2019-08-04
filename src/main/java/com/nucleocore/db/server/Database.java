package com.nucleocore.db.server;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;

import java.util.UUID;

public class Database {
    private ProducerHandler producer;
    private ConsumerHandler consumer;
    private static HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    private IMap<String, Entry> map;
    public Database(String name){
        map = hz.getMap(name);
        producer = new ProducerHandler("192.169.1.16:9092,192.169.1.19:9092,192.169.1.17:9092");
        consumer = new ConsumerHandler("192.169.1.16:9092,192.169.1.19:9092,192.169.1.17:9092", UUID.randomUUID().toString());
    }

    public IMap<String, Entry> getMap() {
        return map;
    }

    public void setMap(IMap<String, Entry> map) {
        this.map = map;
    }
}
