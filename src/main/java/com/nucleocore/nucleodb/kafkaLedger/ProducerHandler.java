package com.nucleocore.nucleodb.kafkaLedger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.database.modifications.Modify;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ProducerHandler implements Runnable {
    private KafkaProducer producer;
    private Queue<Modify> pendingSaves = new LinkedList<>();
    private String table;

    public ProducerHandler(String bootstrap, String table) {
        producer = createProducer(bootstrap);
        this.table = table;
        new Thread(this).start();
    }

    private KafkaProducer createProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer(props);
    }
    public CountDownLatch latch = new CountDownLatch(1);
    public KafkaProducer getProducer() {
        return producer;
    }

    public synchronized void save(Modify modify){
        synchronized(pendingSaves) {
            pendingSaves.add(modify);
            latch.countDown();
        }
    }

    public Modify pop(){
        synchronized(pendingSaves) {
            return (pendingSaves.isEmpty()) ? null : pendingSaves.remove();
        }
    }
    @Override
    public void run() {
        ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping();
        do {
            try {
                latch.await();
                latch = new CountDownLatch(1);
                Modify mod;
                while ((mod = pop()) != null) {
                    try {
                        ProducerRecord record = new ProducerRecord(
                            table,
                            UUID.randomUUID().toString(),
                            mod.getClass().getSimpleName() + om.writeValueAsString(mod)
                        );
                        getProducer().send(record);
                        //System.out.println("Sending to " + table + " datagram: " + mod.getClass().getSimpleName() + om.writeValueAsString(mod));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        } while(!Thread.interrupted());
    }
}