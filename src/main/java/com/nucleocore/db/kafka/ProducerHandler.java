package com.nucleocore.db.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.modifications.Modify;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

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

    public KafkaProducer getProducer() {
        return producer;
    }

    public synchronized void save(Modify modify){
        synchronized(pendingSaves) {
            pendingSaves.add(modify);
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
        do {
            Modify mod;
            while((mod = pop())!=null) {
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
            try {
                Thread.sleep(0, 10);
            }catch (Exception e){
                e.printStackTrace();
            }
        } while(!Thread.interrupted());
    }
}