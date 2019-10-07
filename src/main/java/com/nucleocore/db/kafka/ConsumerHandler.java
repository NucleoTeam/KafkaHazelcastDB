package com.nucleocore.db.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.nucleocore.db.database.TableTemplate;
import com.nucleocore.db.database.utils.Modification;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ConsumerHandler implements Runnable {
    private Queue<String> entries = Queues.newArrayDeque();
    private KafkaConsumer consumer;
    private TableTemplate database;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ConsumerHandler(String bootstrap, String groupName, TableTemplate database, String table) {
        this.database = database;
        this.consumer = createConsumer(bootstrap, groupName);

        this.subscribe(table.split(","));
        new Thread(()->{
            ObjectMapper om = new ObjectMapper();
            do {
                try {
                    String entry;
                    countDownLatch.await();
                    long lastAdd = 0;
                    while ((entry = pop())!=null) {
                        String type = entry.substring(0, 6);
                        String data = entry.substring(6);
                        //System.out.println("Action: " + type + " data: "+data);
                        try {
                            Modification mod = Modification.get(type);
                            if(lastAdd+5>System.currentTimeMillis()) {
                                database.setBuildIndex(false);
                                lastAdd = System.currentTimeMillis();
                            }else{
                                database.setBuildIndex(true);
                                lastAdd = System.currentTimeMillis();
                            }
                            database.modify(mod, om.readValue(data, mod.getModification()));

                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    if(database.isUnsavedIndexModifications()){
                        database.setBuildIndex(true);
                        database.resetIndex();
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            } while(!Thread.interrupted());
        }).start();
        new Thread(this).start();
    }

    public String pop(){
        if(entries.isEmpty())
            return null;
        return entries.remove();
    }

    public void add(String e){
        //System.out.println("Modification from kafka: "+e);
        getEntries().add(e);
        countDownLatch.countDown();
    }
    @Override
    public void run() {
        consumer.commitAsync();
        ObjectMapper om = new ObjectMapper();
        do {
            ConsumerRecords<Integer, String> rs = getConsumer().poll(Duration.ofNanos(20));
            if(!rs.isEmpty()){
                //System.out.println("RECEIVED DATA");
                Iterator<ConsumerRecord<Integer, String>> iter = rs.iterator();
                while(iter.hasNext()){
                    add(iter.next().value());
                }
            }
            consumer.commitAsync();
        } while(!Thread.interrupted());
    }

    private KafkaConsumer createConsumer(String bootstrap, String groupName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        System.out.println(groupName);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer consumer = new KafkaConsumer(props);
        return consumer;
    }
    public void subscribe(String[] topics){
        System.out.print("Subscribed to topic "+Arrays.asList(topics).toString());
        consumer.subscribe(Arrays.asList(topics));
    }

    public KafkaConsumer getConsumer() {
        return this.consumer;
    }

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    public Queue<String> getEntries() {
        return entries;
    }

    public void setEntries(Queue<String> entries) {
        this.entries = entries;
    }

    public TableTemplate getDatabase() {
        return database;
    }

    public void setDatabase(TableTemplate database) {
        this.database = database;
    }
}