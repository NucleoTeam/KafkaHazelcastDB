package com.nucleocore.db.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.nucleocore.db.database.TableTemplate;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Modification;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ConsumerHandler implements Runnable {
    private Queue<String> entries = Queues.newArrayDeque();
    private Queue<String> toIndex = Queues.newArrayDeque();
    private KafkaConsumer consumer;
    private TableTemplate database;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public ConsumerHandler(String bootstrap, String groupName, TableTemplate database, String table) {
        this.database = database;
        this.consumer = createConsumer(bootstrap, groupName);

        this.subscribe(table.split(","));
        new Thread(()->{
            ObjectMapper om = new ObjectMapper();
            om.enableDefaultTyping();
            do {
                try {
                    String entry;
                    database.setBuildIndex(true);
                    countDownLatch.await();
                    countDownLatch=new CountDownLatch(1);
                    while ((entry = pop())!=null) {
                        if(entries.size()>10){
                            database.setBuildIndex(false);
                        }
                        if(Thread.currentThread().interrupted()){
                            return;
                        }
                        String type = entry.substring(0, 6);
                        String data = entry.substring(6);
                        //System.out.println("Action: " + type + " data: "+data);
                        Modification mod = Modification.get(type);
                        if(mod!=null) {
                            database.modify(mod, om.readValue(data, mod.getModification()));
                        }
                    }
                    if(database.isUnsavedIndexModifications()){
                        database.resetIndex();
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            } while(!Thread.currentThread().interrupted());
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
        do {
            ConsumerRecords<Integer, String> rs = getConsumer().poll(Duration.ofMillis(5));
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
        //System.out.println(groupName);
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
        System.out.println("Subscribed to topic "+Arrays.asList(topics).toString());
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