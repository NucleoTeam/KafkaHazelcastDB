package com.nucleocore.db.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.Database;
import com.nucleocore.db.database.Modification;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;

import java.util.*;
import java.util.regex.Pattern;

public class ConsumerHandler implements Runnable {
    private Stack<String> entries = new Stack<>();
    private KafkaConsumer consumer;
    private Database database;

    public ConsumerHandler(String bootstrap, String groupName, Database database) {
        this.database = database;
        this.consumer = createConsumer(bootstrap, groupName);
    }
    public synchronized String pop(){
        return entries.pop();
    }

    @Override
    public void run() {
        ObjectMapper om = new ObjectMapper();
        do {
            String entry;
            if ((entry = pop())!=null) {
                String type = entry.substring(0, 5);
                String data = entry.substring(6);
                try {
                    Modification mod = Modification.get(type);
                    database.modify(mod, om.readValue(data, mod.getModification()));
                }catch (Exception e){
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

    private KafkaConsumer createConsumer(String bootstrap, String groupName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer consumer = new KafkaConsumer(props);
        return consumer;
    }
    public void subscribe(String[] topics){
        //System.out.println("Subscribed to topic " + topic);
        consumer.subscribe(Arrays.asList(topics));
    }

    public KafkaConsumer getConsumer() {
        return this.consumer;
    }

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }
}