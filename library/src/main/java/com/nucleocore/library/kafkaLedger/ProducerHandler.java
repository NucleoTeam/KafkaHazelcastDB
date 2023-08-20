package com.nucleocore.library.kafkaLedger;

import com.nucleocore.library.database.modifications.Modify;
import com.nucleocore.library.database.utils.Serializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class ProducerHandler  {
    private KafkaProducer producer;
    private String table;

    public ProducerHandler(String bootstrap, String table) {
        producer = createProducer(bootstrap);
        this.table = table;
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

    public synchronized void push(Modify modify, Callback callback){
        try {
            ProducerRecord record = new ProducerRecord(
                table,
                UUID.randomUUID().toString(),
                modify.getClass().getSimpleName() + Serializer.getObjectMapper().getOm().writeValueAsString(modify)
            );
            getProducer().send(record, callback);
            getProducer().flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}