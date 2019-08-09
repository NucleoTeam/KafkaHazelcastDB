package com.nucleocore.db.kafka;

import com.nucleocore.db.database.DataEntry;
import com.nucleocore.db.database.Modification;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Stack;

public class ProducerHandler {
    private KafkaProducer producer;
    private Stack<DataEntry> entry = new Stack<DataEntry>();

    public ProducerHandler(String bootstrap) {
        producer = createProducer(bootstrap);
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
}