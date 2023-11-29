package com.nucleocore.library.kafkaLedger;

import com.nucleocore.library.database.modifications.Modify;
import com.nucleocore.library.database.modifications.Update;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.utils.Serializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class ProducerHandler  {
    private static Logger logger = Logger.getLogger(DataTable.class.getName());
    private KafkaProducer producer;
    private String table;

    public ProducerHandler(String bootstrap, String table, String groupId) {
        producer = createProducer(bootstrap, groupId);
        this.table = table;
    }

    private KafkaProducer createProducer(String bootstrap, String groupId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 25);
//        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1500);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 200);
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 200);
        return new KafkaProducer(props);
    }
    public KafkaProducer getProducer() {
        return producer;
    }

    public void push(String key, long version, Modify modify, Callback callback){
        new Thread(()-> {
            try {
                ProducerRecord record = new ProducerRecord(
                    table,
                    key,
                    modify.getClass().getSimpleName() + Serializer.getObjectMapper().getOm().writeValueAsString(modify)
                );
                record.headers().add("version", Long.valueOf(version).toString().getBytes());

                getProducer().send(record, (e, ex) -> {
                    //logger.info("Published");
                    if (ex != null) {
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    if (callback != null) callback.onCompletion(e, ex);
                });
                Thread.currentThread().interrupt();
                //logger.info("produced");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}