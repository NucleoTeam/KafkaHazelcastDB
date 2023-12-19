package com.nucleodb.library.mqs.kafka;

import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class KafkaProducerHandler extends ProducerHandler{
    private static Logger logger = Logger.getLogger(KafkaProducerHandler.class.getName());

    private KafkaProducer producer;


    public KafkaProducerHandler(MQSSettings settings, String servers, String table) {
        super(settings, table);
        createTopics();
        producer = createProducer(servers);
    }

    private KafkaProducer createProducer(String bootstrap) {
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

    public void createTopics() {
        Properties props = new Properties();
        KafkaSettings settings = (KafkaSettings) getSettings();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getServers());
        AdminClient client = KafkaAdminClient.create(props);

        String topic = getSettings().getTable().toLowerCase();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ListTopicsResult listTopicsResult = client.listTopics();
            listTopicsResult.names().whenComplete((names, f)->{
                if(f!=null){
                    f.printStackTrace();
                }
                if (names.stream().filter(name -> name.equals(topic)).count() == 0) {
                    logger.info(String.format("kafka topic not found for %s", topic));
                    final NewTopic newTopic = new NewTopic(topic, 36, (short) 3);
                    newTopic.configs(new TreeMap<>(){{
                        put(TopicConfig.RETENTION_MS_CONFIG, "-1");
                        put(TopicConfig.RETENTION_MS_CONFIG, "-1");
                        put(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
                    }});
                    CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
                    createTopicsResult.all().whenComplete((c, e) -> {
                        if (e != null) {
                            e.printStackTrace();
                        }
                        countDownLatch.countDown();
                    });
                }else{
                    countDownLatch.countDown();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            countDownLatch.await(60, TimeUnit.SECONDS);
            CountDownLatch countDownLatchCreatedCheck = new CountDownLatch(1);
            ListTopicsResult listTopicsResult = client.listTopics();
            listTopicsResult.names().whenComplete((names, f)->{
                if(f!=null){
                    f.printStackTrace();
                }
                if (names.stream().filter(name -> name.equals(topic)).count() == 0) {
                    logger.info("topic not created");
                    System.exit(-1);
                }
                countDownLatchCreatedCheck.countDown();
            });
            countDownLatchCreatedCheck.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        client.close();
    }

    @Override
    public void push(String key, long version, Modify modify, Callback callback){
        new Thread(()-> {
            try {
                ProducerRecord record = new ProducerRecord(
                    getTable().toLowerCase(),
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