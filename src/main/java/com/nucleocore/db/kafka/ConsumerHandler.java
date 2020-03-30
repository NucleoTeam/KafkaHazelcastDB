package com.nucleocore.db.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.nucleocore.db.database.TableTemplate;
import com.nucleocore.db.database.index.combinedhash.HashIndex;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Modification;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ConsumerHandler implements Runnable {
    private KafkaConsumer consumer = null;
    private TableTemplate database;
    private Map<TopicPartition, Long> endMap = null;
    private String table;
    private boolean startup = false;

    ObjectMapper om = new ObjectMapper(){{
        this.enableDefaultTyping();
    }};

    public ConsumerHandler(String bootstrap, String groupName, TableTemplate database, String table) {
        this.database = database;
        this.table = table;
        this.consumer = createConsumer(bootstrap, groupName);

        this.subscribe(table.split(","));

        new Thread(this).start();
        for(int x=0;x<6;x++)
            new Thread(new QueueHandler()).start();
    }

    Queue<String> queue = Queues.newArrayDeque();
    class QueueHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                while (!queue.isEmpty()) {
                        String entry;
                        synchronized (queue) {
                            entry = queue.poll();
                        }
                        if(entry!=null) {
                            try {
                                String type = entry.substring(0, 6);
                                String data = entry.substring(6);
                                //System.out.println("Action: " + type + " data: "+data);
                                Modification mod = Modification.get(type);
                                if (mod != null) {
                                    database.modify(mod, om.readValue(data, mod.getModification()));
                                }
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                }
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean initialLoad(){
        if(this.endMap==null) {
            Set<TopicPartition> partitions = getConsumer().assignment();
            Map<TopicPartition, Long> tmp = getConsumer().endOffsets(partitions);
            if(tmp.size()!=0)
                this.endMap = tmp;
        }
        if(endMap!=null && getConsumer()!=null) {
            return endMap.size()==endMap.entrySet().stream().filter(s->getConsumer().position(s.getKey())==s.getValue()).count();
        }
        return false;
    }

    @Override
    public void run() {
        consumer.commitAsync();
        try {
            do {
                if(!startup && initialLoad()){
                    this.database.startup();
                    startup = true;
                }
                ConsumerRecords<Integer, String> rs = getConsumer().poll(Duration.ofMillis(5));


                if (!rs.isEmpty()) {
                    //System.out.println("RECEIVED DATA");
                    Iterator<ConsumerRecord<Integer, String>> iter = rs.iterator();
                    while (iter.hasNext()) {
                        String pop = iter.next().value();
                        queue.add(pop);
                    }
                }
                
                consumer.commitAsync();
            } while (!Thread.interrupted());
        }catch (Exception e){
            e.printStackTrace();
        }
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

    public TableTemplate getDatabase() {
        return database;
    }

    public void setDatabase(TableTemplate database) {
        this.database = database;
    }

    public boolean isStartup() {
        return startup;
    }

    public void setStartup(boolean startup) {
        this.startup = startup;
    }
}