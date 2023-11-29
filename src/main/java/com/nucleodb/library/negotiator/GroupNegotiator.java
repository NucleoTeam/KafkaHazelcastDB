package com.nucleodb.library.negotiator;

import com.github.f4b6a3.uuid.UuidCreator;
import com.nucleodb.library.NucleoDBNode;
import com.nucleodb.library.negotiator.decision.Arguer;
import com.nucleodb.library.negotiator.decision.hash.HashMeta;
import com.nucleodb.library.negotiator.decision.support.ArgumentKafkaMessage;
import com.nucleodb.library.negotiator.decision.support.ArgumentStep;
import com.nucleodb.library.utils.Serializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class GroupNegotiator implements Runnable {
  private Arguer arguer;
  private KafkaConsumer consumer = null;
  private KafkaProducer kafkaProducer = null;
  private String brokers;
  private String cluster;
  private NucleoDBNode nucleoDBNode;
  private String topic;

  public GroupNegotiator(NucleoDBNode nucleoDBNode, String cluster, String brokers) {
    this.nucleoDBNode = nucleoDBNode;
    this.cluster = cluster;
    this.brokers = brokers;
    this.kafkaProducer = createProducer();
    topic = "_"+cluster+"_arguments_";
    this.arguer = new Arguer(this.kafkaProducer, nucleoDBNode, topic);
    this.consumer = createConsumer();
    subscribe(topic);
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    //executor.execute(arguer);
    executor.execute(this);
  }

  public void initial(String hash, int replicas){
    ArgumentKafkaMessage message = new ArgumentKafkaMessage(ArgumentStep.NEW, new HashMeta(nucleoDBNode.getUniqueId(), hash, replicas, 0));
    final ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, UuidCreator.getTimeOrderedWithRandom().toString(), Serializer.write(message));
    kafkaProducer.send(record);
  }


  @Override
  public void run() {
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(50);
    consumer.commitAsync();

      do {
        ConsumerRecords<String, byte[]> rs = getConsumer().poll(Duration.ofMillis(20));
        if (!rs.isEmpty()) {
          Iterator<ConsumerRecord<String, byte[]>> iter = rs.iterator();
          while (iter.hasNext()) {
            byte[] data = iter.next().value();
            executor.submit(()->arguer.execute(Serializer.read(data))); // handle communications in async way.
          }
        }
        consumer.commitAsync();
      } while (!Thread.interrupted());

  }
  private KafkaConsumer createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, nucleoDBNode.getUniqueId());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024*8);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    KafkaConsumer consumer = new KafkaConsumer(props);

    return consumer;
  }
  private KafkaProducer createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.SEND_BUFFER_CONFIG, 1024*8);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer(props);
  }
  public void subscribe(String... topics){
    consumer.subscribe(Arrays.asList(topics));
  }

  public Arguer getArguer() {
    return arguer;
  }

  public void setArguer(Arguer arguer) {
    this.arguer = arguer;
  }

  public KafkaConsumer getConsumer() {
    return consumer;
  }

  public void setConsumer(KafkaConsumer consumer) {
    this.consumer = consumer;
  }

  public String getBrokers() {
    return brokers;
  }

  public void setBrokers(String brokers) {
    this.brokers = brokers;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }
}
