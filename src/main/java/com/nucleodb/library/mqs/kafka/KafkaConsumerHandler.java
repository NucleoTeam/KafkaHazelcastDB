package com.nucleodb.library.mqs.kafka;

import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class KafkaConsumerHandler extends ConsumerHandler{
  private static Logger logger = Logger.getLogger(KafkaConsumerHandler.class.getName());
  private KafkaConsumer consumer = null;
  private ConnectionHandler connectionHandler = null;

  private Thread kafkaConsumingThread = null;

  public KafkaConsumerHandler(MQSSettings settings, String servers, String groupName, String table) {
    super(settings, table);

    createTopics();

    logger.info(servers + " using group id " + groupName);
    this.consumer = createConsumer(servers, groupName);

    this.subscribe(new String[]{table.toLowerCase()});
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
  public void start(){
    kafkaConsumingThread = new Thread(this);
    kafkaConsumingThread.start();
    super.start();
  }


  private Map<TopicPartition, Long> startupMap = null;

  private boolean initialLoad() {
    Set<TopicPartition> partitions = getConsumer().assignment();
    if (startupMap == null || partitions.size() > startupMap.size()) {
      if (partitions == null) return false;
      if (partitions.size() != 36) return false;
      Map<TopicPartition, Long> tmp = getConsumer().endOffsets(partitions);
      startupMap = tmp;
    }
    return startupMap.size() == startupMap.entrySet().stream().filter(s -> getConsumer().position(s.getKey()) >= s.getValue()).count();
  }

  public Map<TopicPartition, OffsetAndMetadata> createCommitMap(String tableName, Map<Integer, OffsetAndMetadata> offsetMap){
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
    offsetMap.entrySet().forEach(e->{
      offsetAndMetadataMap.put(new TopicPartition(tableName, e.getKey()), e.getValue());
    });
    return offsetAndMetadataMap;
  }

  @Override
  public void run() {
    boolean connectionType = this.getConnectionHandler() != null;
    boolean databaseType = this.getDatabase() != null;
    boolean saveConnection = connectionType && this.getConnectionHandler().getConfig().isSaveChanges();
    boolean saveDatabase = databaseType && this.getDatabase().getConfig().isSaveChanges();
    if (databaseType) {
      super.setStartupLoadCount(getDatabase().getStartupLoadCount());
    }
    if (connectionType) {
      super.setStartupLoadCount(getConnectionHandler().getStartupLoadCount());
    }
    consumer.commitAsync();

    Map<Integer, OffsetAndMetadata> offsetMetaMap = new HashMap<>();
    try {
      do {
        ConsumerRecords<Integer, String> rs = getConsumer().poll(Duration.ofMillis(1000));
        if (rs.count() > 0) {
          rs.iterator().forEachRemaining(action -> {
            if(getStartupPhaseConsume().get()) getStartupLoadCount().incrementAndGet();
            String pop = action.value();
            //System.out.println("Change added to queue.");
            getQueue().add(pop);
            getLeftToRead().incrementAndGet();
            synchronized (getQueue()) {
              getQueue().notifyAll();
            }
            if (saveConnection)
              this.getConnectionHandler().getPartitionOffsets().put(action.partition(), action.offset());
            if (saveDatabase)
              this.getDatabase().getPartitionOffsets().put(action.partition(), action.offset());
            offsetMetaMap.put(action.partition(), new OffsetAndMetadata(action.offset()));
          });
          consumer.commitAsync();
        }
        while(getStartupPhaseConsume().get() && getLeftToRead().get()>50000){
          Thread.sleep(1000);
        }
        //logger.info("consumed: "+leftToRead.get());
        if (getStartupPhaseConsume().get() && initialLoad()) {
          getStartupPhaseConsume().set(false);
          if(getStartupLoadCount().get()==0){
            if(connectionType){
              getConnectionHandler().getStartupPhase().set(false);
              new Thread(() -> getConnectionHandler().startup()).start();
            }
            if(databaseType){
              getDatabase().getStartupPhase().set(false);
              new Thread(() -> getDatabase().startup()).start();
            }
          }
        }
      } while (!Thread.interrupted());
      logger.info("Consumer interrupted " + (databaseType ? this.getDatabase().getConfig().getTable() : "connections"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private org.apache.kafka.clients.consumer.KafkaConsumer createConsumer(String bootstrap, String groupName) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    //System.out.println(groupName);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ((KafkaSettings) getSettings()).getOffsetReset());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    org.apache.kafka.clients.consumer.KafkaConsumer consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(props);

    return consumer;
  }

  public void subscribe(String[] topics) {
    System.out.println("Subscribed to topic " + Arrays.asList(topics).toString());
    consumer.subscribe(Arrays.asList(topics));
  }

  public KafkaConsumer getConsumer() {
    return this.consumer;
  }

  public void setConsumer(KafkaConsumer consumer) {
    this.consumer = consumer;
  }

  public ConnectionHandler getConnectionHandler() {
    return connectionHandler;
  }

  public void setConnectionHandler(ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
  }

}