package com.nucleocore.library.kafkaLedger;

import com.google.common.collect.Queues;
import com.nucleocore.library.database.modifications.Modify;
import com.nucleocore.library.database.tables.connection.ConnectionHandler;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.modifications.Modification;
import com.nucleocore.library.database.utils.Serializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ConsumerHandler implements Runnable{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());
  private KafkaConsumer consumer = null;
  private DataTable database = null;
  private ConnectionHandler connectionHandler = null;
  private String table;
  int startupItems = -1;
  private Queue<String> queue = Queues.newLinkedBlockingQueue();
  private transient AtomicInteger leftToRead = new AtomicInteger(0);
  private AtomicInteger startupLoadCount;
  private AtomicBoolean startupPhaseConsume = new AtomicBoolean(true);

  public ConsumerHandler(String bootstrap, String groupName, DataTable database, String table) {
    this.database = database;
    this.table = table;
    logger.info(bootstrap + " using group id " + groupName);
    this.consumer = createConsumer(bootstrap, groupName);

    this.subscribe(new String[]{table});

    for (int x = 0; x < 36; x++)
      new Thread(new QueueHandler()).start();

    new Thread(this).start();
  }

  public ConsumerHandler(String bootstrap, String groupName, ConnectionHandler connectionHandler, String table) {
    this.connectionHandler = connectionHandler;
    this.table = table;

    logger.info(bootstrap + " using group id " + groupName);
    this.consumer = createConsumer(bootstrap, groupName);

    this.subscribe(new String[]{table});
    //logger.info(table);

    for (int x = 0; x < 36; x++)
      new Thread(new QueueHandler()).start();

    new Thread(this).start();

  }



  class QueueHandler implements Runnable{
    @Override
    public void run() {
      boolean connectionType = getConnectionHandler() != null;
      boolean databaseType = getDatabase() != null;
      while (true) {
        String entry = null;
        while (!queue.isEmpty() && (entry = queue.poll()) != null) {
          leftToRead.decrementAndGet();
          try {
            if (databaseType) {
              String type = entry.substring(0, 6);
              String data = entry.substring(6);
              Modification mod = Modification.get(type);
              if (mod != null) {
                database.modify(mod, Serializer.getObjectMapper().getOm().readValue(data, mod.getModification()));
              }
            } else if (connectionType) {
              String type = entry.substring(0, 16);
              String data = entry.substring(16);
              Modification mod = Modification.get(type);
              if (mod != null) {
                try {
                  Modify modifiedEntry = (Modify) Serializer.getObjectMapper().getOm().readValue(data, mod.getModification());
                  connectionHandler.modify(mod, modifiedEntry);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        if (queue.isEmpty()) {
          try {
            synchronized (queue) {
              if (leftToRead.get() == 0) queue.wait();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
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
    String tableName = "";
    if (databaseType) {
      startupLoadCount = getDatabase().getStartupLoadCount();
      tableName = getDatabase().getConfig().getTable();
    }
    if (connectionType) {
      startupLoadCount = getConnectionHandler().getStartupLoadCount();
      tableName = "connections";
    }
    consumer.commitAsync();

    Map<Integer, OffsetAndMetadata> offsetMetaMap = new HashMap<>();
    try {
      do {
        ConsumerRecords<Integer, String> rs = getConsumer().poll(Duration.ofMillis(1000));
        if (rs.count() > 0) {
          rs.iterator().forEachRemaining(action -> {
            if(startupPhaseConsume.get()) startupLoadCount.incrementAndGet();
            String pop = action.value();
            //System.out.println("Change added to queue.");
            queue.add(pop);
            leftToRead.incrementAndGet();
            synchronized (queue) {
              queue.notifyAll();
            }
            if (saveConnection)
              this.getConnectionHandler().getPartitionOffsets().put(action.partition(), action.offset());
            if (saveDatabase)
              this.getDatabase().getPartitionOffsets().put(action.partition(), action.offset());
            offsetMetaMap.put(action.partition(), new OffsetAndMetadata(action.offset()));
          });
          consumer.commitAsync();
        }
        while(startupPhaseConsume.get() && leftToRead.get()>50000){
          Thread.sleep(1000);
        }
        //logger.info("consumed: "+leftToRead.get());
        if (startupPhaseConsume.get() && initialLoad()) {
          startupPhaseConsume.set(false);
          if(startupLoadCount.get()==0){
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

  private KafkaConsumer createConsumer(String bootstrap, String groupName) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    //System.out.println(groupName);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    KafkaConsumer consumer = new KafkaConsumer(props);

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

  public DataTable getDatabase() {
    return database;
  }

  public void setDatabase(DataTable database) {
    this.database = database;
  }


  public ConnectionHandler getConnectionHandler() {
    return connectionHandler;
  }

  public void setConnectionHandler(ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
  }

  public AtomicBoolean getStartupPhaseConsume() {
    return startupPhaseConsume;
  }

  public void setStartupPhaseConsume(AtomicBoolean startupPhaseConsume) {
    this.startupPhaseConsume = startupPhaseConsume;
  }
}