package com.nucleocore.library.kafkaLedger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class ConsumerHandler implements Runnable{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());
  private KafkaConsumer consumer = null;
  private DataTable database = null;
  private ConnectionHandler connectionHandler = null;
  private String table;
  int startupItems = -1;

  public ConsumerHandler(String bootstrap, String groupName, DataTable database, String table) {
    this.database = database;
    this.table = table;
    logger.info(bootstrap + " using group id " + groupName);
    this.consumer = createConsumer(bootstrap, groupName);

    this.subscribe(new String[]{table});

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

    new Thread(new QueueHandler()).start();

    new Thread(this).start();

  }

  Queue<String> queue = Queues.newArrayDeque();


  class QueueHandler implements Runnable{
    @Override
    public void run() {
      boolean connectionType = getConnectionHandler() != null;
      boolean databaseType = getDatabase() != null;
      int startupReadItems = 0;
      boolean startupPhase = true;
      while (true) {
        String entry = null;
        while (!queue.isEmpty() && (entry = queue.poll()) != null) {
          if (startupPhase) {
            startupReadItems++;
          }
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
        if (startupPhase && startupItems != -1 && startupReadItems >= startupItems) {
          startupPhase = false;
          if (databaseType) {
            logger.info("Initialize loaded " + getDatabase().getConfig().getTable());
            new Thread(() -> database.startup()).start();
          }
          if (connectionType) {
            logger.info("Initialize loaded connections");
            new Thread(() -> connectionHandler.startup()).start();
          }
        } else if (!startupPhase && queue.isEmpty()) {
          try {
            synchronized (queue) {
              if(leftToRead.get()==0) queue.wait();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private Map<TopicPartition, Long> startupMap = null;

  private boolean initialLoad() {
    if (startupMap == null) {
      Set<TopicPartition> partitions = getConsumer().assignment();
      if (partitions == null) return false;
      if (partitions.size() != 36) return false;
      Map<TopicPartition, Long> tmp = getConsumer().endOffsets(partitions);
      startupMap = tmp;
    }
    return startupMap.size() == startupMap.entrySet().stream().filter(s -> getConsumer().position(s.getKey()) >= s.getValue()).count();
  }
  private transient AtomicInteger leftToRead = new AtomicInteger(0);

  @Override
  public void run() {
    boolean connectionType = this.getConnectionHandler() != null;
    boolean databaseType = this.getDatabase() != null;
    boolean saveConnection = connectionType && this.getConnectionHandler().getConfig().isSaveChanges();
    boolean saveDatabase = databaseType && this.getDatabase().getConfig().isSaveChanges();
    try {
      boolean startupPhase = true;
      AtomicInteger initialLoadCount = new AtomicInteger(0);
      do {
        ConsumerRecords<Integer, String> rs = getConsumer().poll(Duration.ofMillis(1000));
        if (rs.count() > 0) {
          boolean finalStartupPhase = startupPhase;
          rs.iterator().forEachRemaining(action -> {
            if (finalStartupPhase) initialLoadCount.getAndIncrement();
            String pop = action.value();
            //System.out.println("Change added to queue.");
            queue.add(pop);
            leftToRead.incrementAndGet();
            synchronized (queue) {
              queue.notify();
            }
            if (saveConnection)
              this.getConnectionHandler().getPartitionOffsets().put(action.partition(), action.offset());
            if (saveDatabase)
              this.getDatabase().getPartitionOffsets().put(action.partition(), action.offset());
          });
          consumer.commitAsync();
        }
        if (startupPhase) {
          if (databaseType) {
            logger.info("Initialize loading " + this.getDatabase().getConfig().getTable());
          } else {
            logger.info("Initialize loading connections");
          }
        }
        if (startupPhase && initialLoad()) {
          startupItems = initialLoadCount.intValue();
          if (startupItems == -1) startupItems = 0;
          startupPhase = false;
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
}