package com.nucleocore.library.database.tables.connection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.modifications.ConnectionCreate;
import com.nucleocore.library.database.modifications.ConnectionDelete;
import com.nucleocore.library.database.modifications.ConnectionUpdate;
import com.nucleocore.library.database.tables.table.DataEntryProjection;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.modifications.Modification;
import com.nucleocore.library.database.utils.InvalidConnectionException;
import com.nucleocore.library.database.utils.JsonOperations;
import com.nucleocore.library.database.utils.ObjectFileReader;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.TreeSetExt;
import com.nucleocore.library.database.utils.Utils;
import com.nucleocore.library.kafkaLedger.ConsumerHandler;
import com.nucleocore.library.kafkaLedger.ProducerHandler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ConnectionHandler implements Serializable{
  private static Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
  private static final long serialVersionUID = 1;
  @JsonIgnore
  private transient Map<String, Set<Connection>> connections = new TreeMap<>();
  @JsonIgnore
  private transient Map<String, Set<Connection>> connectionsReverse = new TreeMap<>();
  @JsonIgnore
  private transient Map<String, Connection> connectionByUUID = new TreeMap<>();
  private Map<Integer, Long> partitionOffsets = new TreeMap<>();
  private String consumerId = UUID.randomUUID().toString();
  private Set<Connection> allConnections = new TreeSetExt<>();
  @JsonIgnore
  private transient NucleoDB nucleoDB;
  private ConnectionConfig config;
  @JsonIgnore
  private transient ProducerHandler producer = null;
  @JsonIgnore
  private transient ConsumerHandler consumer = null;
  private transient Queue<ModificationQueueItem> modqueue = Queues.newLinkedBlockingQueue();
  @JsonIgnore
  private transient boolean inStartup = true;
  @JsonIgnore
  private transient Cache<String, Consumer<Connection>> consumers = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .softValues()
      .expireAfterWrite(5, TimeUnit.SECONDS)
      .removalListener(e -> {
        if (e.getCause().name().equals("EXPIRED")) {
          logger.info("EXPIRED " + e.getKey());
          System.exit(1);
          new Thread(() -> ((Consumer<Connection>) e.getValue()).accept(null)).start();
          ;
        }
      })
      .build();
  private Set<String> deletedEntries = Sets.newTreeSet();
  private long changed = new Date().getTime();
  private transient AtomicInteger leftInModQueue = new AtomicInteger(0);
  private transient AtomicInteger startupLoadCount = new AtomicInteger(0);
  private transient AtomicBoolean startupPhase = new AtomicBoolean(true);
  private AtomicLong itemsToBeCleaned = new AtomicLong(0L);

  public ConnectionHandler(NucleoDB nucleoDB, ConnectionConfig config) {
    this.nucleoDB = nucleoDB;
    this.config = config;
    if (config.isWrite()) {
      createTopics();
    }
    if (config.isLoadSaved()) {
      loadSavedData();
    }

    if (config.isRead()) {
      logger.info("Connecting to " + config.getBootstrap());
      new Thread(new ModQueueHandler(this)).start();
      this.consume();
    }
    if (config.isWrite()) {
      logger.info("Producing to " + config.getBootstrap());
      producer = new ProducerHandler(config.getBootstrap(), config.getTopic(), this.consumerId);
    }
    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }
    if (config.isJsonExport()) {
      new Thread(new ExportHandler(this)).start();
    }
  }

  public ConnectionHandler(NucleoDB nucleoDB, String bootstrap) {
    this.nucleoDB = nucleoDB;
    this.config = new ConnectionConfig();
    this.config.setBootstrap(bootstrap);
    // startup

    if (config.isWrite()) {
      createTopics();
    }

    if (config.isLoadSaved()) {
      loadSavedData();
    }

    if (config.isRead()) {
      System.out.println("Connecting to " + config.getBootstrap());
      new Thread(new ModQueueHandler(this)).start();
      this.consume();
    }
    if (config.isWrite()) {
      System.out.println("Producing to " + config.getBootstrap());
      producer = new ProducerHandler(config.getBootstrap(), config.getTopic(), this.consumerId);
    }
    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }

    if (config.isJsonExport()) {
      new Thread(new ExportHandler(this)).start();
    }
  }


  public void loadSavedData() {
    if (new File("./data/"+ getConfig().getTopic()+".dat").exists()) {
      try {
        ConnectionHandler tmpConnections = (ConnectionHandler) new ObjectFileReader().readObjectFromFile("./data/"+getConfig().getTopic()+".dat");
        tmpConnections.allConnections.forEach(c -> this.addConnection(c));
        this.changed = tmpConnections.changed;
        this.consumerId = tmpConnections.getConsumerId();
        this.partitionOffsets = tmpConnections.partitionOffsets;
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void createTopics() {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrap());
    AdminClient client = KafkaAdminClient.create(props);

    String topic = config.getTopic();
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

  public Set<Connection> getByFrom(DataEntry de, ConnectionProjection connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection();
    }
    Set<Connection> tmp = connections.get(de.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream()).map(c->c.copy(config.getConnectionClass())).map(Connection.class::cast).collect(Collectors.toSet());
    }
    return new TreeSetExt<>();
  }

  public Set<Connection> getByFromAndTo(DataEntry from, DataEntry to, ConnectionProjection connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection();
    }
    Set<Connection> tmp = connections.get(from.getKey() + to.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream()).map(c->c.copy(config.getConnectionClass())).map(Connection.class::cast).collect(Collectors.toSet());
    }
    return new TreeSetExt<>();
  }

  public Set<Connection> get(ConnectionProjection connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection();
    }
    return connectionProjection.process(allConnections.stream()).map(c->c.copy(config.getConnectionClass())).map(Connection.class::cast).collect(Collectors.toSet());
  }

  public Set<Connection> getReverseByTo(DataEntry to, ConnectionProjection connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection();
    }
    Set<Connection> tmp = connectionsReverse.get(to.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream()).map(c->c.copy(config.getConnectionClass())).map(Connection.class::cast).collect(Collectors.toSet());
    }
    return new TreeSetExt<>();
  }

  public Set<Connection> getReverseByFromAndTo(DataEntry de, DataEntry toDe, ConnectionProjection connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection();
    }
    Set<Connection> tmp = connectionsReverse.get(de.getKey() + toDe.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream()).map(c->c.copy(config.getConnectionClass())).map(Connection.class::cast).collect(Collectors.toSet());
    }
    return new TreeSetExt<>();
  }

  private void putConnectionInKey(String key, Connection connection) {
    if (!connections.containsKey(key)) {
      connections.put(key, new TreeSetExt<>());
    }
    connections.get(key).add(connection);
  }

  private void putReverseConnectionInKey(String key, Connection connection) {
    if (!connectionsReverse.containsKey(key)) {
      connectionsReverse.put(key, new TreeSetExt<>());
    }
    connectionsReverse.get(key).add(connection);
  }

  private void addConnection(Connection connection) {
    synchronized (connections) {
      connection.connectionHandler = this;
      connectionByUUID.put(connection.getUuid(), connection);
      this.putConnectionInKey(connection.getFromKey(), connection);
      this.putConnectionInKey(connection.getFromKey() + connection.getToKey(), connection);
      this.putReverseConnectionInKey(connection.getToKey(), connection);
      this.putReverseConnectionInKey(connection.getToKey() + connection.getFromKey(), connection);
      allConnections.add(connection);
    }
  }

  private void removeByKey(String key, Connection connection) {
    if (connections.containsKey(key)) {
      connections.get(key).remove(connection);
      if (connections.get(key).size() == 0) {
        connections.remove(key);
      }
    }
  }

  private void removeReverseByKey(String key, Connection connection) {
    if (connectionsReverse.containsKey(key)) {
      connectionsReverse.get(key).remove(connection);
      if (connectionsReverse.get(key).size() == 0) {
        connectionsReverse.remove(key);
      }
    }
  }

  private void removeConnection(Connection connection) {
    synchronized (connections) {
      connectionByUUID.remove(connection.getUuid());
      this.removeByKey(connection.getFromKey(), connection);
      this.removeByKey(connection.getFromKey() + connection.getToKey(), connection);
      this.removeReverseByKey(connection.getToKey(), connection);
      this.removeReverseByKey(connection.getToKey() + connection.getFromKey(), connection);
      allConnections.remove(connection);
    }
  }

  public void consume() {
    if (this.config.getBootstrap() != null) {
      System.out.println(this.consumerId + " connecting to: " + this.config.getBootstrap());
      this.consumer = new ConsumerHandler(this.config.getBootstrap(), this.consumerId, this, getConfig().getTopic());
    }
  }

  public void removeConnectionFrom(String key) {
    if (connections.containsKey(key)) {
      allConnections.removeAll(connections.get(key));
      connections.remove(key);
    }
  }

  public void removeConnectionTo(DataEntry dataEntry) {
    allConnections.stream().filter(conn -> conn.getToKey().equals(dataEntry.getKey())).collect(Collectors.toSet()).forEach((c) -> removeConnection(c));
  }

  public Map<String, Set<Connection>> getConnections() {
    return connections;
  }

  public void setConnections(Map<String, Set<Connection>> connections) {
    this.connections = connections;
  }

  public Set<Connection> getAllConnections() {
    return allConnections;
  }

  public void setAllConnections(Set<Connection> allConnections) {
    this.allConnections = allConnections;
  }

  public boolean deleteAndForget(Connection connection) throws IOException {
    return deleteInternalConsumer(connection, null);
  }

  public void deleteAsync(Connection connection, Consumer<Connection> consumer) throws IOException {
    deleteInternalConsumer(connection, consumer);
  }

  public boolean deleteSync(Connection connection) throws IOException, InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    deleteInternalConsumer(connection, (c) -> {
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return true;
  }

  public List<String> invalidConnection(Connection c) {
    List<String> invalids = new LinkedList<>();
    if (c.getFromKey() == null) invalids.add("[FromKey]");
    if (c.getToKey() == null) invalids.add("[ToKey]");
    return invalids;
  }

  public boolean saveAndForget(Connection connection) throws InvalidConnectionException {
    List<String> invalids = this.invalidConnection(connection);
    if (invalids.size() > 0) {
      throw new InvalidConnectionException(invalids.stream().collect(Collectors.joining(", ")));
    }
    return saveInternalConsumer(connection, null);
  }

  public boolean saveAsync(Connection connection, Consumer<Connection> consumer) throws InvalidConnectionException {
    List<String> invalids = this.invalidConnection(connection);
    if (invalids.size() > 0) {
      throw new InvalidConnectionException(invalids.stream().collect(Collectors.joining(", ")));
    }
    return saveInternalConsumer(connection, consumer);
  }

  public boolean saveSync(Connection connection) throws InvalidConnectionException, InterruptedException {
    List<String> invalids = this.invalidConnection(connection);
    if (invalids.size() > 0) {
      throw new InvalidConnectionException(invalids.stream().collect(Collectors.joining(", ")));
    }
    CountDownLatch countDownLatch = new CountDownLatch(1);
    boolean v = saveInternalConsumer(connection, (c) -> {
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return true;
  }


  private boolean deleteInternalConsumer(Connection connection, Consumer<Connection> consumer) throws IOException {
    String changeUUID = UUID.randomUUID().toString();
    if (consumer != null) {
      consumers.put(changeUUID, consumer);
    }
    if (deleteInternal(connection, changeUUID)) {
      return true;
    }
    return false;
  }

  private boolean saveInternalConsumer(Connection connection, Consumer<Connection> consumer) {
    String changeUUID = UUID.randomUUID().toString();
    if (consumer != null) {
      consumers.put(changeUUID, consumer);
    }
    if (saveInternal(connection, changeUUID)) {
      return true;
    }
    return false;
  }

  private boolean saveInternalConsumerSync(Connection connection) throws InterruptedException {
    if (!this.config.isWrite()) {
      return false;
    }
    String changeUUID = UUID.randomUUID().toString();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    consumers.put(changeUUID, (conn) -> countDownLatch.countDown());
    if (saveInternalSync(connection, changeUUID)) {
      countDownLatch.await();
      return true;
    }
    return false;
  }

  public void startup() {
    inStartup = false;
    if (this.config.getStartupRun() != null) {
      this.config.getStartupRun().run(this);
    }
  }

  private boolean deleteInternal(Connection connection, String changeUUID) throws IOException {
    if (allConnections.contains(connection)) {
      connection.versionIncrease();
      ConnectionDelete deleteEntry = new ConnectionDelete(changeUUID, connection);
      producer.push(deleteEntry.getUuid(), deleteEntry.getVersion(), deleteEntry, null);
      return true;
    }
    return false;
  }

  private boolean saveInternal(Connection connection, String changeUUID) {
    if (!allConnections.contains(connection)) {
      ConnectionCreate createEntry = new ConnectionCreate(changeUUID, connection);
      producer.push(createEntry.getUuid(), createEntry.getVersion(), createEntry, null);
      return true;
    } else {
      connection.versionIncrease();
      List<JsonOperations> changes = null;
      Connection oldConnection = connectionByUUID.get(connection.getUuid());
      JsonPatch patch = JsonDiff.asJsonPatch(Serializer.getObjectMapper().getOm().valueToTree(oldConnection), Serializer.getObjectMapper().getOm().valueToTree(connection));
      try {
        String json = Serializer.getObjectMapper().getOm().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOm().readValue(json, List.class);
        //Serializer.log(json);
        if (changes != null && changes.size() > 0) {
          ConnectionUpdate updateEntry = new ConnectionUpdate(connection.getVersion(), json, changeUUID, connection.getUuid());
          producer.push(updateEntry.getUuid(), updateEntry.getVersion(), updateEntry, null);
          return true;
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  private boolean saveInternalSync(Connection connection, String changeUUID) throws InterruptedException {
    if (!allConnections.contains(connection)) {
      ConnectionCreate createEntry = new ConnectionCreate(changeUUID, connection);
      producer.push(createEntry.getUuid(), createEntry.getVersion(), createEntry, null);
      return true;
    } else {
      connection.versionIncrease();
      List<JsonOperations> changes = null;
      Connection oldConnection = connectionByUUID.get(connection.getUuid());
      JsonPatch patch = JsonDiff.asJsonPatch(Serializer.getObjectMapper().getOm().valueToTree(oldConnection), Serializer.getObjectMapper().getOm().valueToTree(connection));
      try {
        String json = Serializer.getObjectMapper().getOm().writeValueAsString(patch);
        //Serializer.log(json);
        changes = Serializer.getObjectMapper().getOm().readValue(json, List.class);
        if (changes != null && changes.size() > 0) {
          ConnectionUpdate updateEntry = new ConnectionUpdate(connection.getVersion(), json, changeUUID, connection.getUuid());
          CountDownLatch countDownLatch = new CountDownLatch(1);
          producer.push(updateEntry.getUuid(), updateEntry.getVersion(), updateEntry, null);
          countDownLatch.await();
          return true;
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

    }
    return false;
  }

  private void itemProcessed() {
    if (this.startupPhase.get()) {
      int left = this.startupLoadCount.decrementAndGet();
      if (left != 0 && left % 10000 == 0) logger.info("Startup connection items waiting to process: " + left);
      if (!this.getConsumer().getStartupPhaseConsume().get() && left <= 0) {
        this.startupPhase.set(false);
        System.gc();
        new Thread(() -> this.startup()).start();
      }
    }
  }

  private void itemRequeue() {
    if (this.startupPhase.get()) this.startupLoadCount.incrementAndGet();
  }

  public void modify(Modification mod, Object modification) {
    switch (mod) {
      case CONNECTIONCREATE:
        ConnectionCreate c = (ConnectionCreate) modification;

        logger.info("Create statement called");
        if (c != null) {
          itemProcessed();
          if (this.config.getReadToTime() != null && c.getDate().isAfter(this.config.getReadToTime())) {
            logger.info("Create after target db date");
            return;
          }
          try {
            if (connectionByUUID.containsKey(c.getUuid())) {
              logger.info("Ignore already saved change.");
              return; // ignore this create
            }

            Connection connection = (Connection) getConfig().getConnectionClass().getDeclaredConstructor(ConnectionCreate.class).newInstance(c);
            Serializer.log(connection);
            this.addConnection(connection);
            Serializer.log("Connection added to db");
            Serializer.log(consumers.asMap().keySet());
            this.changed = new Date().getTime();
            if (c.getChangeUUID() != null) {
              Consumer<Connection> consumer = consumers.getIfPresent(c.getChangeUUID());
              if (consumer != null) {
                new Thread(() -> {
                  consumers.invalidate(c.getChangeUUID());
                  consumer.accept(connection);
                }).start();
              }
            }

          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case CONNECTIONDELETE:
        try {
          ConnectionDelete d = (ConnectionDelete) modification;
          logger.info("Delete statement called");
          if (d != null) {
            itemProcessed();
            if (this.config.getReadToTime() != null && d.getTime().isAfter(this.config.getReadToTime())) {
              logger.info("Delete after target db date");
              //System.exit(1);
              return;
            }
            Connection conn = connectionByUUID.get(d.getUuid());
            if (conn != null) {
              if (conn.getVersion() >= d.getVersion()) {
                logger.info("Ignore already saved change.");
                //System.exit(1);
                return; // ignore change
              }
              if (conn.getVersion() + 1 != d.getVersion()) {
                itemRequeue();
                Serializer.log("Version not ready!");
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              } else {
                logger.info("Deleted");
                this.removeConnection(conn);
                logger.info("removed from db");
                deletedEntries.add(d.getUuid());
                logger.info("Added to deleted entries");
                this.changed = new Date().getTime();
                if (d.getChangeUUID() != null) {
                  Consumer<Connection> consumer = consumers.getIfPresent(d.getChangeUUID());
                  if (consumer != null) {
                    new Thread(() -> {
                      consumers.invalidate(d.getChangeUUID());
                      consumer.accept(conn);
                    }).start();
                  }
                }
                long items = itemsToBeCleaned.incrementAndGet();
                if (!startupPhase.get() && items > 100) {
                  itemsToBeCleaned.set(0L);
                  System.gc();
                }
              }
            } else {
              if (deletedEntries.contains(d.getUuid())) {
                logger.info("already deleted conn "+d.getUuid());
                //System.exit(1);
                return;
              } else {
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;
      case CONNECTIONUPDATE:
        ConnectionUpdate u = (ConnectionUpdate) modification;
        //if(!startupPhase.get()) logger.info("Update statement called");
        if (u != null) {
          itemProcessed();
          if (this.config.getReadToTime() != null && u.getTime().isAfter(this.config.getReadToTime())) {
            //System.out.println("Update after target db date");
            return;
          }
          try {
            Connection conn = connectionByUUID.get(u.getUuid());
            if (conn != null) {
              if (conn.getVersion() >= u.getVersion()) {
                //logger.info("Ignore already saved change.");
                return; // ignore change
              }
              if (conn.getVersion() + 1 != u.getVersion()) {
                //Serializer.log("Version not ready!");
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              } else {

                Connection connectionTmp = (Connection) Utils.getOm().readValue(
                    u.getChangesPatch().apply(Utils.getOm().valueToTree(conn)).toString(),
                    config.getConnectionClass()
                );
                conn.setVersion(u.getVersion());
                conn.setMetadata(connectionTmp.getMetadata());
                this.changed = new Date().getTime();
                if (u.getChangeUUID() != null) {
                  Consumer<Connection> consumer = consumers.getIfPresent(u.getChangeUUID());
                  if (consumer != null) {
                    new Thread(() -> {
                      consumers.invalidate(u.getChangeUUID());
                      consumer.accept(conn);
                    }).start();
                  }
                }
                long items = itemsToBeCleaned.incrementAndGet();
                if (!startupPhase.get() && items > 100) {
                  itemsToBeCleaned.set(0L);
                  System.gc();
                }
              }
            } else {
              itemRequeue();
              modqueue.add(new ModificationQueueItem(mod, modification));
              leftInModQueue.incrementAndGet();
              synchronized (modqueue) {
                modqueue.notifyAll();
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
    }
  }

  public Map<String, Connection> getConnectionByUUID() {
    return connectionByUUID;
  }

  public void setConnectionByUUID(Map<String, Connection> connectionByUUID) {
    this.connectionByUUID = connectionByUUID;
  }

  public Map<Integer, Long> getPartitionOffsets() {
    return partitionOffsets;
  }

  public void setPartitionOffsets(Map<Integer, Long> partitionOffsets) {
    this.partitionOffsets = partitionOffsets;
  }

  public NucleoDB getNucleoDB() {
    return nucleoDB;
  }

  public void setNucleoDB(NucleoDB nucleoDB) {
    this.nucleoDB = nucleoDB;
  }

  public ConnectionConfig getConfig() {
    return config;
  }

  public void setConfig(ConnectionConfig config) {
    this.config = config;
  }

  public ProducerHandler getProducer() {
    return producer;
  }

  public void setProducer(ProducerHandler producer) {
    this.producer = producer;
  }

  public ConsumerHandler getConsumer() {
    return consumer;
  }

  public void setConsumer(ConsumerHandler consumer) {
    this.consumer = consumer;
  }

  public Queue<ModificationQueueItem> getModqueue() {
    return modqueue;
  }

  public void setModqueue(Queue<ModificationQueueItem> modqueue) {
    this.modqueue = modqueue;
  }

  public Cache<String, Consumer<Connection>> getConsumers() {
    return consumers;
  }

  public void setConsumers(Cache<String, Consumer<Connection>> consumers) {
    this.consumers = consumers;
  }

  public long getChanged() {
    return changed;
  }

  public void setChanged(long changed) {
    this.changed = changed;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public AtomicInteger getLeftInModQueue() {
    return leftInModQueue;
  }

  public AtomicInteger getStartupLoadCount() {
    return startupLoadCount;
  }

  public void setStartupLoadCount(AtomicInteger startupLoadCount) {
    this.startupLoadCount = startupLoadCount;
  }

  public void setLeftInModQueue(AtomicInteger leftInModQueue) {
    this.leftInModQueue = leftInModQueue;
  }

  public AtomicBoolean getStartupPhase() {
    return startupPhase;
  }

  public void setStartupPhase(AtomicBoolean startupPhase) {
    this.startupPhase = startupPhase;
  }
}
