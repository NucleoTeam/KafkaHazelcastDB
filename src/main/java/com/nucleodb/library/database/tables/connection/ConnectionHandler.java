package com.nucleodb.library.database.tables.connection;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nucleodb.library.NucleoDB;
import com.nucleodb.library.database.modifications.ConnectionCreate;
import com.nucleodb.library.database.modifications.ConnectionDelete;
import com.nucleodb.library.database.modifications.ConnectionUpdate;
import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.tables.annotation.Conn;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.modifications.Modification;
import com.nucleodb.library.database.utils.InvalidConnectionException;
import com.nucleodb.library.database.utils.JsonOperations;
import com.nucleodb.library.database.utils.ObjectFileReader;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.TreeSetExt;
import com.nucleodb.library.database.utils.Utils;
import com.nucleodb.library.event.ConnectionEventListener;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.kafka.KafkaConsumerHandler;
import com.nucleodb.library.mqs.kafka.KafkaProducerHandler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import javax.xml.crypto.Data;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
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

public class ConnectionHandler<C extends Connection> implements Serializable{
  private static Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
  private static final long serialVersionUID = 1;
  private String name;
  @JsonIgnore
  private transient Map<String, Set<C>> connections = new TreeMap<>();
  @JsonIgnore
  private transient Map<String, Set<C>> connectionsReverse = new TreeMap<>();
  private transient Set<String> connectionFields;
  @JsonIgnore
  private transient Map<String, C> connectionByUUID = new TreeMap<>();
  private Map<Integer, Long> partitionOffsets = new TreeMap<>();
  private String consumerId = UUID.randomUUID().toString();
  private Set<C> allConnections = new TreeSetExt<>();
  @JsonIgnore
  private transient ExportHandler exportHandler;
  @JsonIgnore
  private transient NucleoDB nucleoDB;
  private transient ConnectionConfig config;
  @JsonIgnore
  private transient ProducerHandler producer = null;
  @JsonIgnore
  private transient ConsumerHandler consumer = null;
  private transient Queue<ModificationQueueItem> modqueue = Queues.newLinkedBlockingQueue();
  @JsonIgnore
  private transient boolean inStartup = true;
  @JsonIgnore
  private transient Cache<String, Consumer<C>> consumers = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .softValues()
      .expireAfterWrite(5, TimeUnit.SECONDS)
      .removalListener(e -> {
        if (e.getCause().name().equals("EXPIRED")) {
          logger.info("EXPIRED " + e.getKey());
          System.exit(1);
          new Thread(() -> ((Consumer<C>) e.getValue()).accept(null)).start();
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

  public ConnectionHandler(NucleoDB nucleoDB, ConnectionConfig config) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.nucleoDB = nucleoDB;
    this.config = config;

    this.connectionFields = Arrays.stream(config.getConnectionClass().getDeclaredFields()).map(f -> f.getName()).collect(Collectors.toSet());

    if (config.isLoadSaved()) {
      loadSavedData();
    }

    if (config.isRead()) {
      new Thread(new ModQueueHandler(this)).start();
      this.consume();
    }
    if (config.isWrite()) {
      producer = this.config.getMqsConfiguration().createProducerHandler(this.config.getSettingsMap());
    }
    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }
    if (config.isJsonExport()) {
      exportHandler = new ExportHandler(this);
      new Thread(exportHandler).start();
    }
  }

  public ConnectionHandler(NucleoDB nucleoDB, String bootstrap) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.nucleoDB = nucleoDB;
    this.config = new ConnectionConfig();

    this.connectionFields = Arrays.stream(config.getConnectionClass().getDeclaredFields()).map(f -> f.getName()).collect(Collectors.toSet());

    if (config.isLoadSaved()) {
      loadSavedData();
    }

    if (config.isRead()) {
      new Thread(new ModQueueHandler(this)).start();
      this.consume();
    }
    if (config.isWrite()) {
      producer = this.config.getMqsConfiguration().createProducerHandler(this.config.getSettingsMap());
    }
    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }

    if (config.isJsonExport()) {
      exportHandler = new ExportHandler(this);
      new Thread(exportHandler).start();
    }
  }


  public void loadSavedData() {
    if (new File(config.getConnectionFileName()).exists()) {
      try {
        ConnectionHandler tmpConnections = (ConnectionHandler) new ObjectFileReader().readObjectFromFile(config.getConnectionFileName());
        tmpConnections.allConnections.forEach(c -> this.addConnection((C)c));
        this.changed = tmpConnections.changed;
        this.consumerId = tmpConnections.getConsumerId();
        this.partitionOffsets = tmpConnections.partitionOffsets;
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  public Set<C> getByFrom(DataEntry de, ConnectionProjection<C> connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection<C>();
    }
    Set<C> tmp = connections.get(de.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream(), this.getConfig().getConnectionClass());
    }
    return new TreeSetExt<>();
  }

  public Set<C> getByFromAndTo(DataEntry from, DataEntry to, ConnectionProjection<C> connectionProjection) {
    if(connectionProjection == null){
      connectionProjection = new ConnectionProjection<C>();
    }
    Set<C> tmp = connections.get(from.getKey() + to.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream(), this.getConfig().getConnectionClass());
    }
    return new TreeSetExt<>();
  }

  public Set<C> get(ConnectionProjection<C> connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection<C>();
    }
    return connectionProjection.process(allConnections.stream(), this.getConfig().getConnectionClass());
  }

  public Set<C> getReverseByTo(DataEntry to, ConnectionProjection<C> connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection<C>();
    }
    Set<C> tmp = connectionsReverse.get(to.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream(), this.getConfig().getConnectionClass());
    }
    return new TreeSetExt<>();
  }

  public Set<C> getReverseByFromAndTo(DataEntry de, DataEntry toDe, ConnectionProjection<C> connectionProjection) {
    if(connectionProjection ==null){
      connectionProjection = new ConnectionProjection<C>();
    }
    Set<C> tmp = connectionsReverse.get(de.getKey() + toDe.getKey());
    if (tmp != null) {
      return connectionProjection.process(tmp.stream(), this.getConfig().getConnectionClass());
    }
    return new TreeSetExt<>();
  }

  private void putConnectionInKey(String key, C connection) {
    if (!connections.containsKey(key)) {
      connections.put(key, new TreeSetExt<>());
    }
    connections.get(key).add(connection);
  }

  private void putReverseConnectionInKey(String key, C connection) {
    if (!connectionsReverse.containsKey(key)) {
      connectionsReverse.put(key, new TreeSetExt<>());
    }
    connectionsReverse.get(key).add(connection);
  }

  private void addConnection(C connection) {
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

  private void removeByKey(String key, C connection) {
    if (connections.containsKey(key)) {
      connections.get(key).remove(connection);
      if (connections.get(key).size() == 0) {
        connections.remove(key);
      }
    }
  }

  private void removeReverseByKey(String key, C connection) {
    if (connectionsReverse.containsKey(key)) {
      connectionsReverse.get(key).remove(connection);
      if (connectionsReverse.get(key).size() == 0) {
        connectionsReverse.remove(key);
      }
    }
  }

  private void removeConnection(C connection) {
    synchronized (connections) {
      connectionByUUID.remove(connection.getUuid());
      this.removeByKey(connection.getFromKey(), connection);
      this.removeByKey(connection.getFromKey() + connection.getToKey(), connection);
      this.removeReverseByKey(connection.getToKey(), connection);
      this.removeReverseByKey(connection.getToKey() + connection.getFromKey(), connection);
      allConnections.remove(connection);
    }
  }

  public void consume() throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    if (this.config.getSettingsMap() != null) {
      this.consumer = this.config
          .getMqsConfiguration()
          .createConsumerHandler(this.config.getSettingsMap());
      this.consumer.setConnectionHandler(this);
      this.consumer.start(36);
      this.config.getSettingsMap().put("consumerHandler", this.consumer);
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

  public Map<String, Set<C>> getConnections() {
    return connections;
  }

  public void setConnections(Map<String, Set<C>> connections) {
    this.connections = connections;
  }

  public Set<C> getAllConnections() {
    return allConnections;
  }

  public void setAllConnections(Set<C> allConnections) {
    this.allConnections = allConnections;
  }

  public boolean deleteAndForget(C connection) throws IOException {
    return deleteInternalConsumer(connection, null);
  }

  public void deleteAsync(C connection, Consumer<C> consumer) throws IOException {
    deleteInternalConsumer(connection, consumer);
  }

  public boolean deleteSync(C connection) throws IOException, InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    deleteInternalConsumer(connection, (c) -> {
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return true;
  }

  public List<String> invalidConnection(C c) {
    List<String> invalids = new LinkedList<>();
    if (c.getFromKey() == null) invalids.add("[FromKey]");
    if (c.getToKey() == null) invalids.add("[ToKey]");
    return invalids;
  }

  public boolean saveAndForget(C connection) throws InvalidConnectionException {
    List<String> invalids = this.invalidConnection(connection);
    if (invalids.size() > 0) {
      throw new InvalidConnectionException(invalids.stream().collect(Collectors.joining(", ")));
    }
    return saveInternalConsumer(connection, null);
  }

  public boolean saveAsync(C connection, Consumer<C> consumer) throws InvalidConnectionException {
    List<String> invalids = this.invalidConnection(connection);
    if (invalids.size() > 0) {
      throw new InvalidConnectionException(invalids.stream().collect(Collectors.joining(", ")));
    }
    return saveInternalConsumer(connection, consumer);
  }

  public boolean saveSync(C connection) throws InvalidConnectionException, InterruptedException {
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


  private boolean deleteInternalConsumer(C connection, Consumer<C> consumer) throws IOException {
    String changeUUID = UUID.randomUUID().toString();
    if (consumer != null) {
      consumers.put(changeUUID, consumer);
    }
    if (deleteInternal(connection, changeUUID)) {
      return true;
    }
    return false;
  }

  private boolean saveInternalConsumer(C connection, Consumer<C> consumer) {
    String changeUUID = UUID.randomUUID().toString();
    if (consumer != null) {
      consumers.put(changeUUID, consumer);
    }
    if (saveInternal(connection, changeUUID)) {
      return true;
    }
    return false;
  }

  private boolean saveInternalConsumerSync(C connection) throws InterruptedException {
    if (!this.config.isWrite()) {
      return false;
    }
    String changeUUID = UUID.randomUUID().toString();
    CountDownLatch countDownLatch = new CountDownLatch(1);
    consumers.put(changeUUID, (conn) -> countDownLatch.countDown());
    if (saveInternal(connection, changeUUID)) {
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

  private boolean deleteInternal(C connection, String changeUUID) throws IOException {
    if (allConnections.contains(connection)) {
      connection.versionIncrease();
      ConnectionDelete deleteEntry = new ConnectionDelete(changeUUID, connection);
      producer.push(deleteEntry.getUuid(), deleteEntry.getVersion(), deleteEntry, null);
      return true;
    }
    return false;
  }

  JsonNode fromObject(Object o){
    try {
      return Serializer.getObjectMapper().getOmNonType().readTree(
          Serializer.getObjectMapper().getOm().writeValueAsString(o)
      );
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  Object fromJsonNode(JsonNode o, Class type){
    try {
      return Serializer.getObjectMapper().getOm().readValue(
          Serializer.getObjectMapper().getOmNonType().writeValueAsString(o),
          type
      );
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private boolean saveInternal(C connection, String changeUUID) {
    if (!allConnections.contains(connection)) {
      ConnectionCreate createEntry = new ConnectionCreate(changeUUID, connection);
      producer.push(createEntry.getUuid(), createEntry.getVersion(), createEntry, null);
      return true;
    } else {
      connection.versionIncrease();
      List<JsonOperations> changes = null;
      C oldConnection = connectionByUUID.get(connection.getUuid());
      if(oldConnection==null){
        try {
          consumerResponse(null, changeUUID);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
      JsonPatch patch = JsonDiff.asJsonPatch(fromObject(oldConnection), fromObject(connection));
      try {
        String json = Serializer.getObjectMapper().getOmNonType().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOmNonType().readValue(json, List.class);
        if (changes != null && changes.size() > 0) {
          ConnectionUpdate updateEntry = new ConnectionUpdate(connection.getVersion(), json, changeUUID, connection.getUuid(), connection.getRequest());
          producer.push(updateEntry.getUuid(), updateEntry.getVersion(), updateEntry, null);
          return true;
        }else{
          try {
            consumerResponse(oldConnection, changeUUID);
          } catch (ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      } catch (JsonProcessingException e) {
        e.printStackTrace();
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

  private void consumerResponse(C connection, String changeUUID) throws ExecutionException {
    try {
      getNucleoDB().getLockManager().releaseLock(this.config.getLabel(), connection.getUuid(), connection.getRequest());
      if(changeUUID!=null) {
        Consumer<C> connectionConsumer = consumers.getIfPresent(changeUUID);
        if (connectionConsumer != null) {
          new Thread(() -> connectionConsumer.accept(connection)).start();
          consumers.invalidate(changeUUID);
        }
      }
    } catch (CacheLoader.InvalidCacheLoadException e) {
      e.printStackTrace();
    }
    this.changed = new Date().getTime();
  }

  private void triggerEvent(Modify modify, C connection) {
    ConnectionEventListener eventListener = config.getEventListener();
    if(eventListener!=null) {
      if(modify instanceof ConnectionCreate){
        new Thread(()->eventListener.create((ConnectionCreate)modify, connection)).start();
      }else if(modify instanceof ConnectionDelete){
        new Thread(()->eventListener.delete((ConnectionDelete)modify, connection)).start();
      }else if(modify instanceof ConnectionUpdate){
        new Thread(()->eventListener.update((ConnectionUpdate)modify, connection)).start();
      }
    }
  }

  public void modify(Modification mod, Object modification) throws ExecutionException {
    switch (mod) {
      case CONNECTIONCREATE:
        ConnectionCreate c = (ConnectionCreate) modification;

        //logger.info("Create statement called");
        if (c != null) {
          itemProcessed();
          if (this.config.getReadToTime() != null && c.getDate().isAfter(this.config.getReadToTime())) {
            //logger.info("Create after target db date");
            consumerResponse(null, c.getChangeUUID());
            return;
          }
          try {
            if (connectionByUUID.containsKey(c.getUuid())) {
              //logger.info("Ignore already saved change.");
              return; // ignore this create
            }


            C connection = (C) Serializer.getObjectMapper().getOm().readValue(c.getConnectionData(), getConfig().getConnectionClass());

            this.addConnection(connection);
            //Serializer.log("Connection added to db");
            //Serializer.log(consumers.asMap().keySet());
            this.changed = new Date().getTime();
            consumerResponse(connection, c.getChangeUUID());
            triggerEvent(c, connection);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case CONNECTIONDELETE:
        try {
          ConnectionDelete d = (ConnectionDelete) modification;
          //logger.info("Delete statement called");
          if (d != null) {
            itemProcessed();
            if (this.config.getReadToTime() != null && d.getTime().isAfter(this.config.getReadToTime())) {
              consumerResponse(null, d.getChangeUUID());
              //logger.info("Delete after target db date");
              //System.exit(1);
              return;
            }
            C conn = connectionByUUID.get(d.getUuid());
            if (conn != null) {
              if (conn.getVersion() >= d.getVersion()) {
                //logger.info("Ignore already saved change.");
                //System.exit(1);
                return; // ignore change
              }
              if (conn.getVersion() + 1 != d.getVersion()) {
                itemRequeue();
                //Serializer.log("Version not ready!");
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              } else {
                //logger.info("Deleted");
                this.removeConnection(conn);
                conn.setRequest(d.getRequest());
                //logger.info("removed from db");
                deletedEntries.add(d.getUuid());
                //logger.info("Added to deleted entries");
                this.changed = new Date().getTime();
                consumerResponse(conn, d.getChangeUUID());
                triggerEvent(d, conn);
                long items = itemsToBeCleaned.incrementAndGet();
                if (!startupPhase.get() && items > 100) {
                  itemsToBeCleaned.set(0L);
                  System.gc();
                }
              }
            } else {
              if (deletedEntries.contains(d.getUuid())) {
                //logger.info("already deleted conn "+d.getUuid());
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
            consumerResponse(null, u.getChangeUUID());
            return;
          }
          try {
            C conn = connectionByUUID.get(u.getUuid());
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

                C connectionTmp = (C) fromJsonNode(
                  u.getChangesPatch().apply(fromObject(conn)),
                  config.getConnectionClass()
                );
                conn.setVersion(u.getVersion());
                conn.setModified(u.getTime());
                conn.setMetadata(connectionTmp.getMetadata());

                Arrays.stream(config.getConnectionClass().getDeclaredFields()).map(f->f.getName()).filter(fName->!connectionFields.contains(fName)).forEach(
                    fName-> {
                      try {
                        PropertyDescriptor propertyDescriptor = new PropertyDescriptor(fName, config.getConnectionClass());
                        Object obj = propertyDescriptor.getReadMethod().invoke(connectionTmp);
                        propertyDescriptor.getWriteMethod().invoke(conn, obj);
                      } catch (IllegalAccessException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                      } catch (InvocationTargetException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                      } catch (IntrospectionException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                      }
                    }
                );
                this.changed = new Date().getTime();
                conn.setRequest(u.getRequest());
                consumerResponse(conn, u.getChangeUUID());
                triggerEvent(u, conn);
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

  public Map<String, C> getConnectionByUUID() {
    return connectionByUUID;
  }

  public void setConnectionByUUID(Map<String, C> connectionByUUID) {
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

  public Cache<String, Consumer<C>> getConsumers() {
    return consumers;
  }

  public void setConsumers(Cache<String, Consumer<C>> consumers) {
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

  public ExportHandler getExportHandler() {
    return exportHandler;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
