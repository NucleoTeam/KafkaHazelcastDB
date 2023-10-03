package com.nucleocore.library.database.tables;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.modifications.ConnectionCreate;
import com.nucleocore.library.database.modifications.ConnectionDelete;
import com.nucleocore.library.database.modifications.ConnectionUpdate;
import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.modifications.Delete;
import com.nucleocore.library.database.modifications.Update;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.modifications.Modification;
import com.nucleocore.library.database.utils.JsonOperations;
import com.nucleocore.library.database.utils.ObjectFileReader;
import com.nucleocore.library.database.utils.ObjectFileWriter;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.TreeSetExt;
import com.nucleocore.library.database.utils.index.Index;
import com.nucleocore.library.kafkaLedger.ConsumerHandler;
import com.nucleocore.library.kafkaLedger.ProducerHandler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionHandler implements Serializable{
  private static final long serialVersionUID = 1;
  private transient Map<String, Set<Connection>> connections = new TreeMap<>();
  private transient Map<String, Connection> connectionByUUID = new TreeMap<>();
  private Map<Integer, Long> partitionOffsets = new TreeMap<>();

  private Set<Connection> allConnections = new TreeSetExt<>();
  private transient NucleoDB nucleoDB;

  private ConnectionConfig config;

  private transient ProducerHandler producer = null;
  private transient ConsumerHandler consumer = null;

  Stack<ModificationQueueItem> modqueue = new Stack<>();
  private transient boolean inStartup = true;

  private transient Map<String, Consumer<Connection>> consumers = new TreeMap<>();

  private long changed = new Date().getTime();

  public ConnectionHandler(ConnectionConfig config) {
    this.config = config;
  }

  public ConnectionHandler(NucleoDB nucleoDB) {
    this.nucleoDB = nucleoDB;
    this.config = new ConnectionConfig();
    // startup
    if (config.isWrite()) {
      createTopics();
    }
    loadSavedData();

    if (config.isRead()) {
      System.out.println("Connecting to " + config.getBootstrap());
      new Thread(new ModQueueHandler()).start();
      this.consume();
    }
    if (config.isWrite()) {
      System.out.println("Producing to " + config.getBootstrap());
      producer = new ProducerHandler(config.getBootstrap(), "connections");
    }
    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }
    if (!config.isRead()) {
      if (this.config.getStartupRun() != null) {
        this.config.getStartupRun().run(this);
      }
    }
  }

  public void loadSavedData() {
    if (new File("./data/connections.dat").exists()) {
      try {
        ConnectionHandler tmpConnections = (ConnectionHandler) new ObjectFileReader().readObjectFromFile("./data/connections.dat");
        tmpConnections.allConnections.forEach(c->this.addConnection(c));
        this.changed = tmpConnections.changed;
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
    try {
      if (client.listTopics().names().get().stream().filter(x -> x.equals("connections")).count() == 0) {
        try {
          final NewTopic newTopic = new NewTopic("connections", 8, (short) 1);
          final CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
          createTopicsResult.values().get("connections").get();
        } catch (InterruptedException | ExecutionException e) {
          if (!(e.getCause() instanceof TopicExistsException)) {
            throw new RuntimeException(e.getMessage(), e);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    try {
      if (client.listTopics().names().get().stream().filter(x -> x.equals("connections")).count() == 0) {
        System.out.println("topic not created");
        System.exit(-1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    client.close();
  }

  public Set<Connection> get(DataEntry de){
    Set<Connection> tmp = connections.get(de.getKey());
    if(tmp!=null){
      return tmp.stream().map(c->c.clone()).collect(Collectors.toSet());
    }
    return null;
  }
  public Stream<Connection> getStream(DataEntry de){
    Set<Connection> tmp = get(de);
    if(tmp!=null){
      return tmp.stream();
    }
    return null;
  }
  public Set<Connection> getByLabel(DataEntry de, String label){
    Set<Connection> tmp = connections.get(de.getKey());
    if(tmp!=null) {
      return tmp.stream().filter(c->c.getLabel().equals(label)).map(c->c.clone()).collect(Collectors.toSet());
    }
    return null;
  }
  public Stream<Connection> getByLabelStream(DataEntry de, String label){
    Set<Connection> tmp = getByLabel(de, label);
    if(tmp!=null){
      return tmp.stream();
    }
    return null;
  }
  private void addConnection(Connection connection){
    connectionByUUID.put(connection.getUuid(), connection);
    String key = connection.getFromKey();
    if(!connections.containsKey(key)){
      connections.put(key, new TreeSetExt<>());
    }
    connections.get(key).add(connection);
    allConnections.add(connection);
  }
  private void removeConnection(Connection connection){
    connectionByUUID.remove(connection.getUuid());
    String key = connection.getFromKey();
    if(connections.containsKey(key)){
      allConnections.remove(connection);
      connections.get(key).remove(connection);
    }
  }
  public void consume() {
    if (this.config.getBootstrap() != null) {
      String consumer = UUID.randomUUID().toString();
      for (String kafkaBroker : this.config.getBootstrap().split(",")) {
        System.out.println( consumer + " connecting to: " + kafkaBroker);
        new ConsumerHandler(kafkaBroker, consumer, this, "connections");
      }
    }
  }
  public void removeConnectionFrom(String key){
    if(connections.containsKey(key)){
      allConnections.removeAll(connections.get(key));
      connections.remove(key);
    }
  }
  public void removeConnectionTo(DataEntry dataEntry){
    allConnections.stream().filter(conn->conn.getToKey().equals(dataEntry.getKey())).collect(Collectors.toSet()).forEach((c)->removeConnection(c));
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

  public boolean save(Connection connection) {
    return saveInternalConsumer(connection, null);
  }

  public boolean save(Connection connection, Consumer<Connection> consumer) {
    return saveInternalConsumer(connection, consumer);
  }

  private boolean saveInternalConsumer(Connection connection, Consumer<Connection> consumer) {
    String changeUUID = UUID.randomUUID().toString();
    if (saveInternal(connection, changeUUID)) {
      if (consumer != null) {
        consumers.put(changeUUID, consumer);
      }
      return true;
    }
    return false;
  }

  private boolean saveInternalConsumerSync(Connection connection, Consumer<Connection> consumer) throws InterruptedException {
    if (!this.config.isWrite()) {
      if (consumer != null) consumer.accept(null);
      return false;
    }
    String changeUUID = UUID.randomUUID().toString();
    if (saveInternalSync(connection, changeUUID)) {
      if (consumer != null) {
        consumers.put(changeUUID, consumer);
      }
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

  private synchronized boolean saveInternal(Connection connection, String changeUUID) {
    if (!allConnections.contains(connection)) {
      ConnectionCreate createEntry = new ConnectionCreate(changeUUID, connection);
      producer.push(createEntry, null);
      return true;
    } else {
      connection.versionIncrease();
      List<JsonOperations> changes = null;
      Connection oldConnection = connectionByUUID.get(connection.getUuid());
      JsonPatch patch = JsonDiff.asJsonPatch(Serializer.getObjectMapper().getOm().valueToTree(oldConnection), Serializer.getObjectMapper().getOm().valueToTree(connection));
      try {
        String json = Serializer.getObjectMapper().getOm().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOm().readValue(json, List.class);
        if (changes != null && changes.size() > 0) {
          ConnectionUpdate updateEntry = new ConnectionUpdate(json, changeUUID, connection.getUuid());
          producer.push(updateEntry, null);
          return true;
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  private synchronized boolean saveInternalSync(Connection connection, String changeUUID) throws InterruptedException {
    if (!allConnections.contains(connection)) {
      ConnectionCreate createEntry = new ConnectionCreate(changeUUID, connection);
      producer.push(createEntry, null);
      return true;
    } else {
      connection.versionIncrease();
      List<JsonOperations> changes = null;
      Connection oldConnection = connectionByUUID.get(connection.getUuid());
      JsonPatch patch = JsonDiff.asJsonPatch(Serializer.getObjectMapper().getOm().valueToTree(oldConnection), Serializer.getObjectMapper().getOm().valueToTree(connection));
      try {
        String json = Serializer.getObjectMapper().getOm().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOm().readValue(json, List.class);
        if (changes != null && changes.size() > 0) {
          ConnectionUpdate updateEntry = new ConnectionUpdate(json, changeUUID, connection.getUuid());
          CountDownLatch countDownLatch = new CountDownLatch(1);
          producer.push(updateEntry, (meta, e) -> {
            Serializer.log(meta.offset());
            if (e != null) {
              e.printStackTrace();
            }
            countDownLatch.countDown();
          });
          countDownLatch.await();
          return true;
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }

    }
    return false;
  }

  class ModificationQueueItem implements Serializable{
    private static final long serialVersionUID = 1;
    private Modification mod;
    private Object modification;

    public ModificationQueueItem(Modification mod, Object modification) {
      this.mod = mod;
      this.modification = modification;
    }

    public Modification getMod() {
      return mod;
    }

    public Object getModification() {
      return modification;
    }
  }

  class SaveHandler implements Runnable{
    ConnectionHandler connectionHandler;

    public SaveHandler(ConnectionHandler connectionHandler) {
      this.connectionHandler = connectionHandler;
    }

    @Override
    public void run() {
      long changedSaved = this.connectionHandler.changed;

      while (true) {
        try {
          if (changed > changedSaved) {
            System.out.println("Saved connections");
            new ObjectFileWriter().writeObjectToFile(this.connectionHandler, "./data/connections.dat");
            changedSaved = changed;
          }
          Thread.sleep(5000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
  class ModQueueHandler implements Runnable{
    @Override
    public void run() {
      ModificationQueueItem mqi;
      while (true) {
        while (!modqueue.isEmpty()) {
          System.out.println("SOMETHING FOUND");
          Serializer.log(modqueue);
          mqi = modqueue.pop();
          if (mqi != null) {
            modify(mqi.getMod(), mqi.getModification());
          }
          try {
            Thread.sleep(10);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        try {
          Thread.sleep(10);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
  public void modify(Modification mod, Object modification) {
    switch (mod) {
      case CONNECTIONCREATE:
        ConnectionCreate c = (ConnectionCreate) modification;
        //System.out.println("Create statement called");
        if (c != null) {
          if (this.config.getReadToTime() != null && c.getTime().isAfter(this.config.getReadToTime())) {
            System.out.println("Create after target db date");
            return;
          }
          try {
            if (connectionByUUID.containsKey(c.getConnection().getUuid())) {
              System.out.println("Ignore already saved change.");
              return; // ignore this create
            }

            this.addConnection(c.getConnection());
            this.changed = new Date().getTime();
            if (consumers.containsKey(c.getChangeUUID())) {
              new Thread(() -> consumers.remove(c.getChangeUUID()).accept(c.getConnection())).start();
            }

          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case CONNECTIONDELETE:
        ConnectionDelete d = (ConnectionDelete) modification;
        //System.out.println("Delete statement called");
        if (d != null) {
          if (this.config.getReadToTime() != null && d.getTime().isAfter(this.config.getReadToTime())) {
            System.out.println("Delete after target db date");
            return;
          }
          Connection conn = connectionByUUID.get(d.getUuid());
          if (conn != null) {
            if (conn.getVersion() >= d.getVersion()) {
              System.out.println("Ignore already saved change.");
              return; // ignore change
            }
            if (conn.getVersion() + 1 != d.getVersion()) {
              Serializer.log("Version not ready!");
              modqueue.add(new ModificationQueueItem(mod, modification));
            } else {
              this.removeConnection(conn);
              this.changed = new Date().getTime();
              if (consumers.containsKey(d.getChangeUUID())) {
                new Thread(() -> consumers.remove(d.getChangeUUID()).accept(conn)).start();
              }
            }
          } else {
            modqueue.add(new ModificationQueueItem(mod, modification));
          }
        }
        break;
      case CONNECTIONUPDATE:
        ConnectionUpdate u = (ConnectionUpdate) modification;

        System.out.println("Update statement called");
        if (u != null) {
          if (this.config.getReadToTime() != null && u.getTime().isAfter(this.config.getReadToTime())) {
            System.out.println("Update after target db date");
            return;
          }
          try {
            Connection conn = connectionByUUID.get(u.getUuid());
            if (conn != null) {
              if (conn.getVersion() >= u.getVersion()) {
                System.out.println("Ignore already saved change.");
                return; // ignore change
              }
              if (conn.getVersion() + 1 != u.getVersion()) {
                Serializer.log("Version not ready!");
                modqueue.add(new ModificationQueueItem(mod, modification));
              } else {
                Connection connectionTmp = Serializer.getObjectMapper().getOm().readValue(
                  u.getChangesPatch().apply(Serializer.getObjectMapper().getOm().valueToTree(conn)).toString(),
                  Connection.class
                );
                conn.setVersion(u.getVersion());
                conn.setMetadata(connectionTmp.getMetadata());
                conn.setLabel(connectionTmp.getLabel());
                conn.setToKey(connectionTmp.getToKey());
                conn.setToTable(connectionTmp.getToTable());
                this.changed = new Date().getTime();
                if (consumers.containsKey(u.getChangeUUID())) {
                  new Thread(() -> consumers.remove(u.getChangeUUID()).accept(conn)).start();
                }
              }
            } else {
              modqueue.add(new ModificationQueueItem(mod, modification));
            }
          } catch (Exception e) {

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

  public Stack<ModificationQueueItem> getModqueue() {
    return modqueue;
  }

  public void setModqueue(Stack<ModificationQueueItem> modqueue) {
    this.modqueue = modqueue;
  }

  public Map<String, Consumer<Connection>> getConsumers() {
    return consumers;
  }

  public void setConsumers(Map<String, Consumer<Connection>> consumers) {
    this.consumers = consumers;
  }

  public long getChanged() {
    return changed;
  }

  public void setChanged(long changed) {
    this.changed = changed;
  }
}
