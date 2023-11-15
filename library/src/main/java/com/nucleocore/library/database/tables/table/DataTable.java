package com.nucleocore.library.database.tables.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.modifications.Delete;
import com.nucleocore.library.database.modifications.Update;
import com.nucleocore.library.database.utils.JsonOperations;
import com.nucleocore.library.database.modifications.Modification;
import com.nucleocore.library.database.utils.ObjectFileReader;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.TreeSetExt;
import com.nucleocore.library.database.utils.index.Index;
import com.nucleocore.library.database.utils.index.TreeIndex;
import com.nucleocore.library.kafkaLedger.ConsumerHandler;
import com.nucleocore.library.kafkaLedger.ProducerHandler;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import com.github.fge.jsonpatch.JsonPatch;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DataTable implements Serializable{
  private static final long serialVersionUID = 1;
  @JsonIgnore
  private transient static Logger logger = Logger.getLogger(DataTable.class.getName());

  private List<DataEntry> entries = Lists.newArrayList();
  private DataTableConfig config;

  private Map<String, Index> indexes = new TreeMap<>();
  private Set<DataEntry> dataEntries = new TreeSet<>();
  private Map<String, DataEntry> keyToEntry = new TreeMap<>();

  private Map<Integer, Long> partitionOffsets = new TreeMap<>();
  private String consumerId = UUID.randomUUID().toString();

  private long changed = new Date().getTime();
  @JsonIgnore
  private transient Queue<Object[]> indexQueue = Queues.newArrayDeque();
  @JsonIgnore
  private transient ProducerHandler producer = null;
  @JsonIgnore
  private transient ConsumerHandler consumer = null;
  private transient Cache<String, Consumer<DataEntry>> consumers = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .softValues()
      .expireAfterWrite(5, TimeUnit.SECONDS)
      .removalListener(e->{
        if(e.getCause().name().equals("EXPIRED")){
          logger.info("EXPIRED");
          new Thread(() -> ((Consumer<DataEntry>)e.getValue()).accept(null)).start();;
        }
      })
      .build();
  @JsonIgnore
  private static Map<Modification, Set<Consumer<DataEntry>>> listeners = new TreeMap<>();
  @JsonIgnore
  private transient boolean unsavedIndexModifications = false;
  @JsonIgnore
  private transient int size = 0;
  @JsonIgnore
  private transient boolean buildIndex = true;
  @JsonIgnore
  private transient List<Field> fields;
  @JsonIgnore
  private transient boolean inStartup = true;


  public Object[] getIndex() {
    if (indexQueue.isEmpty())
      return null;
    return indexQueue.poll();
  }

  public void loadSavedData() {
    if (new File(config.getTableFileName()).exists()) {
      logger.info("reading " + config.getTableFileName());
      try {
        DataTable tmpTable = (DataTable) new ObjectFileReader().readObjectFromFile(config.getTableFileName());
        this.dataEntries = tmpTable.dataEntries;
        if (tmpTable.config != null)
          this.config.merge(tmpTable.config);
        this.changed = tmpTable.changed;
        this.entries = tmpTable.entries;
        this.consumerId = tmpTable.consumerId;
        this.indexes = tmpTable.indexes;
        this.partitionOffsets = tmpTable.partitionOffsets;
        this.keyToEntry = tmpTable.keyToEntry;
        this.entries.forEach(e->e.setTableName(this.config.getTable()));
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
      if (client.listTopics().names().get().stream().filter(x -> x.equals(config.getTable())).count() == 0) {
        try {
          final NewTopic newTopic = new NewTopic(config.getTable(), 36, (short) 3);
          newTopic.configs(new TreeMap<>(){{
            put(TopicConfig.RETENTION_MS_CONFIG, "-1");
            put(TopicConfig.RETENTION_MS_CONFIG, "-1");
            put(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
          }});
          final CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
          createTopicsResult.values().get(config.getTable()).get();
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
      if (client.listTopics().names().get().stream().filter(x -> x.equals(config.getTable())).count() == 0) {
        logger.info("topic not created");
        System.exit(-1);
      }else{
        logger.info("topic created/exists");
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    client.close();
  }

  public DataTable(DataTableConfig config) {
    this.config = config;

    if (config.isLoadSave()) {
      loadSavedData();
    }

    if (config.isWrite()) {
      createTopics();
    }

    Arrays.stream(config.getIndexes()).map(i -> new TreeIndex(i)).collect(Collectors.toSet()).forEach(i -> {
      if (!this.indexes.containsKey(i.getIndexedKey())) {
        this.indexes.put(i.getIndexedKey(), i);
      }
    });

    this.fields = new ArrayList<>(){{
      addAll(Arrays.asList(config.getClazz().getDeclaredFields()));
      addAll(Arrays.asList(config.getClazz().getSuperclass().getFields()));
    }};


    if (config.isRead()) {
      logger.info("Connecting to " + config.getBootstrap());
      new Thread(new ModQueueHandler(this)).start();
      this.consume();
      logger.info("Connected to  "+config.getBootstrap());
    }

    if (config.isWrite()) {
      logger.info("Producing to " + config.getBootstrap());
      producer = new ProducerHandler(config.getBootstrap(), config.getTable(), this.consumerId);
    }

    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }
    if (config.isJsonExport()) {
      new Thread(new ExportHandler(this)).start();
    }
  }

  public void consume() {
    if (this.config.getBootstrap() != null) {
      logger.info(this.config.getTable() + " with " + this.consumerId + " connecting to: " + this.config.getBootstrap());
      new ConsumerHandler(this.config.getBootstrap(), this.consumerId, this, this.config.getTable());
    }
  }

  public void exportTo(DataTable tb) {
    for (DataEntry de : this.entries) {
      //Serializer.log("INSERTING " + de.getKey());
      try {
        tb.saveSync(de);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void flush() {
    try {
      entries.clear();
    } catch (Exception e) {
      //e.printStackTrace();
    }
    consumers.cleanUp();
    listeners = new HashMap<>();
    System.gc();
  }

  public Set<DataEntry> in(String key, List<Object> values) {
    Set<DataEntry> tmp = new TreeSet<>();
    try {
      for (Object val : values) {
        Set<DataEntry> de = get(key, val);
        if (de != null) {
          tmp.addAll(de);
        }
      }
    } catch (ClassCastException ex) {
      ex.printStackTrace();
    }
    return tmp;
  }

  public void startup() {
    inStartup = false;
    if (this.config.getStartupRun() != null) {
      this.config.getStartupRun().run(this);
    }
  }

  private long counter = 0;
  private long lastReq = 0;

  public Set<DataEntry> createNewObject(Set<DataEntry> o) {
    try {
      Set<DataEntry> set = Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(o), new TypeReference<TreeSet<DataEntry>>(){
      });
      for (DataEntry dataEntry : set) {
        dataEntry.setData(Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(dataEntry.getData()), this.config.getClazz()));
      }
      return set;
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return new TreeSetExt<>();
  }

  DataEntry createNewObject(DataEntry o) {
    try {
      DataEntry dataEntry = Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(o), DataEntry.class);
      dataEntry.setData(Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(dataEntry.getData()), config.getClazz()));
      return dataEntry;
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
  }

  public Set<DataEntry> search(String key, Object searchObject) {
    try {
      return this.indexes.get(key).search(searchObject);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new TreeSet<>();
  }

  public Set<DataEntry> get(String key, Object value) {
    Set<DataEntry> entries = new TreeSet<>();
    try {
      if (key.equals("id")) {
        if(this.keyToEntry.containsKey(value)) {
          entries = new TreeSet<>(Arrays.asList(this.keyToEntry.get(value)));
        }
      } else if (this.indexes.containsKey(key)) {
        Set<DataEntry> tmpEntries = this.indexes.get(key).get(value);
        if (tmpEntries != null) {
          entries = tmpEntries;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return entries;
  }

  public Set<DataEntry> getNotEqual(String key, Object value) {
    try {
      Set<DataEntry> foundEntries = new TreeSetExt<>();
      if (key.equals("id")) {
        foundEntries = new TreeSet<>(Arrays.asList(this.keyToEntry.get(value)));
      } else {
        foundEntries = this.indexes.get(key).get(value);
      }
      Set<DataEntry> negation = new TreeSet<>(entries);
      negation.removeAll(foundEntries);
      if (entries != null) {
        return negation;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new TreeSet<>();
  }


  public DataEntry searchOne(String key, Object obj) {
    Set<DataEntry> entries = search(key, obj);
    if (entries != null && entries.size() > 0) {
      Optional<DataEntry> d = entries.stream().findFirst();
      if (d.isPresent())
        return d.get();
    }
    return null;
  }

  public int size() {
    return size;
  }

  public boolean deleteSync(DataEntry obj) throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    boolean v = deleteInternalConsumer(obj, (de)->{
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return v;
  }

  public boolean deleteAsync(DataEntry obj, Consumer<DataEntry> consumer) {
    return deleteInternalConsumer(obj, consumer);
  }

  public boolean delete(DataEntry entry) {
    return deleteInternalConsumer(entry, null);
  }

  private boolean deleteInternalConsumer(DataEntry entry, Consumer<DataEntry> consumer) {
    if (!this.config.isWrite()) {
      if (consumer != null) consumer.accept(null);
      return false;
    }
    DataEntry entryDelete = createNewObject(entry);
    String changeUUID = UUID.randomUUID().toString();
    entryDelete.versionIncrease();
    Delete delete = new Delete(changeUUID, entryDelete);
    if (consumer != null) {
      consumers.put(changeUUID, consumer);
    }
    producer.push(delete, null);
    return true;
  }

  public boolean saveSync(DataEntry obj) throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    boolean v = saveInternalConsumer(obj, (de) -> {
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return v;
  }

  public boolean saveAndForget(DataEntry entry) {
    return saveInternalConsumer(entry, null);
  }

  public boolean saveAsync(DataEntry entry, Consumer<DataEntry> consumer) {
    return saveInternalConsumer(entry, consumer);
  }

  private boolean saveInternalConsumer(DataEntry entry, Consumer<DataEntry> consumer) {
    if (!this.config.isWrite()) {
      if (consumer != null) consumer.accept(null);
      return false;
    }
    String changeUUID = UUID.randomUUID().toString();
    if (consumer != null) {
      consumers.put(changeUUID, consumer);
    }
    if (saveInternal(entry, changeUUID)) {
      return true;
    }
    return false;
  }

  private boolean saveInternal(DataEntry entry, String changeUUID) {
    if (!dataEntries.contains(entry)) {
      try {
        Create createEntry = new Create(changeUUID, entry);
        producer.push(createEntry, null);
        return true;
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      entry.versionIncrease();
      List<JsonOperations> changes = null;
      JsonPatch patch = JsonDiff.asJsonPatch(entry.getReference(), Serializer.getObjectMapper().getOm().valueToTree(entry.getData()));
      try {
        String json = Serializer.getObjectMapper().getOm().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOm().readValue(json, List.class);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      if (changes != null && changes.size() > 0) {
        Update updateEntry = new Update(changeUUID, entry, patch);
        producer.push(updateEntry, null);
        return true;
      }

    }
    return false;
  }

  private boolean saveInternalSync(DataEntry entry, String changeUUID) throws InterruptedException {
    if (!dataEntries.contains(entry)) {
      try {
        Create createEntry = new Create(changeUUID, entry);
        producer.push(createEntry, null);
        return true;
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      entry.versionIncrease();
      List<JsonOperations> changes = null;
      JsonPatch patch = JsonDiff.asJsonPatch(entry.getReference(), Serializer.getObjectMapper().getOm().valueToTree(entry.getData()));
      try {
        String json = Serializer.getObjectMapper().getOm().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOm().readValue(json, List.class);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      if (changes != null && changes.size() > 0) {
        Update updateEntry = new Update(changeUUID, entry, patch);
        producer.push(updateEntry, null);
        return true;
      }

    }
    return false;
  }




  Stack<ModificationQueueItem> modqueue = new Stack<>();

  public void modify(Modification mod, Object modification) {
    switch (mod) {
      case CREATE:
        Create c = (Create) modification;
        //System.out.println("Create statement called");
        if (c != null) {
          if (this.config.getReadToTime() != null && c.getTime().isAfter(this.config.getReadToTime())) {
            //System.out.println("Create after target db date");
            return;
          }
          try {
            if (keyToEntry.containsKey(c.getKey())) {
              //System.out.println("Ignore already saved change.");
              return; // ignore this create
            }
            DataEntry dataEntry = new DataEntry(c);

            dataEntry.setTableName(this.config.getTable());

            entries.add(dataEntry);
            dataEntries.add(dataEntry);
            keyToEntry.put(dataEntry.getKey(), dataEntry);

            for (Index i : this.indexes.values()) {
              try {
                i.add(dataEntry);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            }
            size++;
            consumerResponse(dataEntry, c.getChangeUUID());
            fireListeners(Modification.CREATE, dataEntry);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case DELETE:
        Delete d = (Delete) modification;
        //System.out.println("Delete statement called");
        if (d != null) {
          if (this.config.getReadToTime() != null && d.getTime().isAfter(this.config.getReadToTime())) {
            //System.out.println("Delete after target db date");
            return;
          }
          try {
            DataEntry de = keyToEntry.get(d.getKey());
            if (de != null) {
              if (de.getVersion() >= d.getVersion()) {
                //System.out.println("Ignore already saved change.");
                return; // ignore change
              }
              if (de.getVersion() + 1 != d.getVersion()) {
                //Serializer.log("Version not ready!");
                modqueue.add(new ModificationQueueItem(mod, modification));
                Serializer.log("ADDED TO MOD QUEUE");
              } else {
                entries.remove(de);
                keyToEntry.remove(de.getKey());
                dataEntries.remove(de);
                this.indexes.values().forEach(i -> i.delete(de));
                size--;
                consumerResponse(de, d.getChangeUUID());
                fireListeners(Modification.DELETE, de);
              }
            } else {
              modqueue.add(new ModificationQueueItem(mod, modification));
            }
          }catch (Exception e){
            e.printStackTrace();
          }
        }
        break;
      case UPDATE:
        Update u = (Update) modification;

        //System.out.println("Update statement called");
        if (u != null) {
          if (this.config.getReadToTime() != null && u.getTime().isAfter(this.config.getReadToTime())) {
            //System.out.println("Update after target db date");
            return;
          }
          try {
            DataEntry de = keyToEntry.get(u.getKey());
            if (de != null) {
              if (de.getVersion() >= u.getVersion()) {
                //System.out.println("Ignore already saved change.");
                return; // ignore change
              }
              if (de.getVersion() + 1 != u.getVersion()) {
                //Serializer.log("Version not ready!");
                modqueue.add(new ModificationQueueItem(mod, modification));
              } else {
                de.setReference(u.getChangesPatch().apply(de.getReference()));
                de.setVersion(u.getVersion());
                de.setData(Serializer.getObjectMapper().getOm().readValue(de.getReference().toString(), de.getData().getClass()));
                //System.out.println(Serializer.getObjectMapper().getOm().writeValueAsString(de.getData()));
                u.getOperations().forEach(op -> {
                  switch (op.getOp()) {
                    case "replace":
                    case "add":
                    case "copy":
                      try {
                        Index index = this.indexes.get(op.getPath());
                        if (index != null) {
                          index.modify(de);
                        }
                      } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      }
                      break;
                    case "move":
                      break;
                    case "remove":
                      Index index = this.indexes.get(op.getPath());
                      if (index != null) index.delete(de);
                      break;
                  }
                });
                this.changed = new Date().getTime();
                consumerResponse(de, u.getChangeUUID());
                fireListeners(Modification.UPDATE, de);
              }
            } else {
              modqueue.add(new ModificationQueueItem(mod, modification));
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
    }
  }

  private void consumerResponse(DataEntry dataEntry, String changeUUID) throws ExecutionException {
    try {
      Consumer<DataEntry> dataEntryConsumer = consumers.getIfPresent(changeUUID);
      if (dataEntryConsumer != null) {
        new Thread(() -> dataEntryConsumer.accept(dataEntry)).start();
        consumers.invalidate(changeUUID);
      }
    }catch (CacheLoader.InvalidCacheLoadException e){}
    this.changed = new Date().getTime();
  }

  public void fireListeners(Modification m, DataEntry data) {
    if (listeners.containsKey(m)) {
      listeners.get(m).forEach(method -> {
        try {
          method.accept(data);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }
  }

  public void multiImport(DataEntry newEntry) throws InterruptedException {
    this.saveSync(newEntry);
  }


  @JsonIgnore
  transient List<Thread> threads = new ArrayList<>();


  public void addListener(Modification m, Consumer<DataEntry> method) {
    if (!listeners.containsKey(m)) {
      listeners.put(m, new HashSet<>());
    }
    listeners.get(m).add(method);
  }

  public List<DataEntry> getEntries() {
    return entries;
  }

  public void setEntries(List<DataEntry> entries) {
    this.entries = entries;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public boolean isBuildIndex() {
    return buildIndex;
  }

  public void setBuildIndex(boolean buildIndex) {
    this.buildIndex = buildIndex;
  }

  public boolean isUnsavedIndexModifications() {
    return unsavedIndexModifications;
  }

  public void setUnsavedIndexModifications(boolean unsavedIndexModifications) {
    this.unsavedIndexModifications = unsavedIndexModifications;
  }

  public Map<String, Index> getIndexes() {
    return indexes;
  }

  public void setIndexes(Map<String, Index> indexes) {
    this.indexes = indexes;
  }

  public Set<DataEntry> getDataEntries() {
    return dataEntries;
  }

  public void setDataEntries(Set<DataEntry> dataEntries) {
    this.dataEntries = dataEntries;
  }

  public Map<String, DataEntry> getKeyToEntry() {
    return keyToEntry;
  }

  public void setKeyToEntry(Map<String, DataEntry> keyToEntry) {
    this.keyToEntry = keyToEntry;
  }

  public long getChanged() {
    return changed;
  }

  public void setChanged(long changed) {
    this.changed = changed;
  }

  public Queue<Object[]> getIndexQueue() {
    return indexQueue;
  }

  public void setIndexQueue(Queue<Object[]> indexQueue) {
    this.indexQueue = indexQueue;
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

  public Cache<String, Consumer<DataEntry>> getConsumers() {
    return consumers;
  }

  public void setConsumers(Cache<String, Consumer<DataEntry>> consumers) {
    this.consumers = consumers;
  }

  public Map<Modification, Set<Consumer<DataEntry>>> getListeners() {
    return listeners;
  }

  public void setListeners(Map<Modification, Set<Consumer<DataEntry>>> listeners) {
    this.listeners = listeners;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public boolean isInStartup() {
    return inStartup;
  }

  public void setInStartup(boolean inStartup) {
    this.inStartup = inStartup;
  }

  public long getCounter() {
    return counter;
  }

  public void setCounter(long counter) {
    this.counter = counter;
  }

  public long getLastReq() {
    return lastReq;
  }

  public void setLastReq(long lastReq) {
    this.lastReq = lastReq;
  }

  public Stack<ModificationQueueItem> getModqueue() {
    return modqueue;
  }

  public void setModqueue(Stack<ModificationQueueItem> modqueue) {
    this.modqueue = modqueue;
  }

  public List<Thread> getThreads() {
    return threads;
  }

  public void setThreads(List<Thread> threads) {
    this.threads = threads;
  }

  public Map<Integer, Long> getPartitionOffsets() {
    return partitionOffsets;
  }

  public void setPartitionOffsets(Map<Integer, Long> partitionOffsets) {
    this.partitionOffsets = partitionOffsets;
  }

  public DataTableConfig getConfig() {
    return config;
  }

  public void setConfig(DataTableConfig config) {
    this.config = config;
  }

  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }
}
