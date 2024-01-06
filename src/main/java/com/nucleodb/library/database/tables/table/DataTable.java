package com.nucleodb.library.database.tables.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.modifications.Update;
import com.nucleodb.library.database.utils.JsonOperations;
import com.nucleodb.library.database.modifications.Modification;
import com.nucleodb.library.database.utils.ObjectFileReader;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.TreeSetExt;
import com.nucleodb.library.database.utils.Utils;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.index.IndexWrapper;
import com.nucleodb.library.database.utils.exceptions.InvalidIndexTypeException;
import com.nucleodb.library.event.DataTableEventListener;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;
import com.github.fge.jsonpatch.JsonPatch;
import java.beans.IntrospectionException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataTable implements Serializable{
  private static final long serialVersionUID = 1;
  @JsonIgnore
  private transient static Logger logger = Logger.getLogger(DataTable.class.getName());

  private Set<DataEntry> entries = new TreeSetExt<>();
  private transient DataTableConfig config;
  @JsonIgnore
  private transient Map<String, IndexWrapper<DataEntry>> indexes = new TreeMap<>();
  private Map<String, DataEntry> keyToEntry = new TreeMap<>();
  private Map<Integer, Long> partitionOffsets = new TreeMap<>();
  private long changed = new Date().getTime();
  private transient AtomicInteger leftInModQueue = new AtomicInteger(0);
  private Set<String> deletedEntries = new TreeSetExt<>();
  @JsonIgnore
  private transient Queue<Object[]> indexQueue = Queues.newLinkedBlockingQueue();
  @JsonIgnore
  private transient ProducerHandler producer = null;
  @JsonIgnore
  private transient ConsumerHandler consumer = null;
  private transient AtomicInteger startupLoadCount = new AtomicInteger(0);
  private transient AtomicBoolean startupPhase = new AtomicBoolean(true);

  private transient Cache<String, Consumer<DataEntry>> consumers = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .softValues()
      .expireAfterWrite(5, TimeUnit.SECONDS)
      .removalListener(e -> {
        if (e.getCause().name().equals("EXPIRED")) {
          //logger.info("EXPIRED" +e.getKey());
          System.exit(1);
          new Thread(() -> ((Consumer<DataEntry>) e.getValue()).accept(null)).start();
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
  private AtomicLong itemsToBeCleaned = new AtomicLong(0L);
  private transient Queue<ModificationQueueItem> modqueue = Queues.newLinkedBlockingQueue();

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
        if (tmpTable.config != null)
          this.config.merge(tmpTable.config);
        this.changed = tmpTable.changed;
        this.entries = tmpTable.entries;
        this.partitionOffsets = tmpTable.partitionOffsets;
        this.keyToEntry = tmpTable.keyToEntry;
        this.entries.forEach(e -> {
          for (IndexWrapper i : this.indexes.values()) {
            try {
              i.add(e);
            } catch (JsonProcessingException ex) {
              throw new RuntimeException(ex);
            } catch (InvalidIndexTypeException ex) {
              throw new RuntimeException(ex);
            }
          }
          e.setTableName(this.config.getTable());
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public DataTable(DataTableConfig config) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.config = config;

    config.getIndexes().forEach(i -> {
      try {
        IndexWrapper indexWrapper = i.getIndexType().getDeclaredConstructor(String.class).newInstance(i.getName());
        if (!this.indexes.containsKey(indexWrapper.getIndexedKey())) {
          this.indexes.put(indexWrapper.getIndexedKey(), indexWrapper);
        }
      } catch (InstantiationException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    });

    if (config.isLoadSave()) {
      loadSavedData();
    }

    this.fields = new ArrayList<>(){{
      addAll(Arrays.asList(config.getClazz().getDeclaredFields()));
      addAll(Arrays.asList(config.getClazz().getSuperclass().getFields()));
    }};


    if (config.isRead()) {
      new Thread(new ModQueueHandler(this)).start();
      this.consume();
    }

    if (config.isWrite()) {

      producer = this.config
          .getMqsConfiguration()
          .createProducerHandler(this.config.getSettingsMap());
    }

    if (config.isSaveChanges()) {
      new Thread(new SaveHandler(this)).start();
    }
    if (config.isJsonExport()) {
      new Thread(new ExportHandler(this)).start();
    }
  }

  public void consume() throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.consumer = this.getConfig()
        .getMqsConfiguration()
        .createConsumerHandler(this.config.getSettingsMap());
    this.consumer.setDatabase(this);
    this.consumer.start();
    this.config.getSettingsMap().put("consumerHandler", this.consumer);
  }

  public void exportTo(DataTable tb) throws IncorrectDataEntryObjectException {
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

  public Set<DataEntry> in(String key, List<Object> values, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<DataEntry> tmp = new TreeSet<>();
    try {
      for (Object val : values) {
        Set<DataEntry> de = get(key, val, dataEntryProjection);
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
      DataEntry dataEntry = Utils.getOm().readValue(
          Utils.getOm().writeValueAsString(o),
          DataEntry.class
      );
      dataEntry.setData(Utils.getOm().readValue(
          Utils.getOm().writeValueAsString(dataEntry.getData()),
          config.getClazz()
      ));
      return dataEntry;
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
  }

  public Set<DataEntry> handleIndexOperation(Object obj, DataEntryProjection dataEntryProjection, Function<Object, Set<DataEntry>> func) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<DataEntry> apply = func.apply(obj);
    if(apply!=null) {
      Stream<DataEntry> process = dataEntryProjection.process(apply.stream());

      if (dataEntryProjection.isWritable()) {
        return process.map(de -> (DataEntry) de.copy(this.getConfig().getDataEntryClass())).collect(Collectors.toSet());
      }
      return process.collect(Collectors.toSet());
    }
    return Sets.newTreeSet();
  }

  public Set<DataEntry> handleIndexStringOperation(String obj, DataEntryProjection dataEntryProjection, Function<String, Set<DataEntry>> func) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<DataEntry> apply = func.apply(obj);
    if(apply!=null) {
      Stream<DataEntry> process = dataEntryProjection.process(apply.stream());
      if (dataEntryProjection.isWritable()) {
        return process.map(de -> (DataEntry) de.copy(this.getConfig().getDataEntryClass())).collect(Collectors.toSet());
      }
      return process.collect(Collectors.toSet());
    }
    return Sets.newTreeSet();
  }

  public Set<DataEntry> search(String key, Object searchObject, DataEntryProjection dataEntryProjection) {
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(searchObject, dataEntryProjection, dataEntryIndexWrapper::contains);
  }

  public Set<DataEntry> startsWith(String key, String str, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexStringOperation(str, dataEntryProjection, dataEntryIndexWrapper::startsWith);
  }


  public Set<DataEntry> endsWith(String key, String str, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexStringOperation(str, dataEntryProjection, dataEntryIndexWrapper::endsWith);
  }

  public Set<DataEntry> greaterThan(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, dataEntryIndexWrapper::greaterThan);
  }

  public Set<DataEntry> greaterThanEqual(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, dataEntryIndexWrapper::greaterThanEqual);
  }

  public Set<DataEntry> lessThan(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, dataEntryIndexWrapper::lessThan);
  }

  public Set<DataEntry> lessThanEqual(String key, Object obj, DataEntryProjection dataEntryProjection) throws InvalidIndexTypeException {
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(obj, dataEntryProjection, dataEntryIndexWrapper::lessThanEqual);
  }

  public Set<DataEntry> get(String key, Object value) {
    return get(key, value, null);
  }

  public Set<DataEntry> get(String key, Object value, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    if (key.equals("id")) {
      if (value != null && this.keyToEntry.size() > 0) {
        DataEntry dataEntry = this.keyToEntry.get(value);
        if (dataEntry != null) {
          Set<DataEntry> entrySet = new TreeSet(Arrays.asList(dataEntry));
          Stream<DataEntry> process = dataEntryProjection.process(entrySet.stream());
          if (dataEntryProjection.isWritable()) {
            return process.map(de -> (DataEntry) de.copy(this.getConfig().getDataEntryClass())).collect(Collectors.toSet());
          }
          return process.collect(Collectors.toSet());
        }
      }
      return new TreeSet<>();
    }
    IndexWrapper<DataEntry> dataEntryIndexWrapper = this.indexes.get(key);
    return handleIndexOperation(value, dataEntryProjection, dataEntryIndexWrapper::get);
  }

  public Set<DataEntry> getNotEqual(String key, Object value, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    try {
      Set<DataEntry> foundEntries;
      if (key.equals("id")) {
        foundEntries = new TreeSet<>(Arrays.asList(this.keyToEntry.get(value)));
      } else {
        foundEntries = this.indexes.get(key).get(value);
      }
      Set<DataEntry> negation = new TreeSet<>(entries);
      negation.removeAll(foundEntries);
      if (entries != null) {
        Stream<DataEntry> process = dataEntryProjection.process(negation.stream());
        if (dataEntryProjection.isWritable()) {
          return process.map(de -> (DataEntry) de.copy(this.getConfig().getDataEntryClass())).collect(Collectors.toSet());
        } else {
          return process.collect(Collectors.toSet());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new TreeSet<>();
  }


  public DataEntry searchOne(String key, Object obj, DataEntryProjection dataEntryProjection) {
    if (dataEntryProjection == null) {
      dataEntryProjection = new DataEntryProjection();
    }
    Set<DataEntry> entries = search(key, obj, dataEntryProjection);
    if (entries != null && entries.size() > 0) {
      Optional<DataEntry> d = dataEntryProjection.process(entries.stream()).findFirst();
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
    boolean v = deleteInternalConsumer(obj, (de) -> {
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
    String changeUUID = UUID.randomUUID().toString();
    entry.versionIncrease();
    Delete delete = new Delete(changeUUID, entry);
    if (consumer != null) {
      consumers.put(changeUUID, consumer);
    }
    producer.push(delete.getKey(), delete.getVersion(), delete, null);
    return true;
  }


  public boolean saveSync(DataEntry entry) throws InterruptedException, IncorrectDataEntryObjectException {
    if (entry.getClass() != getConfig().getDataEntryClass()) {
      throw new IncorrectDataEntryObjectException("Entry " + entry.getKey() + " using incorrect data entry class for this table.");
    }
    CountDownLatch countDownLatch = new CountDownLatch(1);
    boolean v = saveInternalConsumer(entry, (de) -> {
      countDownLatch.countDown();
    });
    countDownLatch.await();
    return v;
  }

  public boolean saveAndForget(DataEntry entry) throws IncorrectDataEntryObjectException {
    if (entry.getClass() != getConfig().getDataEntryClass()) {
      throw new IncorrectDataEntryObjectException("Entry " + entry.getKey() + " using incorrect data entry class for this table.");
    }
    return saveInternalConsumer(entry, null);
  }

  public boolean saveAsync(DataEntry entry, Consumer<DataEntry> consumer) throws IncorrectDataEntryObjectException {
    if (entry.getClass() != getConfig().getDataEntryClass()) {
      throw new IncorrectDataEntryObjectException("Entry " + entry.getKey() + " using incorrect data entry class for this table.");
    }
    return saveInternalConsumer(entry, consumer);
  }

  public boolean saveSync(Object data) throws InterruptedException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException {
    if (getConfig().getDataEntryClass() == DataEntry.class)
      return saveSync((DataEntry) this.getConfig().getDataEntryClass().getDeclaredConstructor(Object.class).newInstance(data));
    return saveSync((DataEntry) this.getConfig().getDataEntryClass().getDeclaredConstructor(data.getClass()).newInstance(data));
  }

  public boolean saveAndForget(Object data) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException {
    if (getConfig().getDataEntryClass() == DataEntry.class)
      return saveAndForget((DataEntry) this.getConfig().getDataEntryClass().getDeclaredConstructor(Object.class).newInstance(data));
    return saveAndForget((DataEntry) this.getConfig().getDataEntryClass().getDeclaredConstructor(data.getClass()).newInstance(data));
  }

  public boolean saveAsync(Object data, Consumer<DataEntry> consumer) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException {
    if (getConfig().getDataEntryClass() == DataEntry.class)
      return saveAsync((DataEntry) this.getConfig().getDataEntryClass().getDeclaredConstructor(Object.class).newInstance(data), consumer);
    return saveAsync((DataEntry) this.getConfig().getDataEntryClass().getDeclaredConstructor(data.getClass()).newInstance(data), consumer);
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

  // convert object to jsonnode without changing types
  JsonNode fromObject(Object o){
    try {
      return Serializer.getObjectMapper().getOmNonType().readTree(
          Serializer.getObjectMapper().getOm().writeValueAsString(o)
      );
    } catch (JsonProcessingException e) {
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
      throw new RuntimeException(e);
    }
  }

  private boolean saveInternal(DataEntry entry, String changeUUID) {
    if (!entries.contains(entry)) {
      try {
        Create createEntry = new Create(changeUUID, entry);
        producer.push(createEntry.key, createEntry.version, createEntry, null);
        return true;
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      entry.versionIncrease();
      List<JsonOperations> changes = null;
      Set<DataEntry> dataEntrySet = get("id", entry.getKey());
      if(dataEntrySet==null || dataEntrySet.isEmpty()){
        try {
          consumerResponse(null, changeUUID);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
        return false;
      }
      DataEntry dataEntry = dataEntrySet.stream().findFirst().get();
      JsonPatch patch = JsonDiff.asJsonPatch(fromObject(dataEntry.getData()), fromObject(entry.getData()));
      try {
        String json = Serializer.getObjectMapper().getOmNonType().writeValueAsString(patch);
        changes = Serializer.getObjectMapper().getOmNonType().readValue(json, List.class);

        if (changes != null && changes.size() > 0) {
          Update updateEntry = new Update(changeUUID, entry, json);
          producer.push(updateEntry.getKey(), updateEntry.getVersion(), updateEntry, null);
          return true;
        }else{
          try {
            consumerResponse(dataEntry, changeUUID);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        }
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }

    }
    return false;
  }

  private void itemProcessed() {
    if (this.startupPhase.get()) {
      int left = this.startupLoadCount.decrementAndGet();
      if (left != 0 && left % 10000 == 0)
        logger.info("startup " + this.getConfig().getTable() + " items waiting to process: " + left);
      if (!this.getConsumer().getStartupPhaseConsume().get() && left <= 0) {
        this.startupPhase.set(false);
        System.gc();
        new Thread(() -> this.startup()).start();
      }
    }
  }

  private void triggerEvent(Modify modify, DataEntry dataEntry) {
    DataTableEventListener eventListener = config.getEventListener();
    if (eventListener != null) {
      if (modify instanceof Create) {
        new Thread(() -> eventListener.create((Create) modify, dataEntry)).start();
      } else if (modify instanceof Delete) {
        new Thread(() -> eventListener.delete((Delete) modify, dataEntry)).start();
      } else if (modify instanceof Update) {
        new Thread(() -> eventListener.update((Update) modify, dataEntry)).start();
      }
    }
  }

  private void itemRequeue() {
    if (this.startupPhase.get()) this.startupLoadCount.incrementAndGet();
  }

  public void modify(Modification mod, Object modification) {
    switch (mod) {
      case CREATE:
        Create c = (Create) modification;
        //if(!startupPhase.get()) logger.info("Create statement called");
        if (c != null) {
          try {
            itemProcessed();
            if (this.config.getReadToTime() != null && c.getTime().isAfter(this.config.getReadToTime())) {
              consumerResponse(null, c.getChangeUUID());
              fireListeners(Modification.CREATE, null);
              return;
            }

            if (keyToEntry.containsKey(c.getKey())) {
              //logger.info("Ignore already saved change.");
              //System.exit(1);
              return; // ignore this create
            }

            DataEntry dataEntry = (DataEntry) this.getConfig().getDataEntryClass().getDeclaredConstructor(Create.class).newInstance(c);

            dataEntry.setTableName(this.config.getTable());

            synchronized (entries) {
              entries.add(dataEntry);
              keyToEntry.put(dataEntry.getKey(), dataEntry);
              for (IndexWrapper i : this.indexes.values()) {
                try {
                  i.add(dataEntry);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              }
            }
            size++;
            consumerResponse(dataEntry, c.getChangeUUID());
            fireListeners(Modification.CREATE, dataEntry);
            triggerEvent(c, dataEntry);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case DELETE:
        Delete d = (Delete) modification;
        //if(!startupPhase.get()) logger.info("Delete statement called");
        if (d != null) {
          try {
            itemProcessed();
            if (this.config.getReadToTime() != null && d.getTime().isAfter(this.config.getReadToTime())) {
              consumerResponse(null, d.getChangeUUID());
              fireListeners(Modification.DELETE, null);
              return;
            }

            DataEntry de = keyToEntry.get(d.getKey());
            if (de != null) {
              if (de.getVersion() >= d.getVersion()) {
                //logger.info("Ignore already saved change.");
                consumerResponse(de, d.getChangeUUID());
                fireListeners(Modification.DELETE, de);
                return; // ignore change
              }
              if (de.getVersion() + 1 != d.getVersion()) {
                //logger.info("Version not ready!");
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
                //Serializer.log("ADDED TO MOD QUEUE");
              } else {
                deletedEntries.add(de.getKey());
                synchronized (entries) {
                  entries.remove(de);
                  keyToEntry.remove(de.getKey());
                  this.indexes.values().forEach(i -> {
                    try {
                      i.delete(de);
                    } catch (InvalidIndexTypeException e) {
                      throw new RuntimeException(e);
                    }
                  });
                }
                size--;
                consumerResponse(de, d.getChangeUUID());
                fireListeners(Modification.DELETE, de);
                triggerEvent(d, de);
                long items = itemsToBeCleaned.incrementAndGet();
                if (!startupPhase.get() && items > 100) {
                  itemsToBeCleaned.set(0L);
                  System.gc();
                }
              }
            } else {
              if (deletedEntries.contains(d.getKey())) {
                //logger.info("ERROR already deleted: "+d.getKey()+" table: "+this.config.getTable());
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
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case UPDATE:
        Update u = (Update) modification;
        //if(!startupPhase.get()) logger.info("Update statement called");
        if (u != null) {
          try {
            itemProcessed();
            if (this.config.getReadToTime() != null && u.getTime().isAfter(this.config.getReadToTime())) {
              logger.info("Update after target db date");
              consumerResponse(null, u.getChangeUUID());
              fireListeners(Modification.UPDATE, null);
              return;
            }
            DataEntry de = keyToEntry.get(u.getKey());
            if (de != null) {
              if (de.getVersion() >= u.getVersion()) {
                logger.info("Ignore already saved change. " + de.getKey()+" table: "+ getConfig().table);
                consumerResponse(null, u.getChangeUUID());
                return; // ignore change
              }
              if (de.getVersion() + 1 != u.getVersion()) {
                logger.info("Version not ready!" + de.getKey()+" table: "+ getConfig().table);
                itemRequeue();
                modqueue.add(new ModificationQueueItem(mod, modification));
                leftInModQueue.incrementAndGet();
                synchronized (modqueue) {
                  modqueue.notifyAll();
                }
              } else {
                de.setData(fromJsonNode(u.getChangesPatch().apply(fromObject(de.getData())), de.getData().getClass()));
                de.setVersion(u.getVersion());
                de.setModified(u.getTime());
                u.getOperations().forEach(op -> {
                  switch (op.getOp()) {
                    case "replace":
                    case "add":
                    case "copy":
                      try {
                        synchronized (entries) {
                          IndexWrapper indexWrapper = this.indexes.get(op.getPath());
                          if (indexWrapper != null) {
                            indexWrapper.modify(de);
                          }
                        }
                      } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      } catch (InvalidIndexTypeException e) {
                        throw new RuntimeException(e);
                      }
                      break;
                    case "move":
                      break;
                    case "remove":
                      synchronized (entries) {
                        IndexWrapper indexWrapper = this.indexes.get(op.getPath());
                        if (indexWrapper != null) {
                          try {
                            indexWrapper.delete(de);
                          } catch (InvalidIndexTypeException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      }
                      break;
                  }
                });
                this.changed = new Date().getTime();
                consumerResponse(de, u.getChangeUUID());
                fireListeners(Modification.UPDATE, de);
                triggerEvent(u, de);
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

  private void consumerResponse(DataEntry dataEntry, String changeUUID) throws ExecutionException {
    try {
      if (changeUUID != null) {
        Consumer<DataEntry> dataEntryConsumer = consumers.getIfPresent(changeUUID);
        if (dataEntryConsumer != null) {
          new Thread(() -> dataEntryConsumer.accept(dataEntry)).start();
          consumers.invalidate(changeUUID);
        }
      }
    } catch (CacheLoader.InvalidCacheLoadException e) {
    }
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

  public void multiImport(DataEntry newEntry) throws InterruptedException, IncorrectDataEntryObjectException {
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

  public Set<DataEntry> getEntries() {
    return entries;
  }

  public void setEntries(Set<DataEntry> entries) {
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

  public Map<String, IndexWrapper<DataEntry>> getIndexes() {
    return indexes;
  }

  public void setIndexes(Map<String, IndexWrapper<DataEntry>> indexes) {
    this.indexes = indexes;
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

  public Queue<ModificationQueueItem> getModqueue() {
    return modqueue;
  }

  public void setModqueue(Queue<ModificationQueueItem> modqueue) {
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

  public AtomicInteger getLeftInModQueue() {
    return leftInModQueue;
  }

  public void setLeftInModQueue(AtomicInteger leftInModQueue) {
    this.leftInModQueue = leftInModQueue;
  }

  public AtomicInteger getStartupLoadCount() {
    return startupLoadCount;
  }

  public void setStartupLoadCount(AtomicInteger startupLoadCount) {
    this.startupLoadCount = startupLoadCount;
  }

  public AtomicBoolean getStartupPhase() {
    return startupPhase;
  }

  public void setStartupPhase(AtomicBoolean startupPhase) {
    this.startupPhase = startupPhase;
  }
}
