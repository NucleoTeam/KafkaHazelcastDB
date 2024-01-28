package com.nucleodb.library.database.lock;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;
import org.jetbrains.annotations.NotNull;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LockManager implements Runnable{

  Queue<LockReference> queue = Queues.newLinkedBlockingQueue();

  @JsonIgnore
  private static Logger logger = Logger.getLogger(LockManager.class.getName());

  private String ownerId = UUID.randomUUID().toString();
  private ScheduledExecutorService executorPool = Executors.newScheduledThreadPool(1000);

  ProducerHandler producerHandler;
  ConsumerHandler consumerHandler;

  ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

  private Map<String, LockReference> pendingLocks = new TreeMap<>();

  private ConcurrentMap<String, ConcurrentLinkedQueue<LockReference>> waiting = new ConcurrentHashMap<>();
  private LockConfig config;
  public LockManager(LockConfig lockConfig) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    this.config = lockConfig;
    consumerHandler = lockConfig.getMqsConfiguration().createConsumerHandler(lockConfig.getSettingsMap());
    consumerHandler.setLockManager(this);
    consumerHandler.start(1);
    lockConfig.getSettingsMap().put("consumerHandler", this.consumerHandler);
    producerHandler = lockConfig.getMqsConfiguration().createProducerHandler(lockConfig.getSettingsMap());
  }


  public void startup() {
    if (this.config.getStartupRun() != null) {
      this.config.getStartupRun().run(this);
    }
  }

  @Override
  public void run() {
    scheduledExecutorService.scheduleAtFixedRate(()->{
      LockReference reference;
      while((reference = queue.poll())!=null){
        String entryKey = String.format("%s_%s", reference.getTableName(), reference.getKey());
        pendingLocks.put(reference.getRequest(), reference);
        ConcurrentLinkedQueue<LockReference> waitingLocks;
        synchronized (waiting) {
          if(!waiting.containsKey(entryKey)) {
            waitingLocks = Queues.newConcurrentLinkedQueue();
            waiting.put(entryKey, waitingLocks);
          }else{
            waitingLocks = waiting.get(entryKey);
          }
        }
        waitingLocks.add(reference);
        try {
          producerHandler.push(entryKey, Serializer.getObjectMapper().getOm().writeValueAsString(reference));
        } catch (JsonProcessingException e) {
          e.printStackTrace();
        }
      }
    }, 0, 10, TimeUnit.MILLISECONDS);

  }



  private transient ConcurrentMap<String, LockReference> activeLocks = new ConcurrentHashMap(){
    @Override
    public Object put(@NotNull Object key, @NotNull Object value) {
      executorPool.schedule(()->{
        LockReference lockReference = (LockReference)this.get(key);
        if(lockReference!=null && value instanceof LockReference && lockReference.getRequest().equals(((LockReference)value).getRequest())) {
          logger.info( "EXPIRED");
          logger.info((String) key);
          try {
            LockReference activeLock = Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(value), LockReference.class);
            activeLock.setLock(false);
            lockAction(activeLock);
          } catch (JsonProcessingException e) {
            e.printStackTrace();
          }
        }
      }, 1000, TimeUnit.MILLISECONDS);
      return super.put(key, value);
    }
  };
  public LockReference waitForLock(String table, String key) throws InterruptedException {
    LockReference lockReference = new LockReference(table, key, ownerId, true);
    addToQueue(lockReference);
    synchronized (lockReference) {
      lockReference.wait();
    }
    return lockReference;
  }

  public void releaseLock(String table, String key, String lockReference) {
    log(lockReference, "RELEASE LOCK CALLED");
    if(lockReference==null) {
      return;
    }
    String entryKey = String.format("%s_%s", table, key);
    LockReference activeLock = activeLocks.get(entryKey);
    if (activeLock != null) {
      if (!activeLock.getRequest().equals(lockReference)) {
        return;
      }
      if (!activeLock.getOwner().equals(ownerId)) {
        return;
      }
      try {
        activeLock = Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(activeLock), LockReference.class);
        activeLock.setLock(false);
        push(activeLock);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
    }
  }

  public boolean availableForLock(String table, String key) {
    LockReference ifPresent = activeLocks.get(String.format("%s_%s", table, key));
    if (ifPresent == null) return true;
    if (!ifPresent.getTime().plusSeconds(1).isAfter(Instant.now())) return true;
    return false;
  }

  public boolean hasLock(String table, String key, String request) {
    LockReference ifPresent = activeLocks.get(String.format("%s_%s", table, key));
    if (ifPresent == null) return false;
    if (!ifPresent.getRequest().equals(request)) return false;
    if (!ifPresent.getOwner().equals(ownerId)) return false;
    if (!ifPresent.getTime().plusSeconds(1).isAfter(Instant.now())) return false;
    return true;
  }

  void addToQueue(LockReference lockReference){
    queue.add(lockReference);
  }
  public void push(LockReference lockReference) {
    try {
      String key = String.format("%s_%s", lockReference.getTableName(), lockReference.getKey());
      producerHandler.push(key, Serializer.getObjectMapper().getOm().writeValueAsString(lockReference));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  public void lockAction(LockReference lockReference) {
    String key = String.format("%s_%s", lockReference.getTableName(), lockReference.getKey());
    LockReference currentActiveLock = activeLocks.get(key);

    if (lockReference.isLock()) {
      if (currentActiveLock != null && !currentActiveLock.getRequest().equals(lockReference.getRequest())) {
        log(currentActiveLock.getRequest(), "active lock check time");
        // do not lock, wait for lock to release
        if (currentActiveLock.getTime().plusSeconds(2).isAfter(Instant.now())) {
          log(currentActiveLock.getRequest(), "active lock check time: WAIT FOR LOCK");
          return;
        }
      }
      //activeLocks.invalidate(key);
      activeLocks.put(key, lockReference);
      if(waiting.containsKey(key)) waiting.get(key).removeIf(c -> c.getRequest().equals(lockReference.getRequest()));
      LockReference lockReferenceLocalObject = pendingLocks.remove(lockReference.getRequest());
      if (lockReferenceLocalObject != null) {
        synchronized (lockReferenceLocalObject) {
          lockReferenceLocalObject.notify();
        }
      }

    } else { // lock is false, release current active lock

      if (currentActiveLock != null) {
        log(currentActiveLock.getRequest()+" ][ "+lockReference.getRequest(), "lockAction: ACTIVE LOCK FOUND");
        if(lockReference.getRequest().equals(currentActiveLock.getRequest())) {
          activeLocks.remove(key);
          log(currentActiveLock.getRequest()+" ][ "+lockReference.getRequest(), "lockAction: CURRENT LOCK REMOVED");
        }else{
          log(currentActiveLock.getRequest()+" ][ "+lockReference.getRequest(), "lockAction: REQUEST NOT SAME AS CURRENT LOCK");
          return;
        }
      }else{
        log(lockReference.getRequest(), "lockAction: NO ACTIVE LOCK");
        return;
      }

      ConcurrentLinkedQueue<LockReference> lockReferences = waiting.get(key);
      if (lockReferences != null && lockReference.getRequest().equals(currentActiveLock.getRequest())) {
        log(lockReference.getRequest(), "lockAction: GETTING WAITING LOCK REQUESTS FOR: "+key);

        Iterator<LockReference> iterator = lockReferences.iterator();
        if (iterator.hasNext()) {
          LockReference first = iterator.next();
          log(first.getRequest(), "lockAction: SENDING TO WAITING LOCK ");
          push(first);
        } else {
          log(key, "lockAction: NO NEW WAITING LOCKS");
          //waiting.remove(key);
          //activeLocks.remove(key);
        }
      }
    }
  }

  void log(String key, String msg){
    logger.log(Level.FINE,String.format("%s: %s", key, msg));
  }

  public ConcurrentMap<String, LockReference> getActiveLocks() {
    return activeLocks;
  }

  public void setActiveLocks(ConcurrentMap<String, LockReference> activeLocks) {
    this.activeLocks = activeLocks;
  }
}
