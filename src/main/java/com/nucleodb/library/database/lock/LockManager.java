package com.nucleodb.library.database.lock;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Queues;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LockManager implements Runnable{
  @JsonIgnore
  private static Logger logger = Logger.getLogger(LockManager.class.getName());

  private String ownerId = UUID.randomUUID().toString();


  ProducerHandler producerHandler;
  ConsumerHandler consumerHandler;

  @Override
  public void run() {
    try {
      while (true) {
        activeLocks.cleanUp();
        Thread.sleep(1000L);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public LockManager(LockConfig lockConfig) throws IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    consumerHandler = lockConfig.getMqsConfiguration().createConsumerHandler(lockConfig.getSettingsMap());
    consumerHandler.setLockManager(this);
    consumerHandler.start();
    lockConfig.getSettingsMap().put("consumerHandler", this.consumerHandler);
    producerHandler = lockConfig.getMqsConfiguration().createProducerHandler(lockConfig.getSettingsMap());
  }

  private Map<String, LockReference> pendingLocks = new TreeMap<>();

  private ConcurrentMap<String, ConcurrentLinkedQueue<LockReference>> waiting = new ConcurrentHashMap<>();

  private transient Cache<String, LockReference> activeLocks = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .softValues()
      .expireAfterWrite(2, TimeUnit.SECONDS)
      .removalListener(e -> {
        logger.log(Level.FINER, e.getCause().toString());
        logger.log(Level.FINER, (String)e.getKey());
        if (e.getValue() instanceof LockReference && e.getCause()== RemovalCause.EXPIRED) {
          LockReference lock = ((LockReference) e.getValue());
          lock.setLock(false);
          logger.log(Level.FINER,"expiring "+lock.getRequest()+" "+Instant.now().toString());
          lockAction(lock);
        }
      })
      .build();

  public LockReference waitForLock(String table, String key) throws InterruptedException {
    String entryKey = String.format("%s_%s", table, key);
    LockReference lockReference = new LockReference(table, key, ownerId, true);
    pendingLocks.put(lockReference.getRequest(), lockReference);
    ConcurrentLinkedQueue<LockReference> lockReferences = waiting.get(entryKey);
    if (lockReferences == null) {
      lockReferences = Queues.newConcurrentLinkedQueue();
      waiting.put(entryKey, lockReferences);
    }
    lockReferences.add(lockReference);
    try {
      producerHandler.push(entryKey, Serializer.getObjectMapper().getOm().writeValueAsString(lockReference));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    synchronized (lockReference) {
      logger.log(Level.FINER,"waiting for lock");
      lockReference.wait();

    }
    return lockReference;
  }

  public void releaseLock(String table, String key, String lockReference) {
    logger.log(Level.FINER,"RELEASE LOCK CALLED FOR "+lockReference);

    if(lockReference==null) return;
    String entryKey = String.format("%s_%s", table, key);
    LockReference activeLock = activeLocks.getIfPresent(entryKey);
    logger.log(Level.FINER, String.format("%s", entryKey));
    if (activeLock != null) {
      logger.log(Level.FINER,"LOCK FOUND");
      if(!activeLock.getRequest().equals(lockReference)){
        logger.log(Level.FINER,"CURRENT LOCK NOT REQUESTED RELEASE");
        return;
      }
      if (!activeLock.getOwner().equals(ownerId)){
        logger.log(Level.FINER,"NOT OWNER NO RELEASE");
        return;
      }
      logger.log(Level.FINER,"RELEASE LOCK");
      activeLock.setLock(false);
      push(activeLock);
    }else{
      logger.log(Level.FINER,"LOCK NOT FOUND");
    }
  }

  public boolean availableForLock(String table, String key) {
    LockReference ifPresent = activeLocks.getIfPresent(String.format("%s_%s", table, key));
    if (ifPresent == null) return true;
    if (!ifPresent.getTime().plusSeconds(1).isAfter(Instant.now())) return true;
    return false;
  }

  public boolean hasLock(String table, String key, String request) {
    LockReference ifPresent = activeLocks.getIfPresent(String.format("%s_%s", table, key));
    if (ifPresent == null) return false;
    if (!ifPresent.getRequest().equals(request)) return false;
    if (!ifPresent.getOwner().equals(ownerId)) return false;
    if (!ifPresent.getTime().plusSeconds(1).isAfter(Instant.now())) return false;
    return true;
  }

  public void push(LockReference lockReference) {
    try {
      String key = String.format("%s_%s", lockReference.getTableName(), lockReference.getKey());
      producerHandler.push(key, Serializer.getObjectMapper().getOm().writeValueAsString(lockReference));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public void lockAction(LockReference lockReference) {
    String key = String.format("%s_%s", lockReference.getTableName(), lockReference.getKey());
    LockReference currentActiveLock = activeLocks.getIfPresent(key);
    if (lockReference.isLock()) {
      if (currentActiveLock != null) {
        // do not lock, wait for lock to release
        if (currentActiveLock.getTime().plusSeconds(2).isAfter(Instant.now())) return;
      }
      logger.log(Level.FINER,"lock request: "+lockReference.getRequest());
      //activeLocks.invalidate(key);
      activeLocks.put(key, lockReference);
      LockReference lockReferenceLocalObject = pendingLocks.remove(lockReference.getRequest());
      if (lockReferenceLocalObject != null) {
        synchronized (lockReferenceLocalObject) {
          lockReferenceLocalObject.notify();
        }
      }

    } else {

      if (currentActiveLock != null) {
        logger.log(Level.FINER,"lockAction: ACTIVE LOCK FOUND");
        if(lockReference.getRequest().equals(currentActiveLock.getRequest())) {
          activeLocks.invalidate(currentActiveLock);
          logger.log(Level.FINER,"lockAction: CURRENT LOCK REMOVED");
        }else{
          logger.log(Level.FINER,"lockAction: REQUEST NOT SAME AS CURRENT LOCK");
        }
      }else{
        logger.log(Level.FINER,"lockAction: NO ACTIVE LOCK");
      }
      ConcurrentLinkedQueue<LockReference> lockReferences = waiting.get(key);
      if (lockReferences != null) {
        logger.log(Level.FINER,"lockAction: LOCK REFERENCES FOR: "+key);
        logger.log(Level.FINER,"lockAction: REMOVING FROM WAITING "+lockReference.getRequest());
        lockReferences.removeIf(c -> c.getRequest().equals(lockReference.getRequest()));
        Optional<LockReference> first = lockReferences.stream().findFirst();
        if (first.isPresent()) {
          logger.log(Level.FINER,"lockAction: SENDING TO LOCK "+first.get().getRequest());
          push(first.get());
        } else {
          logger.log(Level.FINER,"lockAction: NO NEW WAITING LOCKS");
          //waiting.remove(key);
          //activeLocks.invalidate(key);
        }
      }

    }
  }
}
