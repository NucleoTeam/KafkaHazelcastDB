package com.nucleodb.library.mqs;

import com.google.common.collect.Queues;
import com.nucleodb.library.database.lock.LockManager;
import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.exceptions.RequiredMethodNotImplementedException;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class ConsumerHandler implements Runnable{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());

  private DataTable database = null;
  private ConnectionHandler connectionHandler = null;

  private LockManager lockManager = null;
  private String topic;
  int startupItems = -1;
  private Queue<String> queue = Queues.newConcurrentLinkedQueue();
  private transient AtomicInteger leftToRead = new AtomicInteger(0);
  private AtomicInteger startupLoadCount = new AtomicInteger(0);
  private AtomicBoolean startupPhaseConsume = new AtomicBoolean(true);
  boolean reloadConsumer = false;


  private MQSSettings settings;

  private ExecutorService queueTasks = Executors.newFixedThreadPool(60);


  public ConsumerHandler(MQSSettings settings) {
    this.settings = settings;
  }
  public ConsumerHandler reload(java.util.function.Consumer completeCallback) {
    ConsumerHandler reloadConsumerHandler =  new ConsumerHandler(settings);
    reloadConsumerHandler.setReloadConsumer(true);
    return reloadConsumerHandler;
  }
  public void start(int queues){
    for (int x = 0; x < queues; x++) {
      Thread queueThread = new Thread(new QueueHandler(this));
      queueTasks.submit(queueThread);
    }
  }

  @Override
  public void run() {
    try {
      throw new RequiredMethodNotImplementedException("Consumer Handler not implemented");
    } catch (RequiredMethodNotImplementedException e) {
      e.printStackTrace();
    }
    System.exit(1);
  }

  public static Logger getLogger() {
    return logger;
  }

  public static void setLogger(Logger logger) {
    ConsumerHandler.logger = logger;
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
  public int getStartupItems() {
    return startupItems;
  }

  public void setStartupItems(int startupItems) {
    this.startupItems = startupItems;
  }

  public Queue<String> getQueue() {
    return queue;
  }

  public void setQueue(Queue<String> queue) {
    this.queue = queue;
  }

  public AtomicInteger getLeftToRead() {
    return leftToRead;
  }

  public void setLeftToRead(AtomicInteger leftToRead) {
    this.leftToRead = leftToRead;
  }

  public AtomicInteger getStartupLoadCount() {
    return startupLoadCount;
  }

  public void setStartupLoadCount(AtomicInteger startupLoadCount) {
    this.startupLoadCount = startupLoadCount;
  }

  public AtomicBoolean getStartupPhaseConsume() {
    return startupPhaseConsume;
  }

  public void setStartupPhaseConsume(AtomicBoolean startupPhaseConsume) {
    this.startupPhaseConsume = startupPhaseConsume;
  }

  public MQSSettings getSettings() {
    return settings;
  }

  public void setSettings(MQSSettings settings) {
    this.settings = settings;
  }

  public LockManager getLockManager() {
    return lockManager;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setLockManager(LockManager lockManager) {
    this.lockManager = lockManager;
  }

  public boolean isReloadConsumer() {
    return reloadConsumer;
  }

  public void setReloadConsumer(boolean reloadConsumer) {
    this.reloadConsumer = reloadConsumer;
  }
}
