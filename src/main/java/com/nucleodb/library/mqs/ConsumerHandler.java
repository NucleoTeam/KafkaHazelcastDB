package com.nucleodb.library.mqs;

import com.google.common.collect.Queues;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.exceptions.RequiredMethodNotImplementedException;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class ConsumerHandler implements Runnable{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());

  private DataTable database = null;
  private ConnectionHandler connectionHandler = null;
  private String table;
  int startupItems = -1;
  private Queue<String> queue = Queues.newConcurrentLinkedQueue();
  private transient AtomicInteger leftToRead = new AtomicInteger(0);
  private AtomicInteger startupLoadCount;
  private AtomicBoolean startupPhaseConsume = new AtomicBoolean(true);

  private MQSSettings settings;

  public ConsumerHandler(MQSSettings settings, String table) {
    this.table = table;
    this.settings = settings;

  }
  public void start(){
    for (int x = 0; x < 36; x++)
      new Thread(new QueueHandler(this)).start();
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

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
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
}
