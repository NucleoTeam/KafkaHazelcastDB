package com.nucleocore.library.database.tables.connection;

import com.nucleocore.library.database.tables.table.DataTable;

import java.util.Stack;

public class ModQueueHandler implements Runnable{
  ConnectionHandler connectionHandler;

  public ModQueueHandler(ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
  }

  @Override
  public void run() {
    Stack<ModificationQueueItem> modqueue = connectionHandler.getModqueue();
    ModificationQueueItem mqi;
    while (true) {
      while (!modqueue.isEmpty() && (mqi = modqueue.pop())!=null) {
        connectionHandler.modify(mqi.getMod(), mqi.getModification());
      }
      try {
        Thread.sleep(50);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}