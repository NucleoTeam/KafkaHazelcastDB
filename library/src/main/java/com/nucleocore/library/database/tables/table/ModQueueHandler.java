package com.nucleocore.library.database.tables.table;

import java.util.Stack;

class ModQueueHandler implements Runnable{
  DataTable dataTable;

  public ModQueueHandler(DataTable dataTable) {
    this.dataTable = dataTable;
  }

  @Override
  public void run() {
    ModificationQueueItem mqi;
    Stack<ModificationQueueItem> modqueue = dataTable.getModqueue();
    while (true) {
      while (!modqueue.isEmpty() && (mqi = modqueue.pop())!=null) {
        this.dataTable.modify(mqi.getMod(), mqi.getModification());
        dataTable.getLeftInModQueue().decrementAndGet();
      }
      try {
        synchronized (modqueue) {
          if(dataTable.getLeftInModQueue().get()==0) modqueue.wait();
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}