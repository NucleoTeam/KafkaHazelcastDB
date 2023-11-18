package com.nucleocore.library.database.tables.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.database.utils.Serializer;

import java.util.Queue;
import java.util.Stack;
import java.util.logging.Logger;

class ModQueueHandler implements Runnable{
  private static Logger logger = Logger.getLogger(ModQueueHandler.class.getName());
  DataTable dataTable;

  public ModQueueHandler(DataTable dataTable) {
    this.dataTable = dataTable;
  }

  @Override
  public void run() {
    ModificationQueueItem mqi;
    Queue<ModificationQueueItem> modqueue = dataTable.getModqueue();
    ObjectMapper om = new ObjectMapper().findAndRegisterModules();
    boolean overkillCheck = false;
    while (true) {
      int left = 0;
      while (!modqueue.isEmpty() && (mqi = modqueue.poll())!=null) {
        this.dataTable.modify(mqi.getMod(), mqi.getModification());
        int leftTmp = dataTable.getLeftInModQueue().decrementAndGet();
        if(left == leftTmp){
          overkillCheck = true;
          break;
        }else{
          overkillCheck = false;
        }

        left = leftTmp;
      }
      try {
        if(overkillCheck) {
          Thread.sleep(10);
        } else {
          synchronized (modqueue) {
            if (dataTable.getLeftInModQueue().get() == 0) modqueue.wait();
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}