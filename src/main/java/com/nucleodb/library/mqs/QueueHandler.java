package com.nucleodb.library.mqs;

import com.nucleodb.library.database.lock.LockReference;
import com.nucleodb.library.database.modifications.Modification;
import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.kafka.KafkaConsumerHandler;

import java.io.Serial;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

class QueueHandler implements Runnable{
  private static Logger logger = Logger.getLogger(QueueHandler.class.getName());
  private ConsumerHandler consumerHandler;

  public QueueHandler(ConsumerHandler consumerHandler) {
    this.consumerHandler = consumerHandler;
  }

  @Override
  public void run() {
    boolean connectionType = this.consumerHandler.getConnectionHandler() != null;
    boolean databaseType = this.consumerHandler.getDatabase() != null;
    boolean lockdownType = this.consumerHandler.getLockManager() != null;
    while (!Thread.interrupted()) {
      String entry = null;
      while (!this.consumerHandler.getQueue().isEmpty() && (entry = this.consumerHandler.getQueue().poll()) != null) {
        //logger.info("message received");
        this.consumerHandler.getLeftToRead().decrementAndGet();
        try {
          if (databaseType) {
            String type = entry.substring(0, 6);
            String data = entry.substring(6);
            Modification mod = Modification.get(type);
            if (mod != null) {
              if (this.consumerHandler.getDatabase()!=null && this.consumerHandler.getDatabase().getConfig() != null && this.consumerHandler.getDatabase().getConfig().isJsonExport()) {
                this.consumerHandler.getDatabase().getExportHandler().getModifications().add(entry);
              }
              this.consumerHandler.getDatabase().modify(mod, Serializer.getObjectMapper().getOm().readValue(data, mod.getModification()));
            }
          } else if (connectionType) {
            String type = entry.substring(0, 16);
            String data = entry.substring(16);
            Modification mod = Modification.get(type);
            if (mod != null) {
              try {
                Modify modifiedEntry = (Modify) Serializer.getObjectMapper().getOm().readValue(data, mod.getModification());
                if (this.consumerHandler.getConnectionHandler()!=null && this.consumerHandler.getConnectionHandler().getConfig() != null  && this.consumerHandler.getConnectionHandler().getConfig().isJsonExport()) {
                  this.consumerHandler.getConnectionHandler().getExportHandler().getModifications().add(entry);
                }
                this.consumerHandler.getConnectionHandler().modify(mod, modifiedEntry);
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }else if(lockdownType){
            logger.info("processing lockdown");
            this.consumerHandler.getLockManager().lockAction(
                Serializer.getObjectMapper().getOm().readValue(entry, LockReference.class)
            );
          }else{
            Serializer.log(entry);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (this.consumerHandler.getQueue().isEmpty()) {
        try {
          synchronized (this.consumerHandler.getQueue()) {
            if (this.consumerHandler.getLeftToRead().get() == 0) this.consumerHandler.getQueue().wait(100);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}