package com.nucleodb.library.mqs.local;

import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.kafka.KafkaConsumerHandler;

import java.util.logging.Logger;

public class LocalConsumerHandler extends ConsumerHandler{
  private static Logger logger = Logger.getLogger(LocalConsumerHandler.class.getName());

  public LocalConsumerHandler(MQSSettings settings, String table) {
    super(settings, table);
    logger.info("local consumer handler started for "+table);
  }

  @Override
  public void start(int queueHandlers) {
    super.getStartupPhaseConsume().set(false);
    if(getConnectionHandler()!=null){
      logger.info("startup for connection for  "+getTable());
      getConnectionHandler().getStartupPhase().set(false);
      new Thread(() -> getConnectionHandler().startup()).start();
    }
    if(getDatabase()!=null){
      logger.info("startup for data table for  "+getTable());
      getDatabase().getStartupPhase().set(false);
      new Thread(() -> getDatabase().startup()).start();
    }
    if(getLockManager()!=null){
      new Thread(() -> getLockManager().startup()).start();
    }
    super.start(queueHandlers);
  }

  @Override
  public void run() {

  }
}
