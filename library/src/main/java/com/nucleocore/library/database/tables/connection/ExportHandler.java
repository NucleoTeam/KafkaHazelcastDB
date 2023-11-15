package com.nucleocore.library.database.tables.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.database.tables.table.DataTable;

import java.io.File;

public class ExportHandler implements Runnable{
  static ObjectMapper om = new ObjectMapper().findAndRegisterModules();
  ConnectionHandler connectionHandler;

  public ExportHandler(ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
  }

  @Override
  public void run() {
    long changedSaved = this.connectionHandler.getChanged();

    while (true) {
      try {
        if (this.connectionHandler.getChanged() > changedSaved) {
          //System.out.println("Saved connections");
          om.writeValue(new File("./export/connections.json"), this.connectionHandler.getAllConnections());
          changedSaved = this.connectionHandler.getChanged();
        }
        Thread.sleep(5000);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

