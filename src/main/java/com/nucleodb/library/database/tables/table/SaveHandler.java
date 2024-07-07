package com.nucleodb.library.database.tables.table;

import com.nucleodb.library.database.utils.ObjectFileWriter;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SaveHandler implements Runnable{
    private static Logger logger = Logger.getLogger(SaveHandler.class.getName());
    DataTable dataTable;


    public SaveHandler(DataTable dataTable) {
      this.dataTable = dataTable;
    }

    @Override
    public void run() {
      long changedSaved = this.dataTable.getChanged();
      while (true) {
        try {
          if (this.dataTable.getChanged() > changedSaved) {
              logger.log(Level.FINEST,"Saved " + this.dataTable.getConfig().getTableFileName());
            new ObjectFileWriter().writeObjectToFile(this.dataTable, this.dataTable.getConfig().getTableFileName());
            changedSaved = this.dataTable.getChanged();
          }
          Thread.sleep(this.dataTable.getConfig().getSaveInterval());
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }