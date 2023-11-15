package com.nucleocore.library.database.tables.table;

import com.nucleocore.library.database.utils.ObjectFileWriter;

public class SaveHandler implements Runnable{
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
            //System.out.println("Saved " + this.dataTable.getConfig().getTable());
            new ObjectFileWriter().writeObjectToFile(this.dataTable, this.dataTable.getConfig().getTableFileName());
            changedSaved = this.dataTable.getChanged();
          }
          Thread.sleep(5000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }