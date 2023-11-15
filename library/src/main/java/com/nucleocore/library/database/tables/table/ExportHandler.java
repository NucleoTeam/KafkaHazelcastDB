package com.nucleocore.library.database.tables.table;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;

public class ExportHandler implements Runnable{
    DataTable dataTable;
    static ObjectMapper om = new ObjectMapper().findAndRegisterModules();

    public ExportHandler(DataTable dataTable) {
      this.dataTable = dataTable;
    }

    @Override
    public void run() {
      long changedSaved = this.dataTable.getChanged();
      while (true) {
        try {
          if (this.dataTable.getChanged() > changedSaved) {
            //System.out.println("Saved " + this.dataTable.getConfig().getTable());
            om.writeValue(new File("./export/"+this.dataTable.getConfig().getTable()+".json"), this.dataTable.getEntries());
            changedSaved = this.dataTable.getChanged();
          }
          Thread.sleep(5000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

