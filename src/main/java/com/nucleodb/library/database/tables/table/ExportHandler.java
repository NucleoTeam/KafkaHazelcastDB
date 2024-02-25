package com.nucleodb.library.database.tables.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.library.database.modifications.Create;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

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
            OutputStream os = new FileOutputStream("./export/"+this.dataTable.getConfig().getTable()+".txt", false);
            this.dataTable.getEntries().stream().forEach(de->{
              try {
                os.write((Create.class.getSimpleName() + om.writeValueAsString(new Create((DataEntry) de))+"\n").getBytes(StandardCharsets.UTF_8));
              } catch (JsonProcessingException e) {
                e.printStackTrace();
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
            os.close();
            changedSaved = this.dataTable.getChanged();
          }
          Thread.sleep(5000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

