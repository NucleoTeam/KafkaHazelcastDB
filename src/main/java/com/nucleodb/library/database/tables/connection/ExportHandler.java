package com.nucleodb.library.database.tables.connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.library.database.modifications.ConnectionCreate;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

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
          OutputStream os = new FileOutputStream("./export/connections.txt", false);
          this.connectionHandler.getAllConnections().stream().collect(Collectors.toSet()).stream().forEach(de->{
            try {
              os.write((ConnectionCreate.class.getSimpleName() + om.writeValueAsString(new ConnectionCreate(de))+"\n").getBytes(StandardCharsets.UTF_8));
            } catch (JsonProcessingException e) {
              e.printStackTrace();
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
          os.close();
          changedSaved = this.connectionHandler.getChanged();
        }
        Thread.sleep(5000);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

