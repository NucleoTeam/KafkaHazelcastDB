package com.nucleocore.memcache;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.DataTable;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.index.TreeIndex;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class ClientHandler implements Runnable{
  private static final String END_OF_RESPONSE = "END\r\n";
  private static final String STORED_RESPONSE = "STORED\r\n";

  private final Socket clientSocket;
  private DataTable table;

  public ClientHandler(Socket socket, DataTable table) {
    this.clientSocket = socket;
    this.table = table;
  }

  @Override
  public void run() {
    try (
        InputStreamReader isr = new InputStreamReader(clientSocket.getInputStream());
        BufferedReader reader = new BufferedReader(isr);
        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
    ) {
      String command;
      while ((command = reader.readLine()) != null) {
        String[] parts = command.split(" ");
        String operation = parts[0].toLowerCase();

        switch (operation) {
          case "get":
            String getValue = null;
            Set<DataEntry> entrySet = this.table.get("name", parts[1]);
            if (entrySet != null && entrySet.size() >= 1) {
              KeyVal keyval = (KeyVal) entrySet.stream().findFirst().get().getData();
              getValue = keyval.getValue();
              out.print("VALUE " + parts[1] + " 0 " + getValue.length() + "\r\n" + getValue + "\r\n");
            }
            out.print(END_OF_RESPONSE);
            break;
          case "set":
            handleSetCommand(parts, reader, out);
            break;
          case "flush_all":
            int countdown = this.table.getEntries().size();
            CountDownLatch countDownLatch = new CountDownLatch(countdown);
            this.table.getEntries().parallelStream().forEach(entry->this.table.delete(entry, (de)->countDownLatch.countDown()));
            countDownLatch.await();
            out.print("OK\r\n");
            break;
          case "delete":
            entrySet = this.table.get("name", parts[1]);
            if (entrySet != null && entrySet.size() >= 1) {
              countDownLatch = new CountDownLatch(1);
              this.table.delete(entrySet.stream().findFirst().get(), (de)->{
                countDownLatch.countDown();
              });
              countDownLatch.await();
              out.print("DELETED\r\n");
            } else {
              out.print("NOT_FOUND\r\n");
            }
            break;
          case "version":
            out.print("1.4.5-ndb\r\n");
            break;
          case "quit":
            reader.close();
            isr.close();
            out.close();
            break;
          case "stats":
            if (parts.length >= 2) {
              switch (parts[1].toLowerCase()) {
                case "cachedump":
                  if(!parts[2].equals("0")) {
                    out.print(END_OF_RESPONSE);
                    break;
                  }
                  Set<Map.Entry<Object, Set<DataEntry>>> objs = ((TreeIndex) this.table.getIndexes().get("name")).getIndex().entrySet();
                  for (Map.Entry<Object, Set<DataEntry>> obj : objs) {
                    if (obj.getKey() instanceof String) {
                      Optional<DataEntry> objectOptional = obj.getValue().stream().findFirst();
                      if (objectOptional.isPresent()) {
                        out.print("ITEM " + obj.getKey() + " [" + ((KeyVal)objectOptional.get().getData()).getValue().length() + " b; 0 s]\r\n");
                      }
                    }
                  }
                  out.print(END_OF_RESPONSE);
                  break;
              }
            } else {
              out.print("STAT pid " + Thread.currentThread().getId() + "\r\n");
              out.print("STAT version 1.4.5-ndb\r\n");
              out.print("STAT uptime 3600\r\n");
              out.print("STAT active_slabs 1\r\n");
              out.print("STAT curr_items " + table.getEntries().size() + "\r\n");
              out.print("STAT total_items " + table.getEntries().size() + "\r\n");
              out.print("STAT time 1677645759\r\n");
              out.print(END_OF_RESPONSE);
            }
            break;
          default:
            Serializer.log("RESPONDED WITH ERROR");
            Serializer.log(command);
            out.print("ERROR\r\n");
            out.print(END_OF_RESPONSE);
            break;
        }
        out.flush();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    Serializer.log("DISCONNECTED");
  }

  private void handleSetCommand(String[] parts, BufferedReader in, PrintWriter out) throws IOException, InterruptedException {
    int bytes = Integer.parseInt(parts[4]);

    char[] buffer = new char[bytes];
    in.read(buffer);

    String value = new String(buffer);


    // For demonstration purposes, just print the extracted value.
    System.out.println("Extracted value from set command: " + value);

    Set<DataEntry> entrySet = this.table.get("name", parts[1]);
    if (entrySet != null && entrySet.size() >= 1) {
      DataEntry entry = table.createNewObject(entrySet).stream().findFirst().get();
      ((KeyVal) entry.getData()).setValue(value);
      CountDownLatch countDownLatch = new CountDownLatch(1);
      table.save(entry, (de)->{
        System.out.println("saved update");
        countDownLatch.countDown();
      });
      countDownLatch.await();
      out.print(STORED_RESPONSE);
    } else {
      CountDownLatch countDownLatch = new CountDownLatch(1);
      this.table.insert(new KeyVal(parts[1], value), (de)->{
        System.out.println("saved new");
        countDownLatch.countDown();
      });
      countDownLatch.await();
      out.print(STORED_RESPONSE);
    }

    // Consume the terminating '\r\n' after the value
    in.readLine();
  }
}