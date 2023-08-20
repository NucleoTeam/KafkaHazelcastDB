package com.nucleocore.memcache;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.DataTable;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.Serializer;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.Set;

public class ClientHandler implements Runnable{
  private static final String END_OF_RESPONSE = "END";
  private static final String STORED_RESPONSE = "STORED";

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
        Serializer.log(command);
        String[] parts = command.split(" ");
        String operation = parts[0].toLowerCase();

        switch (operation) {
          case "get":
            Serializer.log(parts);
            Serializer.log(this.table.getIndexes());
            String getValue = null;

            Set<DataEntry> entrySet = this.table.get("name", parts[1]);
            if (entrySet != null && entrySet.size() >= 1) {
              KeyVal keyval = (KeyVal) entrySet.stream().findFirst().get().getData();
              getValue = keyval.getValue();
            }
            out.println(getValue == null ? "NOT_FOUND" : "VALUE " + parts[1] + " 0 " + getValue.length() + "\r\n" + getValue + "\r\n");
            break;
          case "set":
            handleSetCommand(parts, reader, out);
            break;
          case "delete":
            entrySet = this.table.get("name", parts[1]);
            if (entrySet != null && entrySet.size() >= 1) {
              this.table.delete(entrySet.stream().findFirst().get(), (de) -> {
                out.println("DELETED");
              });
            }
            break;
          case "version":
            out.println("1.0.0");
            break;
          case "stats":
            if (parts.length >= 2) {
              switch (parts[1].toLowerCase()) {
                case "cachedump":
                  //out.println("ITEM sample_key [11 b; 1677645759 s]");
                  //out.println("ITEM another_key [15 b; 1677645759 s]");
                  out.println(END_OF_RESPONSE);
                  break;
              }
            } else {
              out.println("STAT pid " + Thread.currentThread().getId());
              out.println("STAT version 1.0.0");
              out.println("STAT uptime 3600");
              out.println("STAT total_items " + table.getEntries().size());
              out.println("STAT time 1677645759");
              out.println(END_OF_RESPONSE);
            }
            break;
          default:
            Serializer.log(command);
            out.println("ERROR");
            break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void handleSetCommand(String[] parts, BufferedReader in, PrintWriter out) throws IOException {
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
      table.save(entry, (de) -> {
        out.println(STORED_RESPONSE);
      });
    } else {
      this.table.insert(new KeyVal(parts[1], value), (de) -> {
        out.println(STORED_RESPONSE);
      });
    }

    // Consume the terminating '\r\n' after the value
    in.readLine();
  }
}