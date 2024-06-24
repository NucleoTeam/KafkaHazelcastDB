package com.nucleodb.library.database.tables.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.nucleodb.library.database.modifications.Create;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.stream.Collectors;

public class ExportHandler implements Runnable {
    DataTable dataTable;

    Queue<String> modifications = Queues.newConcurrentLinkedQueue();

    public ExportHandler(DataTable dataTable) {
        this.dataTable = dataTable;
    }

    @Override
    public void run() {
        long changedSaved = this.dataTable.getChanged();
        while (true) {
            try {
                if (this.dataTable.getChanged() > changedSaved) {
                    System.out.println("datatable export saved "+this.dataTable.getConfig().getTable() );
                    OutputStream os = new FileOutputStream("./export/" + this.dataTable.getConfig().getTable() + ".txt", true);
                    String entry;
                    while ((entry = modifications.poll()) != null) {
                        try {
                            os.write((entry + "\n").getBytes(StandardCharsets.UTF_8));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    os.close();
                    changedSaved = this.dataTable.getChanged();
                }
                Thread.sleep(this.dataTable.getConfig().getExportInterval());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Queue<String> getModifications() {
        return modifications;
    }

    public void setModifications(Queue<String> modifications) {
        this.modifications = modifications;
    }
}

