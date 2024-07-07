package com.nucleodb.library.database.tables.connection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Queues;
import com.nucleodb.library.database.modifications.ConnectionCreate;
import com.nucleodb.library.database.utils.Serializer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ExportHandler implements Runnable {
    private static Logger logger = Logger.getLogger(ExportHandler.class.getName());
    ConnectionHandler connectionHandler;

    Queue<String> modifications = Queues.newConcurrentLinkedQueue();

    public ExportHandler(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    @Override
    public void run() {
        long changedSaved = this.connectionHandler.getChanged();
        while (true) {
            try {
                if (this.connectionHandler.getChanged() > changedSaved) {
                    logger.log(Level.FINEST, "datatable export saved "+this.connectionHandler.getConfig().getLabel() );
                    OutputStream os = new FileOutputStream("./export/" + this.connectionHandler.getConfig().getLabel() + ".txt", true);
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
                    changedSaved = this.connectionHandler.getChanged();
                }
                Thread.sleep(this.connectionHandler.getConfig().getExportInterval());
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

