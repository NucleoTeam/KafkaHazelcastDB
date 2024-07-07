package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.database.utils.ObjectFileWriter;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SaveHandler implements Runnable {
    private static Logger logger = Logger.getLogger(SaveHandler.class.getName());
    ConnectionHandler connectionHandler;

    public SaveHandler(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    @Override
    public void run() {
        long changedSaved = this.connectionHandler.getChanged();

        while (true) {
            try {
                if (this.connectionHandler.getChanged() > changedSaved) {
                    logger.log(Level.FINEST,"Saved " + connectionHandler.getConfig().getConnectionFileName());
                    new ObjectFileWriter().writeObjectToFile(this.connectionHandler, connectionHandler.getConfig().getConnectionFileName());
                    changedSaved = this.connectionHandler.getChanged();
                }
                Thread.sleep(this.connectionHandler.getConfig().getSaveInterval());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}