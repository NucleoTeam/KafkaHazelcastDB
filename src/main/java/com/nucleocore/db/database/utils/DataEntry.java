package com.nucleocore.db.database.utils;

import java.io.Serializable;
import java.util.UUID;

public abstract class DataEntry implements Serializable {
    private static int incrementId = 0;
    public String key;

    public DataEntry() {
        incrementId++;
        this.key = ""+incrementId;
    }

    public DataEntry(String key) {
        this.key = key;
    }

    public String getKey(){
        return key;
    }

    public void setKey(String key){
        this.key = key;
    }

}
