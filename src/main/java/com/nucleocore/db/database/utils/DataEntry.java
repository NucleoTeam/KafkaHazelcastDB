package com.nucleocore.db.database.utils;

import java.io.Serializable;
import java.util.UUID;

public abstract class DataEntry implements Serializable {

    @Index
    public String key;

    public DataEntry() {
        this.key = UUID.randomUUID().toString().replaceAll("-","");
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
