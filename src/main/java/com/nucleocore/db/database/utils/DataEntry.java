package com.nucleocore.db.database.utils;

import java.io.Serializable;

public abstract class DataEntry implements Serializable {
    public String key = "";

    public DataEntry() {
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
