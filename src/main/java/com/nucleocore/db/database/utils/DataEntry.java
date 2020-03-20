package com.nucleocore.db.database.utils;

import com.nucleocore.db.database.index.Index;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.UUID;

public abstract class DataEntry implements Serializable {

    @Index
    public String key;

    public DataEntry(DataEntry toCopy) {
        try {
            for (Field field : this.getClass().getDeclaredFields()) {
                field.set(this, field.get(toCopy));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

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
