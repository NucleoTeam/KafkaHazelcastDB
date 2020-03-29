package com.nucleocore.db.database.utils;

import com.nucleocore.db.database.index.Index;
import com.nucleocore.db.database.index.IndexType;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.UUID;

public abstract class DataEntry implements Serializable {

    @Index(IndexType.BINARY)
    public String key;

    public long version = -1;

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

    public long getVersion() {
        return version;
    }

    public void versionIncrease(){
        version++;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}
