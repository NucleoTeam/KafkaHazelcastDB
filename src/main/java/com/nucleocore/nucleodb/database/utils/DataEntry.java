package com.nucleocore.nucleodb.database.utils;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.database.modifications.Create;
import org.jetbrains.annotations.NotNull;

import javax.json.JsonObject;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.UUID;

public class DataEntry implements Serializable, Comparable<DataEntry> {

    public String key;

    public long version = -1;

    private JsonNode reference;
    public Object data;

    public DataEntry(Object obj) {
        this.data = obj;
        this.reference = new ObjectMapper().valueToTree(data);
        this.key = UUID.randomUUID().toString();
    }

    public DataEntry(Create create) throws ClassNotFoundException, JsonProcessingException {
        this.data = new ObjectMapper().readValue(create.getData(), Class.forName(create.getMasterClass()));
        this.version = create.getVersion();
        this.reference = new ObjectMapper().valueToTree(data);
        this.key = create.getKey();
    }


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

    public JsonNode getReference() {
        return reference;
    }

    public Object getData() {
        return data;
    }

    public void setReference(JsonNode reference) {
        this.reference = reference;
    }

    public void setData(Object data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "key='" + key;
    }

    @Override
    public int compareTo(@NotNull DataEntry o) {
        return o.key.compareTo(this.key);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof DataEntry){
            return ((DataEntry) obj).getKey().equals(this.getKey());
        }
        return super.equals(obj);
    }
}
