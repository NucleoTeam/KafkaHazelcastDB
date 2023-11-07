package com.nucleocore.library.database.utils;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.database.modifications.Create;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.UUID;

public class DataEntry implements Serializable, Comparable<DataEntry> {
    private static final long serialVersionUID = 1;
    public String key;
    public long version = -1;
    private JsonNode reference;
    public Object data;
    private transient String tableName;
    private Date created;
    private Date modified;

    public DataEntry(Object obj) {
        this.data = obj;
        this.reference = Serializer.getObjectMapper().getOm().valueToTree(data);
        this.key = UUID.randomUUID().toString();
        this.created = new Date();
    }

    public DataEntry(Create create) throws ClassNotFoundException, JsonProcessingException {
        this.data = Serializer.getObjectMapper().getOm().readValue(create.getData(), Class.forName(create.getMasterClass()));
        this.version = create.getVersion();
        this.reference = Serializer.getObjectMapper().getOm().valueToTree(data);
        this.key = create.getKey();
        this.created = new Date();
    }


    public DataEntry(DataEntry toCopy) {
        try {
            for (Field field : DataEntry.class.getDeclaredFields()) {
                if(field.isAnnotationPresent(SkipCopy.class)) continue;
                System.out.println(field.getName());
                field.set(this, field.get(toCopy));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public DataEntry() {
        this.key = UUID.randomUUID().toString();
        this.created = new Date();
    }

    public DataEntry(String key) {
        this.key = key;
        this.created = new Date();
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
        version+=1;
        this.modified = new Date();
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Date getCreated() {
        return created;
    }

    public Date getModified() {
        return modified;
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
    public static Object cast(DataEntry dataEntry, Class<?> clazz) throws JsonProcessingException {
        return new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(dataEntry), clazz);
    }
    public Object cast(Class<?> clazz) throws JsonProcessingException {
        return new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(this), clazz);
    }
}
