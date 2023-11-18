package com.nucleocore.library.database.tables.table;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.SkipCopy;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class DataEntry implements Serializable, Comparable<DataEntry> {
    @SkipCopy
    private static final long serialVersionUID = 1;
    public String key;
    public long version = -1;
    private JsonNode reference;
    public Object data;
    private transient String tableName;
    private Instant created;
    private Instant modified;

    public DataEntry(Object obj) {
        this.data = obj;
        this.reference = Serializer.getObjectMapper().getOm().valueToTree(data);
        this.key = UUID.randomUUID().toString();
        this.created = Instant.now();
    }

    public DataEntry(Create create) throws ClassNotFoundException, JsonProcessingException {
        this.data = Serializer.getObjectMapper().getOm().readValue(create.getData(), Class.forName(create.getMasterClass()));
        this.version = create.getVersion();
        this.reference = Serializer.getObjectMapper().getOm().valueToTree(data);
        this.key = create.getKey();
        this.created = create.getTime();
    }


    public <T> T copy(Class<T> clazz) {
        ObjectMapper om = new ObjectMapper()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .findAndRegisterModules();
        try {
            T obj =  om.readValue(om.writeValueAsString(this), clazz);
            return obj;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public DataEntry() {
        this.key = UUID.randomUUID().toString();
        this.created = Instant.now();
    }

    public DataEntry(String key) {
        this.key = key;
        this.created = Instant.now();
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
        this.modified = Instant.now();
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

    public Instant getCreated() {
        return created;
    }

    public Instant getModified() {
        return modified;
    }

    @Override
    public String toString() {
        return "key='" + key;
    }

    @Override
    public int compareTo(@NotNull DataEntry o) {

        return o.key.hashCode()-this.key.hashCode();
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
