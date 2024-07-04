package com.nucleodb.library.database.tables.table;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleodb.library.database.lock.LockReference;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.SkipCopy;
import com.nucleodb.library.database.utils.Utils;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class DataEntry<T> implements Serializable, Comparable<DataEntry> {
    @SkipCopy
    private static final long serialVersionUID = 1;
    public String key;
    public long version = 0;
    public T data;

    @JsonIgnore
    private transient String tableName;

    private Instant created;
    private Instant modified;
    private String request;

    @JsonIgnore
    public transient DataTable dataTable;

    public DataEntry(T obj) {
        this.data = obj;
        this.key = UUID.randomUUID().toString();
        this.created = Instant.now();
    }

    public DataEntry(Create create) throws ClassNotFoundException, JsonProcessingException {
        this.data = (T) Serializer.getObjectMapper().getOm().readValue(create.getData(), Class.forName(create.getMasterClass()));
        this.version = create.getVersion();
        this.key = create.getKey();
        this.created = create.getTime();
    }

    public <T extends DataEntry> T copy(Class<T> clazz, boolean lock) {
        if (lock){
            // get lock
            try {
                LockReference lockReference = this.dataTable.getNucleoDB().getLockManager().waitForLock(
                    this.tableName,
                    key
                );
                try {
                    T obj =  Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(this), clazz);
                    obj.setRequest(lockReference.getRequest());
                    return obj;
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }else{
            try {
                T obj =  Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(this), clazz);
                return obj;
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
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

    public T getData() {
        return data;
    }

    public void setData(T data) {
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
        return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof DataEntry){
            return ((DataEntry) obj).getKey().equals(this.key);
        }
        return super.equals(obj);
    }
    public static Object cast(DataEntry dataEntry, Class<?> clazz) throws JsonProcessingException {
        return new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(dataEntry), clazz);
    }
    public Object cast(Class<?> clazz) throws JsonProcessingException {
        return new ObjectMapper().readValue(new ObjectMapper().writeValueAsString(this), clazz);
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public void setModified(Instant modified) {
        this.modified = modified;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

}
