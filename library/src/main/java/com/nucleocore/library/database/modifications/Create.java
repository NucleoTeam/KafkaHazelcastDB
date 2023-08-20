package com.nucleocore.library.database.modifications;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.Serializer;

import java.io.IOException;
import java.time.Instant;

public class Create extends Modify{
    public String data;
    public String key;
    public String changeUUID;
    public String masterClass;
    public long version;
    public Instant time;

    public Create() {
    }

    public Create(String changeUUID, DataEntry entry) throws IOException{
        this.changeUUID = changeUUID;
        this.key = entry.getKey();
        this.masterClass = entry.getData().getClass().getName();
        this.data = Serializer.getObjectMapper().getOm().writeValueAsString(entry.getData());
        this.version = entry.getVersion();
        this.time = Instant.now();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getData() {
        return data;
    }

    @JsonIgnore
    public Object getValue() throws ClassNotFoundException, IOException {
        return new ObjectMapper().readValue(data, Class.forName(masterClass));
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getMasterClass() {
        return masterClass;
    }

    public void setMasterClass(String masterClass) {
        this.masterClass = masterClass;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getChangeUUID() {
        return changeUUID;
    }

    public void setChangeUUID(String changeUUID) {
        this.changeUUID = changeUUID;
    }

    public Instant getTime() {
        return time;
    }

    public void setTime(Instant time) {
        this.time = time;
    }
}
