package com.nucleocore.nucleodb.database.modifications;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.nucleodb.database.utils.DataEntry;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;

public class Create extends Modify{
    public String data;
    public String key;
    public String changeUUID;
    public String masterClass;
    public long version;
    public String time;

    public Create() {
    }

    public Create(String changeUUID, DataEntry entry) throws IOException{
        this.changeUUID = changeUUID;
        this.key = entry.getKey();
        this.masterClass = entry.getData().getClass().getName();
        this.data = new ObjectMapper().writeValueAsString(entry.getData());
        this.version = entry.getVersion();
        this.time = Instant.now().toString();
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

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
