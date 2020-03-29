package com.nucleocore.db.database.modifications;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.utils.DataEntry;

import java.io.IOException;

public class Create extends Modify {
    public String data;
    public String key;
    public String masterClass;
    public long version;
    private static ObjectMapper om = new ObjectMapper();

    public Create() {
    }

    public Create(String key, DataEntry data) throws IOException{

        this.key = key;
        this.masterClass = data.getClass().getName();
        this.data = om.writeValueAsString(data);
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
    public DataEntry getValue() throws ClassNotFoundException, IOException {
        return (DataEntry) om.readValue(data, Class.forName(masterClass));
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
}
