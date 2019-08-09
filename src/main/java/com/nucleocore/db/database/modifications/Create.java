package com.nucleocore.db.database.modifications;

import com.nucleocore.db.database.DataEntry;
import com.nucleocore.db.database.Modification;

import java.io.Serializable;

public class Create {
    private DataEntry data;
    private String key;

    public Create(DataEntry data, String key) {
        this.data = data;
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public DataEntry getData() {
        return data;
    }

    public void setData(DataEntry data) {
        this.data = data;
    }
}
