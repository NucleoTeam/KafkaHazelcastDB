package com.nucleocore.db.database.modifications;

import com.nucleocore.db.database.Modification;

import java.util.Map;
import java.util.TreeMap;

public class Update extends Modify {
    public String key;
    public Map<String, Object> change = new TreeMap<>();
    public String masterClass;

    public Update() {
    }

    public Update(String key, Map<String, Object> change, String masterClass) {
        this.key = key;
        this.change = change;
        this.masterClass = masterClass;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Map<String, Object> getChange() {
        return change;
    }

    public void setChange(Map<String, Object> change) {
        this.change = change;
    }

    public String getMasterClass() {
        return masterClass;
    }

    public void setMasterClass(String masterClass) {
        this.masterClass = masterClass;
    }
}
