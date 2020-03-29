package com.nucleocore.db.database.modifications;

public class Delete extends Modify {
    public String key;
    public long version;

    public Delete() {
    }

    public Delete(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}