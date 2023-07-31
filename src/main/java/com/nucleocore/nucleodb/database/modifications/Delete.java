package com.nucleocore.nucleodb.database.modifications;

import com.nucleocore.nucleodb.database.utils.DataEntry;

import java.time.Instant;
import java.util.Date;

public class Delete extends Modify{
    public String changeUUID;
    public String key;
    public long version;

    public Instant time;

    public Delete() {
    }

    public Delete(String changeUUID, DataEntry dataEntry) {
        this.key = dataEntry.getKey();
        this.changeUUID = changeUUID;
        this.version = dataEntry.getVersion();
        this.time = Instant.now();
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

    public Instant getTime() {
        return time;
    }

    public void setTime(Instant time) {
        this.time = time;
    }

    public String getChangeUUID() {
        return changeUUID;
    }

    public void setChangeUUID(String changeUUID) {
        this.changeUUID = changeUUID;
    }
}