package com.nucleocore.db.database.modifications;

import com.nucleocore.db.database.Modification;

public class Delete extends Modify {
    public String key;

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
}
