package com.nucleocore.db;

import com.nucleocore.db.database.DataEntry;

import java.io.Serializable;

public class Test implements DataEntry, Serializable {
    public String name;
    public String user;

    public Test(String name, String user) {
        this.name = name;
        this.user = user;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
