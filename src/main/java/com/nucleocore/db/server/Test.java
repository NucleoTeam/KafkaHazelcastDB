package com.nucleocore.db.server;

import com.nucleocore.db.server.Entry;

import java.io.Serializable;

public class Test implements Entry, Serializable {
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
