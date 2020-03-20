package com.nucleocore.db.database.utils;

import com.nucleocore.db.database.index.Index;
import com.nucleocore.db.database.index.IndexType;

public class Test extends DataEntry {

    @Index
    public String name;

    @Index(IndexType.TRIE)
    public String user;

    public Test() {

    }

    public Test(Test t) {
        this.name = t.name;
        this.user = t.user;
        this.key = t.key;
    }


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Test(String key, String name, String user) {
        super(key);
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
