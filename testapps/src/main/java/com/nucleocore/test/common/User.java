package com.nucleocore.test.common;


import com.nucleocore.library.database.tables.annotation.Index;
import com.nucleocore.library.database.tables.annotation.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@Table(tableName= "user", dataEntryClass = UserDE.class)
public class User implements Serializable{
    private static final long serialVersionUID = 1;


    @Index("name")
    private String name;

    private List<UserNested> testingNested = new ArrayList<>();

    private String user;

    public User() {
        this.testingNested.add(new UserNested());
    }

    public User(User t) {
        this.name = t.name;
        this.user = t.user;
        this.testingNested.add(new UserNested());
    }


    public User(String name, String user) {
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

    public List<UserNested> getTestingNested() {
        return testingNested;
    }

    public void setTestingNested(List<UserNested> testingNested) {
        this.testingNested = testingNested;
    }
}
