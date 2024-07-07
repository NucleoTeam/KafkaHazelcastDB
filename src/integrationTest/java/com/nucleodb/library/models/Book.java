package com.nucleodb.library.models;

import com.nucleodb.library.database.index.TrieIndex;
import com.nucleodb.library.database.index.annotation.Index;
import com.nucleodb.library.database.tables.annotation.Table;

import java.io.Serializable;

@Table(tableName = "bookIT", dataEntryClass = BookDE.class)
public class Book implements Serializable {
    private static final long serialVersionUID = 1;
    @Index(type = TrieIndex.class)
    String name;

    public Book() {
    }

    public Book(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}