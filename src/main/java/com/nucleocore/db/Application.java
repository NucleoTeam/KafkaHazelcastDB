package com.nucleocore.db;

import com.nucleocore.db.server.Database;

public class Application {
    public static void main(String... args){
        String tableString = System.getenv("tables");
        String[] tables = tableString.split(",");

    }
}
