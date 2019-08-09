package com.nucleocore.db;

import com.nucleocore.db.database.Modification;

public class Application {
    public static void main(String... args) {
        String tableString = System.getenv("tables");
        if (tableString != null){
            String[] tables = tableString.split(",");
        }
    }
}
