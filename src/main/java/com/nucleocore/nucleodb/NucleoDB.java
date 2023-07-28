package com.nucleocore.nucleodb;

import com.nucleocore.nucleodb.database.tables.DataTable;
import com.nucleocore.nucleodb.database.utils.StartupRun;

import java.util.*;

public class NucleoDB {
    private TreeMap<String, DataTable> tables = new TreeMap<>();
    static String latestSave = "";

    public DataTable getTable(String table){
        return (DataTable)tables.get(table);
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, String... index){
        StartupRun startup = new StartupRun(){
            public void run(DataTable table) {
                table.consume();
            }
        };
        DataTable t = new DataTable(bootstrap, table, clazz, startup, index);
        tables.put(table, t);
        return t;
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, StartupRun runnable, String... index){
        DataTable t = new DataTable(bootstrap, table, clazz, runnable, index);
        tables.put(table, t);
        return t;
    }
}
