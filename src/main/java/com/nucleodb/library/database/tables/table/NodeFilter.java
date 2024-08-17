package com.nucleodb.library.database.tables.table;

import com.nucleodb.library.database.modifications.*;

public class NodeFilter {
    public boolean create(Create c){
        return true;
    }
    public <T extends DataEntry> boolean delete(Delete d, T existing){
        return true;
    }
    public <T extends DataEntry> boolean update(Update u, T existing){
        return true;
    }
    public <T extends DataEntry> boolean accept(String key){
        return true;
    }
}
