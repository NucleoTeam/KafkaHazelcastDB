package com.nucleodb.library.database.tables.connection;

import com.nucleodb.library.database.modifications.*;

public class NodeFilter {
    public  boolean create(ConnectionCreate c){
        return true;
    }
    public <C extends Connection> boolean delete(ConnectionDelete d, C existing){
        return true;
    }
    public <C extends Connection> boolean update(ConnectionUpdate u, C existing){
        return true;
    }
    public <C extends Connection> boolean accept(String key){
        return true;
    }
}
