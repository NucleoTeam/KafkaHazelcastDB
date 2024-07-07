package com.nucleodb.library.database.utils.exceptions;

import com.nucleodb.library.database.tables.table.DataEntry;

public class ObjectNotSavedException extends Exception {
    public ObjectNotSavedException(DataEntry e) {
        super("Object Not Saved: "+e.key);
    }
}
