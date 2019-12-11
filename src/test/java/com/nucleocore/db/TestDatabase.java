package com.nucleocore.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleocore.db.database.DataTable;
import com.nucleocore.db.database.utils.DataEntry;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class TestDatabase {
    @Test
    public void shouldCreateEntry(){
        DataTable db = new DataTable(null, "test", com.nucleocore.db.database.utils.Test.class);
        db.save(null, new com.nucleocore.db.database.utils.Test("tes","Nathaniel", "nathanield"));
        assertTrue(db.size()==1);
        db.flush();
    }

    @Test
    public void shouldGetCreatedEntry() throws JsonProcessingException {
        DataTable db = new DataTable(null,"test2", com.nucleocore.db.database.utils.Test.class);
        db.save(null, new com.nucleocore.db.database.utils.Test("test","David", "davidl"));
        assertTrue(db.size()==1);
        Stream<Map.Entry<String, DataEntry>> entries = db.filterMap(d->((com.nucleocore.db.database.utils.Test)d.getValue()).getName().equals("David"));
        entries.forEach(entry->{
            System.out.println(((com.nucleocore.db.database.utils.Test)entry.getValue()).getName());
        });
        assertTrue(db.size()==1);
        db.flush();
    }
}
