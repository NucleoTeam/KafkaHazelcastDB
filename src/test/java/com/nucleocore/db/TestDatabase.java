package com.nucleocore.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.nucleocore.db.database.Table;
import com.nucleocore.db.database.utils.DataEntry;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

public class TestDatabase {
    @Test
    public void shouldCreateEntry(){
        Table db = new Table("192.169.1.16:9093,192.169.1.19:9093,192.169.1.17:9093", "test");
        db.getMap().set("nathaniel", new com.nucleocore.db.database.utils.Test("tes","Nathaniel", "nathanield"));
        assertTrue(db.getMap().size()==1);
        db.getMap().flush();
    }

    @Test
    public void shouldGetCreatedEntry() throws JsonProcessingException {
        Table db = new Table("192.169.1.16:9093,192.169.1.19:9093,192.169.1.17:9093","test2");
        db.getMap().set("david", new com.nucleocore.db.database.utils.Test("test","David", "davidl"));
        assertTrue(db.getMap().size()==1);
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate sqlQuery = e.get("name").equal("David");
        Collection<DataEntry> entries = db.getMap().values( sqlQuery );
        for(DataEntry de : entries){
            System.out.println(((com.nucleocore.db.database.utils.Test)de).getName());
        }
        assertTrue(entries.size()==1);
        db.getMap().flush();
    }
}
