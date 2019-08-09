package com.nucleocore.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.nucleocore.db.database.Database;
import com.nucleocore.db.database.DataEntry;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

public class TestDatabase {
    @Test
    public void shouldCreateEntry(){
        Database db = new Database("test");
        db.getMap().set("nathaniel", new com.nucleocore.db.Test("Nathaniel", "nathanield"));
        assertTrue(db.getMap().size()==1);
        db.getMap().flush();
    }

    @Test
    public void shouldGetCreatedEntry() throws JsonProcessingException {
        Database db = new Database("test2");
        db.getMap().set("david", new com.nucleocore.db.Test("David", "davidl"));
        assertTrue(db.getMap().size()==1);
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate sqlQuery = e.get("name").equal("David");
        Collection<DataEntry> entries = db.getMap().values( sqlQuery );
        for(DataEntry de : entries){
            System.out.println(((com.nucleocore.db.Test)de).getName());
        }
        assertTrue(entries.size()==1);
        db.getMap().flush();
    }
}
