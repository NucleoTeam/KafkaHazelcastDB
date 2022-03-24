package com.nucleocore.nucleodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleocore.nucleodb.database.tables.DataTable;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;
import static org.junit.Assert.*;

public class TestDatabase {

    @Parameterized.Parameter
    static NucleoDB db = new NucleoDB();

    //@Test
    public void shouldCreateEntry(){
        DataTable table = db.launchNucleoTable(null, "test", com.nucleocore.nucleodb.database.utils.Test.class, null, false);
        table.save(null, new com.nucleocore.nucleodb.database.utils.Test("tes", "Nathaniel", "nathanield"));
        assertTrue(table.size() == 1);
        table.flush();
    }

    //@Test
    public void shouldGetCreatedEntry() throws JsonProcessingException {
        DataTable table = db.launchNucleoTable(null,"test2", com.nucleocore.nucleodb.database.utils.Test.class, null, false);
        table.save(null, new com.nucleocore.nucleodb.database.utils.Test("test","David", "davidl"));
        assertTrue(table.size()==1);
        com.nucleocore.nucleodb.database.utils.Test test = new com.nucleocore.nucleodb.database.utils.Test();
        test.setName("David");
        List<DataEntry> entries = table.search("name", test);
        entries.forEach(entry->{
            System.out.println(((com.nucleocore.nucleodb.database.utils.Test)entry).getName());
        });
        assertTrue(table.size()==1);
        table.flush();
    }
}
