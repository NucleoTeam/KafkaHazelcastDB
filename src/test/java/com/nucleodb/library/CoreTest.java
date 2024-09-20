package com.nucleodb.library;

import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.database.utils.exceptions.ObjectNotSavedException;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

class CoreTest{
  NucleoDB nucleoDB;
  DataTable<AuthorDE> table;
  @BeforeEach
  public void createLocalDB() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException, InterruptedException {
    nucleoDB = new NucleoDB(
        NucleoDB.DBType.NO_LOCAL,
        c -> {
          c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration());
        },
        c -> {
          c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration());
        },
        c->{
          c.setMqsConfiguration(new LocalConfiguration());
        },
        "com.nucleodb.library.helpers.models"
    );
    nucleoDB.startConsuming();
    nucleoDB.waitTillReady();
    table = nucleoDB.getTable(Author.class);
    table.saveSync( new AuthorDE(new Author("George Orwell", "science-fiction")));
  }
  @AfterEach
  public void deleteEntries(){
    table.getEntries().forEach(author-> {
      try {
        table.deleteSync(author);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
  @Test
  public void checkSaving() throws IncorrectDataEntryObjectException, InterruptedException {
    AuthorDE edgarAllenPoe = new AuthorDE(new Author("Edgar Allen Poe", "fiction"));
    table.saveSync(edgarAllenPoe);
    assertEquals(
        1,
        table.get(
            "id",
            edgarAllenPoe.getKey(),
            null
        ).size()
    );
  }

  @Test
  public void checkSavingWithoutChanges() throws IncorrectDataEntryObjectException, InterruptedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    AuthorDE edgarAllenPoe = new AuthorDE(new Author("Edgar Allen Poe", "fiction"));
    table.saveSync(edgarAllenPoe);
    Set<AuthorDE> dataEntrySet = table.get("id", edgarAllenPoe.getKey());
    assertEquals(1, dataEntrySet.size());
    if(dataEntrySet.size()>0){
      DataEntry dataEntry = dataEntrySet.iterator().next();
        try {
            dataEntry.copy(AuthorDE.class, false);
        } catch (ObjectNotSavedException e) {
            throw new RuntimeException(e);
        }
        assertTrue(true);
    }
  }

  @Test
  public void checkSearch() {
    assertEquals(
        1,
        table.search("name", "Geor", null).size()
    );
  }

  @Test
  public void deleteTest() {
    table.get("name", "George Orwell", new DataEntryProjection(){{
      setWritable(true);
    }}).forEach(author-> {
      try {
        table.deleteSync(author);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    assertEquals(
        0,
        table.search("name", "Geor", null).size()
    );
  }

}