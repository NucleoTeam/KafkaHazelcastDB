package com.nucleodb.library;

import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Update;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.event.DataTableEventListener;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LockTest{
  @Test
  public void lockingTest() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    NucleoDB nucleoDB = new NucleoDB(
        NucleoDB.DBType.NO_LOCAL,
        c -> {
          c.getConnectionConfig().setMqsConfiguration(new LocalConfiguration());
        },
        c -> {
          c.getDataTableConfig().setMqsConfiguration(new LocalConfiguration());
        },
        c -> {
          c.setMqsConfiguration(new LocalConfiguration());
        },
        "com.nucleodb.library.helpers.models"
    );

    try {
      AuthorDE authorDE = new AuthorDE(new Author("test", "testing"));
      nucleoDB.getTable(Author.class).saveSync(authorDE);
      DataEntry first = nucleoDB.getTable(Author.class).get("name", "test", new DataEntryProjection(){{
        setWritable(true);
        setLockUntilWrite(true);
      }}).stream().findFirst().get();
      ((AuthorDE) first).getData().setName("test2");
      nucleoDB.getTable(Author.class).saveSync(first);
      System.out.println("SAVED FIRST");
      AuthorDE secondSave = (AuthorDE) nucleoDB.getTable(Author.class).get("name", "test", new DataEntryProjection(){{
        setWritable(true);
        setLockUntilWrite(true);
      }}).stream().findFirst().get();
      secondSave.getData().setName("test4");
      nucleoDB.getTable(Author.class).saveSync(secondSave);
      System.out.println("SAVED SECOND");
      AuthorDE thirdSave = (AuthorDE) nucleoDB.getTable(Author.class).get("name", "test", new DataEntryProjection(){{
        setWritable(true);
        setLockUntilWrite(true);
      }}).stream().findFirst().get();
      nucleoDB.getTable(Author.class).deleteSync(thirdSave);
      System.out.println("FINISHED");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (IncorrectDataEntryObjectException e) {
      throw new RuntimeException(e);
    }

  }
}
