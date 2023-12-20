package com.nucleodb.library;

import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.Test;
import java.beans.IntrospectionException;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NucleoDBReadToTime{

  @Test
  public void readToTimeTest() throws IncorrectDataEntryObjectException, InterruptedException, IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    Instant instant = Instant.now().plusSeconds(5);
    NucleoDB nucleoDB = new NucleoDB(
        NucleoDB.DBType.NO_LOCAL,
        instant.toString(),
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        "com.nucleodb.library.helpers.models"
    );
    Serializer.log(instant.toString());
    DataTable table = nucleoDB.getTable(Author.class);
    table.saveSync( new AuthorDE(new Author("George Orwell", "science-fiction")));
    Thread.sleep(5000);
    table.saveSync( new AuthorDE(new Author("Jane Austen", "romance")));
    assertEquals(0, table.search("name", "Jane", null).size());
    new File(table.getConfig().getTableFileName()).delete();
  }
  @Test
  public void readOnlyDB() throws IncorrectDataEntryObjectException, InterruptedException, IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    NucleoDB nucleoDB = new NucleoDB(
        NucleoDB.DBType.READ_ONLY,
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        "com.nucleodb.library.helpers.models"
    );
    DataTable table = nucleoDB.getTable(Author.class);
    table.saveSync( new AuthorDE(new Author("George Orwell", "science-fiction")));
    assertEquals(0, table.get("name", "George Orwell", null).size());
  }
  @Test
  public void exportTest() throws IncorrectDataEntryObjectException, InterruptedException, IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    NucleoDB nucleoDB = new NucleoDB(
        NucleoDB.DBType.EXPORT,
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        "com.nucleodb.library.helpers.models"
    );
    DataTable table = nucleoDB.getTable(Author.class);
    table.saveSync( new AuthorDE(new Author("George Orwell", "science-fiction")));
    Thread.sleep(10000);
    assertTrue(new File("./export/"+table.getConfig().getTable()+".txt").exists());
    new File("./export/"+table.getConfig().getTable()+".txt").delete();
    new File(table.getConfig().getTableFileName()).delete();
  }

  @Test
  public void saveTest() throws IncorrectDataEntryObjectException, InterruptedException, IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    NucleoDB nucleoDB = new NucleoDB(
        NucleoDB.DBType.ALL,
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        c -> c.setMqsConfiguration(new LocalConfiguration()),
        "com.nucleodb.library.helpers.models"
    );
    DataTable table = nucleoDB.getTable(Author.class);
    table.saveSync( new AuthorDE(new Author("George Orwell", "science-fiction")));
    Thread.sleep(10000);
    assertTrue(new File(table.getConfig().getTableFileName()).exists());
    new File(table.getConfig().getTableFileName()).delete();
  }
}
