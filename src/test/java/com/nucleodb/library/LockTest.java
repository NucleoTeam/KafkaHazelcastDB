package com.nucleodb.library;

import com.nucleodb.library.database.lock.LockManager;
import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Delete;
import com.nucleodb.library.database.modifications.Update;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.event.DataTableEventListener;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import com.nucleodb.library.mqs.kafka.KafkaConfiguration;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class LockTest{
  private static Logger logger = Logger.getLogger(LockTest.class.getName());
  @Test
  public void lockingTest() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    logger.info("Starting lock test");
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
      int x = 1;
      AuthorDE authorDE = new AuthorDE(new Author("test", "testing"));
      DataTable<AuthorDE> table = nucleoDB.getTable(Author.class);
      table.saveSync(authorDE);
      while(x<25) {

        AuthorDE firstSave = table.get("name", "test", new DataEntryProjection(){{
          setWritable(true);
          setLockUntilWrite(true);
        }}).iterator().next();


        firstSave.getData().setName("test2");
        logger.info(firstSave.getRequest()+": SENDING TO SAVE");
        //assertEquals(1, nucleoDB.getLockManager().getActiveLocks().size());
        table.saveSync(firstSave);
        //assertEquals(0, nucleoDB.getLockManager().getActiveLocks().size());
        logger.info(firstSave.getRequest()+": SAVED FIRST");

        long start = System.currentTimeMillis();
        AuthorDE secondSave = table.get("name", "test", new DataEntryProjection(){{
          setWritable(true);
          setLockUntilWrite(true);
        }}).iterator().next();
        secondSave.getData().setName("test4");
        //assertEquals(1, nucleoDB.getLockManager().getActiveLocks().size());
        logger.info(secondSave.getRequest()+": SENDING TO SAVE");
        table.saveSync(secondSave);
        //assertEquals(0, nucleoDB.getLockManager().getActiveLocks().size());
        logger.info(secondSave.getRequest()+": SAVED SECOND");

        AuthorDE thirdSave = table.get("name", "test", new DataEntryProjection(){{
          setWritable(true);
          setLockUntilWrite(true);
        }}).iterator().next();
        if(System.currentTimeMillis()-start>1000){
          logger.info("SHOULD FAIL, LOCK TOO SLOW");
          fail();
        }
        thirdSave.getData().setName("test4");
        //assertEquals(1, nucleoDB.getLockManager().getActiveLocks().size());
        logger.info(thirdSave.getRequest()+": SENDING TO SAVE");
        table.saveSync(thirdSave);
        logger.info(thirdSave.getRequest()+": SAVED THIRD");
        //assertEquals(0, nucleoDB.getLockManager().getActiveLocks().size());
        logger.info("-----------------------------"+x+"-------------------------");
        x++;
      }
      logger.info("FINISHED");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (IncorrectDataEntryObjectException e) {
      throw new RuntimeException(e);
    }

  }
}
