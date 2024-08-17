package com.nucleodb.library.mqs;

import com.nucleodb.library.database.modifications.Create;
import com.nucleodb.library.database.modifications.Modification;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.tables.table.DataTableConfig;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.helpers.models.Author;
import com.nucleodb.library.helpers.models.AuthorDE;
import net.sf.jsqlparser.schema.Database;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class QueueHandlerTest {
    
    private ConsumerHandler consumerHandler;
    private QueueHandler queueHandler;

    @BeforeEach
    public void setup() {
        consumerHandler = mock(ConsumerHandler.class);
        queueHandler = new QueueHandler(consumerHandler);
    }

    @Test
    public void testRun_method() throws IOException, ExecutionException {
      
        // Define mocks, for testing different branches of the run method
        // Mock for the database modification branch
        ConcurrentLinkedQueue<String> queueMock = new ConcurrentLinkedQueue<>();
        queueMock.add("CREATE"+ Serializer.getObjectMapper().getOm().writeValueAsString(new Create(new AuthorDE(new Author("poe", "horror")))));
        when(consumerHandler.getQueue()).thenReturn(queueMock);
        when(consumerHandler.getDatabase()).thenReturn(mock(DataTable.class));
        AtomicInteger queueSize = new AtomicInteger(1);
        when(consumerHandler.getLeftToRead()).thenReturn(queueSize);

        assertEquals(consumerHandler.getLeftToRead().get(), 1);

        Thread thread = new Thread(queueHandler);
        thread.start();

        try {
            // Giving some time for thread to process queue
            Thread.sleep(500);
        } catch (InterruptedException e) {
           // necessary for the rare case where sleep is interrupted
        } finally {
                thread.interrupt();
        }

        assertEquals(consumerHandler.getLeftToRead().get(), 0);

        // Check if the modify method called once for the Modification Mock
        verify(consumerHandler.getDatabase(), times(1)).modify(Mockito.any(Modification.class), Mockito.any(Object.class));
    }

    // Similar for the rest branches
}