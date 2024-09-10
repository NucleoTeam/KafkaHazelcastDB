package com.nucleodb.library;

import com.nucleodb.library.database.modifications.*;
import com.nucleodb.library.database.tables.connection.Connection;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.tables.table.NodeFilter;
import com.nucleodb.library.database.utils.InvalidConnectionException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.database.utils.exceptions.ObjectNotSavedException;
import com.nucleodb.library.models.*;
import com.nucleodb.library.mqs.kafka.KafkaConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NodeFilterTest {
    NucleoDB nucleoDB;
    DataTable<AuthorDE> authorTable;
    DataTable<BookDE> bookTable;
    ConnectionHandler<WroteConnection> wroteConnections;

    @BeforeEach
    public void createLocalDB() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException, InterruptedException, InvalidConnectionException {
        nucleoDB = new NucleoDB(
                NucleoDB.DBType.ALL,
                c -> {
                    c.getConnectionConfig().setMqsConfiguration(new KafkaConfiguration());
                    c.getConnectionConfig().setLoadSaved(true);
                    c.getConnectionConfig().setJsonExport(true);
                    c.getConnectionConfig().setSaveChanges(true);
                    c.getConnectionConfig().setConnectionFileName("./data/"+ c.getConnectionConfig().getLabel()+".dat");
                    c.getConnectionConfig().setExportInterval(50);
                    c.getConnectionConfig().setSaveInterval(50);
                    c.getConnectionConfig().setNodeFilter(new com.nucleodb.library.database.tables.connection.NodeFilter(){
                        List accept = Arrays.asList("test1", "test2");
                        @Override
                        public boolean create(ConnectionCreate c) {
                            return accept.contains(c.getUuid());
                        }

                        @Override
                        public <C extends Connection> boolean delete(ConnectionDelete d, C existing) {
                            return accept.contains(d.getUuid());
                        }

                        @Override
                        public <C extends Connection> boolean update(ConnectionUpdate u, C existing) {
                            return accept.contains(u.getUuid());
                        }

                        @Override
                        public <C extends Connection> boolean accept(String key) {
                            return accept.contains(key);
                        }
                    });
                },
                c -> {
                    c.getDataTableConfig().setMqsConfiguration(new KafkaConfiguration());
                    c.getDataTableConfig().setLoadSave(true);
                    c.getDataTableConfig().setSaveChanges(true);
                    c.getDataTableConfig().setJsonExport(true);
                    c.getDataTableConfig().setTableFileName("./data/"+ c.getDataTableConfig().getTable()+".dat");
                    c.getDataTableConfig().setExportInterval(50);
                    c.getDataTableConfig().setSaveInterval(50);
                    c.getDataTableConfig().setNodeFilter(new NodeFilter(){
                        List accept = Arrays.asList("test1", "test2");
                        @Override
                        public boolean create(Create c) {
                            return accept.contains(c.getKey());
                        }

                        @Override
                        public <T extends DataEntry> boolean delete(Delete d, T existing) {
                            return accept.contains(d.getKey());
                        }

                        @Override
                        public <T extends DataEntry> boolean update(Update u, T existing) {
                            return accept.contains(u.getKey());
                        }

                        @Override
                        public <T extends DataEntry> boolean accept(String key) {
                            return accept.contains(key);
                        }
                    });
                },
                c -> {
                    c.setMqsConfiguration(new KafkaConfiguration());
                },
                "com.nucleodb.library.models"
        );
        authorTable = nucleoDB.getTable(Author.class);
        bookTable = nucleoDB.getTable(Book.class);
        wroteConnections = nucleoDB.getConnectionHandler(WroteConnection.class);
    }
    @AfterEach
    public void deleteEntries() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        authorTable
                .getEntries()
                .stream()
                .map(author -> {
                    try {
                        return author.copy(AuthorDE.class, true);
                    } catch (ObjectNotSavedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toSet()).forEach(author -> {
                    try {
                        authorTable.deleteSync(author);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
        bookTable
                .getEntries()
                .stream()
                .map(book -> {
                    try {
                        return book.copy(BookDE.class, true);
                    } catch (ObjectNotSavedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toSet()).forEach(book -> {
                    try {
                        bookTable.deleteSync(book);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
        wroteConnections
                .getAllConnections()
                .stream()
                .map(c->c.copy(WroteConnection.class,true))
                .collect(Collectors.toSet()).forEach(c -> {
                    try {
                        wroteConnections.deleteSync(c);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void checkSaving() throws IncorrectDataEntryObjectException, InterruptedException {
        AuthorDE edgarAllenPoe = new AuthorDE(new Author("Edgar Allen Poe", "fiction"));
        edgarAllenPoe.setKey("test1");
        authorTable.saveSync(edgarAllenPoe);
        assertEquals(
                1,
                authorTable.get(
                        "id",
                        edgarAllenPoe.getKey(),
                        null
                ).size()
        );
        assertEquals(2, authorTable.getEntries().size());
    }

}
