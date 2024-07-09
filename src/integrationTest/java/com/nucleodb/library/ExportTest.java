package com.nucleodb.library;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.InvalidConnectionException;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.database.utils.exceptions.ObjectNotSavedException;
import com.nucleodb.library.models.*;
import com.nucleodb.library.mqs.kafka.KafkaConfiguration;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExportTest {
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
                },
                c -> {
                    c.getDataTableConfig().setMqsConfiguration(new KafkaConfiguration());
                    c.getDataTableConfig().setLoadSave(true);
                    c.getDataTableConfig().setSaveChanges(true);
                    c.getDataTableConfig().setJsonExport(true);
                    c.getDataTableConfig().setTableFileName("./data/"+ c.getDataTableConfig().getTable()+".dat");
                    c.getDataTableConfig().setExportInterval(50);
                    c.getDataTableConfig().setSaveInterval(50);
                },
                c -> {
                    c.setMqsConfiguration(new KafkaConfiguration());
                },
                "com.nucleodb.library.models"
        );
        authorTable = nucleoDB.getTable(Author.class);
        bookTable = nucleoDB.getTable(Book.class);
        wroteConnections = nucleoDB.getConnectionHandler(WroteConnection.class);

        AuthorDE georgeOrwell = new AuthorDE(new Author("George Orwell", "science-fiction"));
        BookDE bookDE = new BookDE(new Book("Nineteen Eighty-Four"));
        authorTable.saveSync(georgeOrwell);
        bookTable.saveSync(bookDE);
        AuthorDE author = authorTable.get("name", "George Orwell").stream().findFirst().get();
        BookDE book = bookTable.get("name", "Nineteen Eighty-Four").stream().findFirst().get();
        wroteConnections.saveSync(new WroteConnection(author, book));

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
        Thread.sleep(5000);
    }
    @Test
    public void fileSaving() throws IncorrectDataEntryObjectException, InterruptedException {
        AuthorDE author = authorTable.get("name", "George Orwell").stream().findFirst().get();
        assertEquals(
                1,
                wroteConnections.getByFrom(author, null).size()
        );
        Thread.sleep(5000);
    }

}