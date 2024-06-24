package com.nucleodb.library;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.tables.table.DataEntryProjection;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleodb.library.database.utils.exceptions.IncorrectDataEntryObjectException;
import com.nucleodb.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleodb.library.models.Author;
import com.nucleodb.library.models.AuthorDE;
import com.nucleodb.library.mqs.kafka.KafkaConfiguration;
import com.nucleodb.library.mqs.local.LocalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExportTest {
    NucleoDB nucleoDB;
    DataTable<AuthorDE> table;

    @BeforeEach
    public void createLocalDB() throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException, IntrospectionException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IncorrectDataEntryObjectException, InterruptedException {
        nucleoDB = new NucleoDB(
                NucleoDB.DBType.ALL,
                c -> {
                    c.getConnectionConfig().setMqsConfiguration(new KafkaConfiguration());

                    c.getConnectionConfig().setLoadSaved(true);
                    c.getConnectionConfig().setJsonExport(true);
                    c.getConnectionConfig().setSaveChanges(true);
                    c.getConnectionConfig().setConnectionFileName("./data/connection.dat");
                    c.getConnectionConfig().setExportInterval(50);
                    c.getConnectionConfig().setSaveInterval(50);
                },
                c -> {
                    c.getDataTableConfig().setMqsConfiguration(new KafkaConfiguration());
                    c.getDataTableConfig().setLoadSave(true);
                    c.getDataTableConfig().setSaveChanges(true);
                    c.getDataTableConfig().setJsonExport(true);
                    c.getDataTableConfig().setTableFileName("./data/datatable.dat");
                    c.getDataTableConfig().setExportInterval(50);
                    c.getDataTableConfig().setSaveInterval(50);
                },
                c -> {
                    c.setMqsConfiguration(new KafkaConfiguration());
                },
                "com.nucleodb.library.models"
        );
        table = nucleoDB.getTable(Author.class);
        System.out.println("STARTED");
        table.saveSync(new AuthorDE(new Author("George Orwell", "science-fiction")));
    }

    public AuthorDE copy(AuthorDE authorDE) throws JsonProcessingException {
        return Serializer.getObjectMapper().getOm().readValue(Serializer.getObjectMapper().getOm().writeValueAsString(authorDE), AuthorDE.class);
    }
    @AfterEach
    public void deleteEntries() {
        System.out.println("DONE");
        table.getEntries().stream().map(author -> {
            try {
                return copy(author);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toSet()).forEach(author-> {
            try {
                table.deleteSync(author);
            } catch (InterruptedException e) {
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
        table.saveSync(edgarAllenPoe);
        assertEquals(
                1,
                table.get(
                        "id",
                        edgarAllenPoe.getKey(),
                        null
                ).size()
        );
        Thread.sleep(5000);
    }

}