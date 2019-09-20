package com.nucleocore.db.database;

import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Modification;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;

import java.util.Set;
import java.util.function.Consumer;

public interface TableTemplate {
    void modify(Modification mod, Object modification);
    boolean save(DataEntry oldEntry, DataEntry newEntry, Consumer<DataEntry> consumer);
    boolean save(DataEntry oldEntry, DataEntry newEntry);
    <T> Set<T> search(String name, Object obj, Class clazz);
    void multiImport(DataEntry newEntry);
    void updateIndex(Class clazz);
    void startImportThreads();
    void resetIndex(Class clazz);
    void updateIndex(DataEntry de, Class clazz);
}
