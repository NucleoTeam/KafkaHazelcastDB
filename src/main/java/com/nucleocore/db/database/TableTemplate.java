package com.nucleocore.db.database;

import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Modification;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public interface TableTemplate {
    void modify(Modification mod, Object modification);
    boolean save(DataEntry oldEntry, DataEntry newEntry, Consumer<DataEntry> consumer);
    boolean save(DataEntry oldEntry, DataEntry newEntry);
    void multiImport(DataEntry newEntry);
    int getSize();
    void startup();
    void setBuildIndex(boolean buildIndex);
    void setUnsavedIndexModifications(boolean unsavedIndexModifications);
    void consume();
}
