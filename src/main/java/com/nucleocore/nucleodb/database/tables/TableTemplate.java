package com.nucleocore.nucleodb.database.tables;

import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.Modification;

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
