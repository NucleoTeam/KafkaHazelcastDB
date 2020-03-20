package com.nucleocore.db.database.index;

import com.nucleocore.db.database.utils.DataEntry;

import java.util.List;

public class HashIndex extends IndexTemplate {
    @Override
    void add(DataEntry entry) {
        super.add(entry);
    }

    @Override
    boolean update(DataEntry entry) {
        return super.update(entry);
    }

    @Override
    List<DataEntry> search(Object indexCheck) {
        return super.search(indexCheck);
    }

    @Override
    boolean delete(DataEntry entry) {
        return super.delete(entry);
    }
}
