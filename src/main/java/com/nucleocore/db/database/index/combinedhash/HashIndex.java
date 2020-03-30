package com.nucleocore.db.database.index.combinedhash;

import com.nucleocore.db.database.index.IndexTemplate;
import com.nucleocore.db.database.index.binary.BinaryIndex;
import com.nucleocore.db.database.utils.DataEntry;

import java.lang.reflect.Field;
import java.util.List;

public class HashIndex extends IndexTemplate {
    HashObject root;
    @Override
    public void add(DataEntry entry) {
        try {
            root.add(entry, this.getField().get(entry).toString());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public IndexTemplate indexOn(Field field) {
        root = new HashObject(field);
        return super.indexOn(field);
    }

    @Override
    public boolean update(DataEntry entry) {
        return super.update(entry);
    }

    @Override
    public List<DataEntry> search(DataEntry indexCheck) {
        try {
            BinaryIndex bi = root.getIndex(this.getField().get(indexCheck).toString());
            if(bi!=null) {
                return bi.search(indexCheck);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean delete(DataEntry entry) {
        return super.delete(entry);
    }

    @Override
    public boolean addAll(List<DataEntry> dataEntries) {
        dataEntries.stream().forEach(i->add(i));
        return true;
    }

    public HashObject getRoot() {
        return root;
    }
}
