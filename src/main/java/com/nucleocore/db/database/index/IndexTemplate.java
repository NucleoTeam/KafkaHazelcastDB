package com.nucleocore.db.database.index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nucleocore.db.database.utils.DataEntry;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

public class IndexTemplate implements Serializable {
    Field field;
    public void add(DataEntry entry){

    }
    public IndexTemplate indexOn(Field field){
        this.field = field;
        return this;
    }
    public boolean update(DataEntry entry){
        return false;
    }
    public List<DataEntry> search(DataEntry indexCheck){
        return null;
    }
    public boolean delete(DataEntry entry){
        return false;
    }
    public boolean addAll(List<DataEntry> dataEntries){ return false; }
    public boolean reset(){ return false; }

    @JsonIgnore
    public Field getField() {
        return field;
    }
}
