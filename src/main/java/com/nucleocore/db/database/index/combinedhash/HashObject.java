package com.nucleocore.db.database.index.combinedhash;

import com.nucleocore.db.database.index.binary.BinaryIndex;
import com.nucleocore.db.database.utils.DataEntry;

import java.lang.reflect.Field;

public class HashObject {
    HashObject[] combined = new HashObject[25];
    BinaryIndex index = null;
    Field field;

    public HashObject(Field field) {
        this.field = field;
    }

    public void add(DataEntry entry, String value) {
        String key = null;
        String stringLeft = null;
        if(value!=null) {
            int len = value.length();
            if (len > 0) {
                key = value.substring(0, (len >= 2) ? 2 : 1);
            }
            if (len > 2) {
                stringLeft = value.substring(2, len-1);
            }
        }
        System.out.println(key);
        if (key == null) {
            if (index == null) {
                index = new BinaryIndex().indexOn(this.field);
            }
            index.add(entry);
        } else {
            int pos = 0;
            int k = 0;
            for (char character : key.toCharArray()) {
                pos += character * (k * 100);
                k++;
            }
            pos = pos % 24;
            if(combined[pos]==null){
                combined[pos] = new HashObject(field);
            }
            combined[pos].add(entry, stringLeft);
        }
    }

    public BinaryIndex getIndex(String value){
        String key = null;
        String stringLeft = null;
        if(value!=null) {
            int len = value.length();
            if (len > 0 && len <= 2) {
                key = value.substring(0, (len == 2) ? 1 : len-1);
            }
            if (len > 2) {
                stringLeft = value.substring(3);
            }
        }
        if (key == null) {
            if (index == null) {
                index = new BinaryIndex().indexOn(this.field);
            }
            return index;
        } else {
            int pos = 0;
            int k = 1;
            for (char character : key.toCharArray()) {
                pos += character * k;
                k++;
            }
            pos = pos % 25;
            if(combined[pos]!=null){
                return combined[pos].getIndex(stringLeft);
            }
        }
        return null;
    }

    public HashObject[] getCombined() {
        return combined;
    }


    public BinaryIndex getIndex() {
        return index;
    }

    public void setIndex(BinaryIndex index) {
        this.index = index;
    }
}
