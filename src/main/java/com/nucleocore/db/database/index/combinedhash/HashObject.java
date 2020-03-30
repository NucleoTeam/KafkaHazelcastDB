package com.nucleocore.db.database.index.combinedhash;

import com.nucleocore.db.database.index.binary.BinaryIndex;
import com.nucleocore.db.database.utils.DataEntry;

import java.lang.reflect.Field;

public class HashObject {
    HashObject[] combined = null;
    BinaryIndex index = null;
    Field field;
    int hashSize = 5;
    int range = 4;

    public HashObject(Field field) {
        this.field = field;
    }

    public void add(DataEntry entry, String value) {
        String key = null;
        String stringLeft = null;
        if(value!=null) {
            int len = value.length();
            if (len > 0) {
                key = value.substring(0, (len >= range) ? range : len);
            }
            if (len > range) {
                stringLeft = value.substring(range, len-1);
            }
        }
        System.out.println("=================");
        System.out.println("left\t"+value);
        System.out.println("left\t"+stringLeft);
        System.out.println("key\t"+key);
        if (key == null) {
            if (index == null) {
                index = new BinaryIndex().indexOn(this.field);
            }
            index.add(entry);
        } else {
            int pos = 0;
            for (char character : key.toCharArray()) {
                pos += (int)character;
            }
            pos = pos % (hashSize-1);
            if(combined==null){
                combined = new HashObject[hashSize];
            }
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
            if (len > 0) {
                key = value.substring(0, (len >= range) ? range : len);
            }
            if (len > range) {
                stringLeft = value.substring(range, len-1);
            }
        }
        if (key == null) {
            if (index == null) {
                index = new BinaryIndex().indexOn(this.field);
            }
            return index;
        } else {
            int pos = 0;
            for (char character : key.toCharArray()) {
                pos += (int)character;
            }
            pos = pos % (hashSize-1);
            if(combined!=null) {
                if (combined[pos] != null) {
                    return combined[pos].getIndex(stringLeft);
                }
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
