package com.nucleocore.db.database.utils;

public enum IndexType {
    HASH(1),
    TRIE(2),
    SETFULLTEXT(3);

    private final int type;
    IndexType(int type){
        this.type = type;
    }

    public int getIndexType() {
        return type;
    }
    public static IndexType get(String val){
        return IndexType.valueOf(val.toUpperCase());
    }
}
