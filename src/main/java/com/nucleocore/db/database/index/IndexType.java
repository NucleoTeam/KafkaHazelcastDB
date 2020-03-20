package com.nucleocore.db.database.index;

public enum IndexType {
    HASH(new HashIndex()),
    TRIE(new TrieIndex()),
    SETFULLTEXT(new SetIndex()),
    BINARY(new BinaryIndex()),
    ;

    private final IndexTemplate type;
    IndexType(IndexTemplate type){
        this.type = type;
    }

    public IndexTemplate getIndexType() {
        return type;
    }
    public static IndexType get(String val){
        return IndexType.valueOf(val.toUpperCase());
    }
}
