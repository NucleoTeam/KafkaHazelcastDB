package com.nucleocore.db.database.index;

import com.nucleocore.db.database.index.binary.BinaryIndex;
import com.nucleocore.db.database.index.combinedhash.HashIndex;
import com.nucleocore.db.database.index.set.SetIndex;
import com.nucleocore.db.database.index.trie.TrieIndex;

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
