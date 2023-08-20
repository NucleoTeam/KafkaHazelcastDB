package com.nucleocore.library.database.utils;

import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.modifications.Delete;
import com.nucleocore.library.database.modifications.Update;

public enum Modification {
    DELETE(Delete.class),
    UPDATE(Update.class),
    CREATE(Create.class);
    private final Class modification;
    Modification(Class modification){
        this.modification = modification;
    }

    public Class<?> getModification() {
        return modification;
    }
    public static Modification get(String val){
        return Modification.valueOf(val.toUpperCase());
    }
}
