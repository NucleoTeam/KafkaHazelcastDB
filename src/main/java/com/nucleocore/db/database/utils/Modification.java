package com.nucleocore.db.database.utils;

public enum Modification {
    DELETE(com.nucleocore.db.database.modifications.Delete.class),
    UPDATE(com.nucleocore.db.database.modifications.Update.class),
    CREATE(com.nucleocore.db.database.modifications.Create.class);
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
