package com.nucleocore.nucleodb.database.utils;

public enum Modification {
    DELETE(com.nucleocore.nucleodb.database.modifications.Delete.class),
    UPDATE(com.nucleocore.nucleodb.database.modifications.Update.class),
    CREATE(com.nucleocore.nucleodb.database.modifications.Create.class);
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
