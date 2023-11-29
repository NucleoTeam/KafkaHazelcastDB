package com.nucleocore.library.database.modifications;

public enum Modification {
    DELETE(Delete.class),
    UPDATE(Update.class),
    CREATE(Create.class),

    CONNECTIONCREATE(ConnectionCreate.class),
    CONNECTIONDELETE(ConnectionDelete.class),
    CONNECTIONUPDATE(ConnectionUpdate.class);

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
