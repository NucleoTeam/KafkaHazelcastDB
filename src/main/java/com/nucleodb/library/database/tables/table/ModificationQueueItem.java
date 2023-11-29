package com.nucleodb.library.database.tables.table;

import com.nucleodb.library.database.modifications.Modification;

import java.io.Serializable;

public class ModificationQueueItem implements Serializable{
    private static final long serialVersionUID = 1;
    private Modification mod;
    private Object modification;

    public ModificationQueueItem(Modification mod, Object modification) {
      this.mod = mod;
      this.modification = modification;
    }

    public Modification getMod() {
      return mod;
    }

    public Object getModification() {
      return modification;
    }
  }