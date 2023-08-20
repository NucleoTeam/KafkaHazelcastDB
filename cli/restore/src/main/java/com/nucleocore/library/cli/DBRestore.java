package com.nucleocore.library.cli;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.Serializer;

public class DBRestore{
  public static void main(String[] args) throws ClassNotFoundException {
    NucleoDB db = new NucleoDB();
    Serializer.log(args);
    db.launchLocalOnlyTable(args[0], "temptable1", Class.forName(args[3]), table->{
      try {
        table.exportTo(db.launchWriteOnlyTable(args[0], args[2], Class.forName(args[3])).build());
        System.exit(0);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }).setTableFileName("./data/" + args[1] + ".dat").build();

  }
}
