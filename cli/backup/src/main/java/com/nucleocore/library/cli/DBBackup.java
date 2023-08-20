package com.nucleocore.library.cli;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.Serializer;

import java.nio.file.Paths;

public class DBBackup{
  public static void main(String[] args) throws ClassNotFoundException {
    NucleoDB db = new NucleoDB();
    db.launchReadOnlyTable(args[0], args[1], Class.forName(args[3])).setTableFileName(Paths.get(args[2],  args[1]+".dat").toString()).setSaveChanges(true).build();
  }
}
