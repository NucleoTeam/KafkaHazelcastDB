package com.nucleocore.nucleodb.cli;

import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.utils.Serializer;

import java.nio.file.Paths;

public class DBBackup{
  public static void main(String[] args) throws ClassNotFoundException {
    NucleoDB db = new NucleoDB();
    Serializer.log(args);
    db.launchReadOnlyTable(args[0], args[1], Class.forName(args[3])).setTableFileName(Paths.get(args[2],  args[1]+".dat").toString()).setSaveChanges(true).build();
  }
}
