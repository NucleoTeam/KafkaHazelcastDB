package com.nucleocore.library.cli;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.StartupRun;
import com.nucleocore.library.database.utils.exceptions.IncorrectDataEntryObjectException;

public class DBRestore{
  public static void main(String[] args) throws ClassNotFoundException {

    String sourceKafkaHosts = System.getenv().getOrDefault("SOURCE_KAFKA_HOSTS","127.0.0.1:29092");
    String sourceTable = System.getenv().getOrDefault("SOURCE_TABLE","temptable1");

    String clazz = System.getenv().getOrDefault("TABLE_CLASS","");

    String targetKafkaHosts = System.getenv().getOrDefault("TARGET_KAFKA_HOSTS","127.0.0.1:29092");
    String targetTable = System.getenv().getOrDefault("TARGET_TABLE","restored");

    NucleoDB db = new NucleoDB();
    Serializer.log(args);
    db.launchLocalOnlyTable(sourceKafkaHosts, "temptable1", DataEntry.class, Class.forName(clazz), new StartupRun(){
      @Override
      public void run(DataTable table) {
        try {
          table.exportTo(db.launchWriteOnlyTable(targetKafkaHosts, targetTable, DataEntry.class, Class.forName(clazz)).build());
          System.exit(0);
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        } catch (IncorrectDataEntryObjectException e) {
          e.printStackTrace();
        }
      }
    }).setTableFileName("./data/" + sourceTable + ".dat").build();

  }
}
