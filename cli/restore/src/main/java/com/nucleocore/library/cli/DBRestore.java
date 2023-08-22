package com.nucleocore.library.cli;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.Serializer;

public class DBRestore{
  public static void main(String[] args) throws ClassNotFoundException {

    String sourceKafkaHosts = System.getenv().getOrDefault("SOURCE_KAFKA_HOSTS","127.0.0.1:29092");
    String sourceTable = System.getenv().getOrDefault("SOURCE_TABLE","temptable1");
    String clazz = System.getenv().getOrDefault("TABLE_CLASS","");
    String targetKafkaHosts = System.getenv().getOrDefault("TARGET_KAFKA_HOSTS","127.0.0.1:29092");
    String targetTable = System.getenv().getOrDefault("TARGET_TABLE","restored");

    NucleoDB db = new NucleoDB();
    Serializer.log(args);
    db.launchLocalOnlyTable(sourceKafkaHosts, "temptable1", Class.forName(clazz), table->{
      try {
        table.exportTo(db.launchWriteOnlyTable(targetKafkaHosts, targetTable, Class.forName(clazz)).build());
        System.exit(0);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }).setTableFileName("./data/" + sourceTable + ".dat").build();

  }
}
