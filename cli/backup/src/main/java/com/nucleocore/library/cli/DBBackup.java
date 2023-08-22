package com.nucleocore.library.cli;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.utils.Serializer;

import java.nio.file.Paths;

public class DBBackup{
  public static void main(String[] args) throws ClassNotFoundException {
    String sourceKafkaHosts = System.getenv().getOrDefault("SOURCE_KAFKA_HOSTS","127.0.0.1:29092");
    String sourceTable = System.getenv().getOrDefault("SOURCE_TABLE","temptable1");
    String clazz = System.getenv().getOrDefault("TABLE_CLASS","");
    String targetTable = System.getenv().getOrDefault("TARGET_TABLE","restored");

    NucleoDB db = new NucleoDB();
    db
        .launchReadOnlyTable(sourceKafkaHosts, sourceTable, Class.forName(clazz))
        .setTableFileName(Paths.get("./data/",  targetTable+".dat").toString())
        .setSaveChanges(true)
        .build();
  }
}
