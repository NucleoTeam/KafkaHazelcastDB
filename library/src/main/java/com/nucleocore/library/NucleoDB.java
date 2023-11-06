package com.nucleocore.library;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Queues;
import com.nucleocore.library.database.tables.ConnectionHandler;
import com.nucleocore.library.database.tables.annotation.Index;
import com.nucleocore.library.database.tables.annotation.Relationship;
import com.nucleocore.library.database.tables.annotation.Relationships;
import com.nucleocore.library.database.tables.annotation.Table;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.TreeSetExt;
import com.nucleocore.library.database.utils.index.TreeIndex;
import com.nucleocore.library.database.utils.sql.SQLHandler;
import com.nucleocore.library.database.tables.DataTable;
import com.nucleocore.library.database.tables.DataTableBuilder;
import com.nucleocore.library.database.utils.DataEntry;
import com.nucleocore.library.database.utils.StartupRun;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.reflections.Reflections;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class NucleoDB{
  private TreeMap<String, DataTable> tables = new TreeMap<>();
  static String latestSave = "";

  private ConnectionHandler connectionHandler = new ConnectionHandler(this);

  public NucleoDB() {
  }

  public class TableData{

  }

  public NucleoDB(String bootstrap, String packageToScan) {
    Set<Class<?>> types = new Reflections(packageToScan).getTypesAnnotatedWith(Table.class);
    CountDownLatch latch = new CountDownLatch(types.size());
    Set<DataTableBuilder> tables = new TreeSetExt<>();
    Map<String, Set<String>> indexes = new TreeMap<>();

    types.stream().forEach(type->{
      String tableName = type.getAnnotation(Table.class).value();
      processTableClass(tableName, indexes, type);
      tables.add(launchTable(bootstrap, tableName, type, new StartupRun(){
        public void run(DataTable table) {
          latch.countDown();
        }
      }));
    });
    tables.stream().forEach(table->{
      table.setIndexes(indexes.get(table.getConfig().getTable()).toArray(new String[0]));
      table.build();
    });
    try {
      latch.await();
      System.out.println("NucleoDB Started");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

  public Set<DataEntry> getAllRelated(DataEntry dataEntry) {
    Set<DataEntry> set = new TreeSetExt<>();
    if (dataEntry.getData().getClass().isAnnotationPresent(Table.class)) {
      Table localTable = dataEntry.getData().getClass().getAnnotation(Table.class);
      String localTableName = localTable.value();
      for (Relationship relationship : getRelationships(dataEntry.getData().getClass())) {
        getRelatedByRelationship(dataEntry, set, localTableName, relationship);
      }
    }
    return set;
  }

  public Set<DataEntry> getRelated(DataEntry dataEntry, Class clazz) {
    Set<DataEntry> set = new TreeSetExt<>();
    if (dataEntry.getData().getClass().isAnnotationPresent(Table.class)) {
      Table localTable = dataEntry.getData().getClass().getAnnotation(Table.class);
      String localTableName = localTable.value();
      for (Relationship relationship : getRelationships(dataEntry.getData().getClass())) {
        if (relationship.clazz() == clazz) {
          getRelatedByRelationship(dataEntry, set, localTableName, relationship);
        } else {
          //Serializer.log("Relation ignored");
        }
      }
    }
    return set;
  }

  public Set<DataEntry> getRelatedRemote(DataEntry dataEntry, Class clazz, String index) {
    Set<DataEntry> set = new TreeSetExt<>();
    if (dataEntry.getData().getClass().isAnnotationPresent(Table.class)) {
      Table localTable = dataEntry.getData().getClass().getAnnotation(Table.class);
      String localTableName = localTable.value();
      for (Relationship relationship : getRelationships(dataEntry.getData().getClass())) {
        if (relationship.clazz() == clazz && index.equals(relationship.remoteKey())) {
          getRelatedByRelationship(dataEntry, set, localTableName, relationship);
        } else {
          //Serializer.log("Relation ignored");
        }
      }
    }
    return set;
  }

  public Set<DataEntry> getRelatedLocal(DataEntry dataEntry, Class clazz, String index) {
    Set<DataEntry> set = new TreeSetExt<>();
    if (dataEntry.getData().getClass().isAnnotationPresent(Table.class)) {
      Table localTable = dataEntry.getData().getClass().getAnnotation(Table.class);
      String localTableName = localTable.value();
      for (Relationship relationship : getRelationships(dataEntry.getData().getClass())) {
        if (relationship.clazz() == clazz && index.equals(relationship.localKey())) {
          getRelatedByRelationship(dataEntry, set, localTableName, relationship);
        } else {
          //Serializer.log("Relation ignored");
        }
      }
    }
    return set;
  }

  private void getRelatedByRelationship(DataEntry dataEntry, Set<DataEntry> set, String localTableName, Relationship relationship) {
    if (relationship.clazz().isAnnotationPresent(Table.class)) {
      Table remoteTable = (Table) relationship.clazz().getAnnotation(Table.class);
      String remoteTableName = remoteTable.value();
      //Serializer.log("getting for relationship from " + localTableName + " to " + remoteTableName);
      try {
        //Serializer.log(relationship.localKey());
        List<Object> values = new TreeIndex().getValues(Queues.newLinkedBlockingDeque(Arrays.asList(relationship.localKey().split("\\."))), dataEntry.getData());
        if (values.size() > 0) {
          for (Object value : values) {
            set.addAll(this.getTable(remoteTableName).get(relationship.remoteKey(), value));
          }
        }
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
    } else {
      Serializer.log("Target class not a NucleoDB Table.");
    }

  }

  private void processRelationship(String tableName, Map<String, Set<String>> indexes, Relationship relationship) {
    indexes.get(tableName).add(relationship.localKey());
    if (relationship.clazz().isAnnotationPresent(Table.class)) {
      String tableNameRemote = ((Table) relationship.clazz().getAnnotation(Table.class)).value();
      if (!indexes.containsKey(tableNameRemote)) {
        processTableClass(tableNameRemote, indexes, relationship.clazz());
      }
      indexes.get(tableNameRemote).add(relationship.remoteKey());
    }
  }

  private List<Relationship> getRelationships(Class<?> clazz) {
    List<Relationship> relationships = new LinkedList<>();
    if (clazz.isAnnotationPresent(Relationship.class)) {
      Relationship relationship = clazz.getAnnotation(Relationship.class);
      relationships.add(relationship);
    }
    if (clazz.isAnnotationPresent(Relationships.class)) {
      Relationships relationship = clazz.getAnnotation(Relationships.class);
      relationships.addAll(Arrays.stream(relationship.value()).toList());
    }
    return relationships;
  }

  private void processTableClass(String tableName, Map<String, Set<String>> indexes, Class<?> clazz) {
    if (!indexes.containsKey(tableName)) {
      indexes.put(tableName, new TreeSetExt<>());
    }
    for (Field declaredField : clazz.getDeclaredFields()) {
      if (declaredField.isAnnotationPresent(Index.class)) {
        Index i = declaredField.getAnnotation(Index.class);
        if (i.value().isEmpty()) {
          indexes.get(tableName).addAll(Arrays.asList(declaredField.getName().toLowerCase()));
        } else {
          indexes.get(tableName).addAll(Arrays.asList(i.value()));
        }
      }
    }

    for (Relationship relationship : getRelationships(clazz)) {
      processRelationship(tableName, indexes, relationship);
    }
  }

  public <T> Object sql(String sqlStr) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);
      if (sqlStatement instanceof Select) {
        return SQLHandler.handleSelect((Select) sqlStatement, this, null);
      } else if (sqlStatement instanceof Insert) {
        return SQLHandler.handleInsert((Insert) sqlStatement, this);
      } else if (sqlStatement instanceof Update) {
        return SQLHandler.handleUpdate((Update) sqlStatement, this);
      } else if (sqlStatement instanceof Delete) {
        return SQLHandler.handleDelete((Delete) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public <T> Object sql(String sqlStr, Class clazz) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);

      if (sqlStatement instanceof Select) {
        return SQLHandler.handleSelect((Select) sqlStatement, this, clazz);
      } else if (sqlStatement instanceof Insert) {
        return SQLHandler.handleInsert((Insert) sqlStatement, this);
      } else if (sqlStatement instanceof Update) {
        return SQLHandler.handleUpdate((Update) sqlStatement, this);
      } else if (sqlStatement instanceof Delete) {
        return SQLHandler.handleDelete((Delete) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public <T> List<T> select(String sqlStr, Class clazz) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);

      if (sqlStatement instanceof Select) {
        return SQLHandler.handleSelect((Select) sqlStatement, this, clazz);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public DataEntry insert(String sqlStr) throws JSQLParserException {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);
      if (sqlStatement instanceof Insert) {
        return SQLHandler.handleInsert((Insert) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public boolean update(String sqlStr) {
    try {
      Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);
      if (sqlStatement instanceof Update) {
        return SQLHandler.handleUpdate((Update) sqlStatement, this);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public TreeMap<String, DataTable> getTables() {
    return tables;
  }

  public DataTable getTable(String table) {
    return tables.get(table);
  }

  public DataTableBuilder launchTable(String bootstrap, String table, Class clazz) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDb(this);
  }

  public DataTableBuilder launchTable(String bootstrap, String table, Class clazz, StartupRun runnable) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDb(this).setStartupRun(runnable);
  }

  public DataTableBuilder launchReadOnlyTable(String bootstrap, String table, Class clazz) {
    return DataTableBuilder.createReadOnly(bootstrap, table, clazz).setDb(this);
  }

  public DataTableBuilder launchReadOnlyTable(String bootstrap, String table, Class clazz, StartupRun startupRun) {
    return DataTableBuilder.createReadOnly(bootstrap, table, clazz).setDb(this).setStartupRun(startupRun);
  }

  public DataTableBuilder launchLocalOnlyTable(String bootstrap, String table, Class clazz) {
    return DataTableBuilder.create(bootstrap, table, clazz).setRead(false).setWrite(false).setDb(this);
  }

  public DataTableBuilder launchLocalOnlyTable(String bootstrap, String table, Class clazz, StartupRun startupRun) {
    return DataTableBuilder.create(bootstrap, table, clazz).setRead(false).setWrite(false).setDb(this).setStartupRun(startupRun);
  }

  public DataTableBuilder launchWriteOnlyTable(String bootstrap, String table, Class clazz) {
    return DataTableBuilder.createWriteOnly(bootstrap, table, clazz).setLoadSave(false).setDb(this);
  }

  public DataTableBuilder launchWriteOnlyTable(String bootstrap, String table, Class clazz, StartupRun startupRun) {
    return DataTableBuilder.createWriteOnly(bootstrap, table, clazz).setLoadSave(false).setDb(this).setStartupRun(startupRun);
  }

  public ConnectionHandler getConnectionHandler() {
    return connectionHandler;
  }

  public void setConnectionHandler(ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
  }
}
