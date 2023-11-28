package com.nucleocore.library;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Queues;
import com.nucleocore.library.database.modifications.Create;
import com.nucleocore.library.database.tables.annotation.Conn;
import com.nucleocore.library.database.tables.connection.Connection;
import com.nucleocore.library.database.tables.connection.ConnectionConfig;
import com.nucleocore.library.database.tables.connection.ConnectionHandler;
import com.nucleocore.library.database.tables.annotation.Index;
import com.nucleocore.library.database.tables.annotation.Relationship;
import com.nucleocore.library.database.tables.annotation.Relationships;
import com.nucleocore.library.database.tables.annotation.Table;
import com.nucleocore.library.database.utils.TreeSetExt;
import com.nucleocore.library.database.utils.exceptions.IncorrectDataEntryClassException;
import com.nucleocore.library.database.utils.exceptions.MissingDataEntryConstructorsException;
import com.nucleocore.library.database.utils.index.TreeIndex;
import com.nucleocore.library.database.utils.sql.SQLHandler;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.tables.table.DataTableBuilder;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.utils.StartupRun;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class NucleoDB{
  private static Logger logger = Logger.getLogger(DataTable.class.getName());
  private TreeMap<String, DataTable> tables = new TreeMap<>();
  static String latestSave = "";

  private TreeMap<String, ConnectionHandler> connections = new TreeMap<>();

  public NucleoDB() {
  }


  public enum DBType{
    NO_LOCAL,
    EXPORT,
    ALL;
  }

  public NucleoDB(String bootstrap, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
    this(bootstrap, DBType.ALL, packagesToScan);
  }

  public NucleoDB(String bootstrap, DBType dbType, String... packagesToScan) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
    startTables(bootstrap, packagesToScan, dbType);
    startConnections(bootstrap, packagesToScan, dbType);
  }
  private void startConnections(String bootstrap, String[] packagesToScan, DBType dbType){
    logger.info("NucleoDB Connections Starting");
    Optional<Set<Class<?>>> connectionTypesOptional = Arrays.stream(packagesToScan).map(packageToScan->new Reflections(packageToScan).getTypesAnnotatedWith(Conn.class)).reduce((a, b)->{
      a.addAll(b);
      return a;
    });
    if(!connectionTypesOptional.isPresent()){
      return;
    }
    Set<Class<?>> connectionTypes = connectionTypesOptional.get();
    CountDownLatch latch = new CountDownLatch(connectionTypes.size());
    for (Class<?> type : connectionTypes) {
      Conn connectionType = type.getAnnotation(Conn.class);
      String topic = String.format("%ss",connectionType.value().toLowerCase());
      ConnectionConfig config = new ConnectionConfig();
      config.setBootstrap(bootstrap);
      config.setTopic(topic);

      Type[] actualTypeArguments = ((ParameterizedType) type.getGenericSuperclass()).getActualTypeArguments();
      if(actualTypeArguments.length==2) {
        Class<?> toTable = (Class<?>) actualTypeArguments[0];
        Type[] toTableTypeArguments = ((ParameterizedType) toTable.getGenericSuperclass()).getActualTypeArguments();
        if(toTableTypeArguments.length==1) {
          config.setToTable((Class<?>) toTableTypeArguments[0]);
          logger.info("To Table " + config.getToTable().getName());
        }
        Class<?> fromTable = (Class<?>) actualTypeArguments[1];
        Type[] fromTableTypeArguments = ((ParameterizedType) fromTable.getGenericSuperclass()).getActualTypeArguments();
        if(fromTableTypeArguments.length==1) {
          config.setFromTable((Class<?>) fromTableTypeArguments[0]);
          logger.info("From table " + config.getFromTable().getName());
        }
      }
      config.setConnectionClass(type);
      config.setLabel(connectionType.value().toUpperCase());
      config.setStartupRun(new StartupRun(){
        public void run(ConnectionHandler connectionHandler) {
          latch.countDown();
        }
      });
      switch (dbType) {
        case NO_LOCAL -> {
          config.setSaveChanges(false);
          config.setLoadSaved(false);
        }
        case EXPORT -> config.setJsonExport(true);
      }
      connections.put(connectionType.value().toUpperCase(), new ConnectionHandler(this, config));
    }

    try {

      latch.await();
      logger.info("NucleoDB Connections Started");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  private void startTables(String bootstrap, String[] packagesToScan, DBType dbType) throws IncorrectDataEntryClassException, MissingDataEntryConstructorsException {
    logger.info("NucleoDB Tables Starting");
    Optional<Set<Class<?>>> tableTypesOptional = Arrays.stream(packagesToScan).map(packageToScan->new Reflections(packageToScan).getTypesAnnotatedWith(Table.class)).reduce((a, b)->{
      a.addAll(b);
      return a;
    });
    if(!tableTypesOptional.isPresent()){
      return;
    }
    Set<Class<?>> tableTypes = tableTypesOptional.get();
    CountDownLatch latch = new CountDownLatch(tableTypes.size());
    Set<DataTableBuilder> tables = new TreeSetExt<>();
    Map<String, Set<String>> indexes = new TreeMap<>();
    for (Class<?> type : tableTypes) {
      Table tableAnnotation = type.getAnnotation(Table.class);
      String tableName = tableAnnotation.tableName();
      Class dataEntryClass = tableAnnotation.dataEntryClass();
      if(!DataEntry.class.isAssignableFrom(dataEntryClass)){
        throw new IncorrectDataEntryClassException(String.format("%s does not extend DataEntry", dataEntryClass.getName()));
      }
      try {
        if(dataEntryClass!= DataEntry.class)
          dataEntryClass.getDeclaredConstructor(type);
        dataEntryClass.getDeclaredConstructor(Create.class);
        dataEntryClass.getDeclaredConstructor();
        dataEntryClass.getDeclaredConstructor(String.class);
      } catch (NoSuchMethodException e) {
        throw new MissingDataEntryConstructorsException(String.format("%s does not have all DataEntry constructors overridden!", dataEntryClass.getName()), e);
      }
      processTableClass(tableName, indexes, type);
      switch (dbType) {
        case ALL -> tables.add(launchTable(bootstrap, tableName, dataEntryClass, type, new StartupRun(){
          public void run(DataTable table) {
            latch.countDown();
          }
        }));
        case NO_LOCAL -> tables.add(launchLocalOnlyTable(bootstrap, tableName, dataEntryClass, type, new StartupRun(){
          public void run(DataTable table) {
            latch.countDown();
          }
        }));
        case EXPORT -> tables.add(launchExportOnlyTable(bootstrap, tableName, dataEntryClass, type, new StartupRun(){
          public void run(DataTable table) {
            latch.countDown();
          }
        }));
      }
    }
    tables.stream().forEach(table -> {
      table.setIndexes(indexes.get(table.getConfig().getTable()).toArray(new String[0]));
      table.build();
    });
    try {

      latch.await();
      logger.info("NucleoDB Tables Started");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Set<DataEntry> getAllRelated(DataEntry dataEntry) {
    Set<DataEntry> set = new TreeSetExt<>();
    if (dataEntry.getData().getClass().isAnnotationPresent(Table.class)) {
      Table localTable = dataEntry.getData().getClass().getAnnotation(Table.class);
      String localTableName = localTable.tableName();
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
      String localTableName = localTable.tableName();
      for (Relationship relationship : getRelationships(dataEntry.getData().getClass())) {
        if (relationship.clazz() == clazz) {
          getRelatedByRelationship(dataEntry, set, localTableName, relationship);
        } else {
          //logger.info("Relation ignored");
        }
      }
    }
    return set;
  }

  public Set<DataEntry> getRelatedRemote(DataEntry dataEntry, Class clazz, String index) {
    Set<DataEntry> set = new TreeSetExt<>();
    if (dataEntry.getData().getClass().isAnnotationPresent(Table.class)) {
      Table localTable = dataEntry.getData().getClass().getAnnotation(Table.class);
      String localTableName = localTable.tableName();
      for (Relationship relationship : getRelationships(dataEntry.getData().getClass())) {
        if (relationship.clazz() == clazz && index.equals(relationship.remoteKey())) {
          getRelatedByRelationship(dataEntry, set, localTableName, relationship);
        } else {
          //logger.info("Relation ignored");
        }
      }
    }
    return set;
  }

  public Set<DataEntry> getRelatedLocal(DataEntry dataEntry, Class clazz, String index) {
    Set<DataEntry> set = new TreeSetExt<>();
    if (dataEntry.getData().getClass().isAnnotationPresent(Table.class)) {
      Table localTable = dataEntry.getData().getClass().getAnnotation(Table.class);
      String localTableName = localTable.tableName();
      for (Relationship relationship : getRelationships(dataEntry.getData().getClass())) {
        if (relationship.clazz() == clazz && index.equals(relationship.localKey())) {
          getRelatedByRelationship(dataEntry, set, localTableName, relationship);
        } else {
          //logger.info("Relation ignored");
        }
      }
    }
    return set;
  }

  private void getRelatedByRelationship(DataEntry dataEntry, Set<DataEntry> set, String localTableName, Relationship relationship) {
    if (relationship.clazz().isAnnotationPresent(Table.class)) {
      Table remoteTable = (Table) relationship.clazz().getAnnotation(Table.class);
      String remoteTableName = remoteTable.tableName();
      //logger.info("getting for relationship from " + localTableName + " to " + remoteTableName);
      try {
        //logger.info(relationship.localKey());
        List<Object> values = new TreeIndex().getValues(Queues.newLinkedBlockingDeque(Arrays.asList(relationship.localKey().split("\\."))), dataEntry.getData());
        if (values.size() > 0) {
          for (Object value : values) {
            set.addAll(this.getTable(remoteTableName).get(relationship.remoteKey(), value, null));
          }
        }
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
    } else {
      logger.info("Target class not a NucleoDB Table.");
    }

  }

  private void processRelationship(String tableName, Map<String, Set<String>> indexes, Relationship relationship) {
    indexes.get(tableName).add(relationship.localKey());
    if (relationship.clazz().isAnnotationPresent(Table.class)) {
      String tableNameRemote = ((Table) relationship.clazz().getAnnotation(Table.class)).tableName();
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

  public DataTable getTable(Class clazz) {
    Annotation annotation = clazz.getAnnotation(Table.class);
    if(annotation!=null) {
      return tables.get(((Table)annotation).tableName());
    }
    return null;
  }

  public DataTableBuilder launchTable(String bootstrap, String table, Class dataEntryClass, Class clazz) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setDb(this);
  }

  public DataTableBuilder launchTable(String bootstrap, String table, Class dataEntryClass, Class clazz, StartupRun runnable) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setDb(this).setStartupRun(runnable);
  }

  public DataTableBuilder launchReadOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz) {
    return DataTableBuilder.createReadOnly(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setDb(this);
  }

  public DataTableBuilder launchReadOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz, StartupRun startupRun) {
    return DataTableBuilder.createReadOnly(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setDb(this).setStartupRun(startupRun);
  }

  public DataTableBuilder launchLocalOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setSaveChanges(false).setDb(this);
  }

  public DataTableBuilder launchLocalOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz, StartupRun startupRun) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setSaveChanges(false).setDb(this).setStartupRun(startupRun);
  }

  public DataTableBuilder launchExportOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setJSONExport(true).setDb(this);
  }
  public DataTableBuilder launchExportOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz, StartupRun startupRun) {
    return DataTableBuilder.create(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setJSONExport(true).setDb(this).setStartupRun(startupRun);
  }

  public DataTableBuilder launchWriteOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz) {
    return DataTableBuilder.createWriteOnly(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setDb(this);
  }

  public DataTableBuilder launchWriteOnlyTable(String bootstrap, String table, Class dataEntryClass, Class clazz, StartupRun startupRun) {
    return DataTableBuilder.createWriteOnly(bootstrap, table, clazz).setDataEntryClass(dataEntryClass).setLoadSave(false).setDb(this).setStartupRun(startupRun);
  }

  public ConnectionHandler getConnectionHandler(Class clazz) {
    if(!clazz.isAnnotationPresent(Conn.class)){
      return null;
    }
    Conn conn = (Conn) clazz.getDeclaredAnnotation(Conn.class);
    return connections.get(conn.value().toUpperCase());
  }

  public TreeMap<String, ConnectionHandler> getConnections() {
    return connections;
  }

}
