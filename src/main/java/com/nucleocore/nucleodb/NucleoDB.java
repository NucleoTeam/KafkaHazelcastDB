package com.nucleocore.nucleodb;

import com.nucleocore.nucleodb.database.tables.DataTable;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.sql.SQLHandler;
import com.nucleocore.nucleodb.database.utils.StartupRun;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.time.Instant;
import java.util.*;

public class NucleoDB {
    private TreeMap<String, DataTable> tables = new TreeMap<>();
    static String latestSave = "";

    public <T> Object sql(String sqlStr) throws JSQLParserException {
        try {
            Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);
            if (sqlStatement instanceof Select) {
                return SQLHandler.handleSelect((Select) sqlStatement, this, null);
            }else if (sqlStatement instanceof Insert) {
                return SQLHandler.handleInsert((Insert) sqlStatement, this);
            }else if (sqlStatement instanceof Update) {
                return SQLHandler.handleUpdate((Update) sqlStatement, this);
            }else if (sqlStatement instanceof Delete) {
                return SQLHandler.handleDelete((Delete) sqlStatement, this);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public <T> Object sql(String sqlStr, Class clazz) throws JSQLParserException {
        try {
            Statement sqlStatement = CCJSqlParserUtil.parse(sqlStr);

            if (sqlStatement instanceof Select) {
                return SQLHandler.handleSelect((Select) sqlStatement, this, clazz);
            }else if (sqlStatement instanceof Insert) {
                return SQLHandler.handleInsert((Insert) sqlStatement, this);
            }else if (sqlStatement instanceof Update) {
                return SQLHandler.handleUpdate((Update) sqlStatement, this);
            }else if (sqlStatement instanceof Delete) {
                return SQLHandler.handleDelete((Delete) sqlStatement, this);
            }
        }catch (Exception e){
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
        }catch (Exception e){
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
        }catch (Exception e){
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
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public DataTable getTable(String table){
        return tables.get(table);
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, String... index){
        StartupRun startup = new StartupRun(){
            public void run(DataTable table) {
                table.consume();
            }
        };
        DataTable t = new DataTable(bootstrap, table, clazz, null, startup, index);
        tables.put(table, t);
        return t;
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, Instant toTime, String... index){
        StartupRun startup = new StartupRun(){
            public void run(DataTable table) {
                table.consume();
            }
        };
        DataTable t = new DataTable(bootstrap, table, clazz, toTime, startup, index);
        tables.put(table, t);
        return t;
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, Instant toTime, StartupRun runnable, String... index){
        DataTable t = new DataTable(bootstrap, table, clazz, toTime, runnable, index);
        tables.put(table, t);
        return t;
    }
    public DataTable launchNucleoTable(String bootstrap, String table, Class clazz, StartupRun runnable, String... index){
        DataTable t = new DataTable(bootstrap, table, clazz, null, runnable, index);
        tables.put(table, t);
        return t;
    }
}
