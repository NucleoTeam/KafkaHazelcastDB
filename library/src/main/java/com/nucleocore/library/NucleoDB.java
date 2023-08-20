package com.nucleocore.library;

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

    public TreeMap<String, DataTable> getTables() {
        return tables;
    }

    public DataTable getTable(String table){
        return tables.get(table);
    }

    public DataTableBuilder launchTable(String bootstrap, String table, Class clazz){
        return DataTableBuilder.create(bootstrap, table, clazz).setDb(this);
    }
    public DataTableBuilder launchTable(String bootstrap, String table, Class clazz, StartupRun runnable){
        return DataTableBuilder.create(bootstrap, table, clazz).setDb(this).setStartupRun(runnable);
    }
    public DataTableBuilder launchReadOnlyTable(String bootstrap, String table, Class clazz){
        return DataTableBuilder.createReadOnly(bootstrap, table, clazz).setDb(this);
    }
    public DataTableBuilder launchReadOnlyTable(String bootstrap, String table, Class clazz, StartupRun startupRun){
        return DataTableBuilder.createReadOnly(bootstrap, table, clazz).setDb(this).setStartupRun(startupRun);
    }
    public DataTableBuilder launchLocalOnlyTable(String bootstrap, String table, Class clazz){
        return DataTableBuilder.create(bootstrap, table, clazz).setRead(false).setWrite(false).setDb(this);
    }
    public DataTableBuilder launchLocalOnlyTable(String bootstrap, String table, Class clazz, StartupRun startupRun){
        return DataTableBuilder.create(bootstrap, table, clazz).setRead(false).setWrite(false).setDb(this).setStartupRun(startupRun);
    }
    public DataTableBuilder launchWriteOnlyTable(String bootstrap, String table, Class clazz){
        return DataTableBuilder.createWriteOnly(bootstrap, table, clazz).setLoadSave(false).setDb(this);
    }
    public DataTableBuilder launchWriteOnlyTable(String bootstrap, String table, Class clazz, StartupRun startupRun){
        return DataTableBuilder.createWriteOnly(bootstrap, table, clazz).setLoadSave(false).setDb(this).setStartupRun(startupRun);
    }
}
