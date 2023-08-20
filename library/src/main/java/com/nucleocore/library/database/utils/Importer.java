package com.nucleocore.library.database.utils;

import com.nucleocore.library.database.tables.DataTable;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvCellProcessorException;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.TreeMap;

public class Importer {
    private CellProcessor[] processors = new CellProcessor[]{};
    private TreeMap<String, String> columnToMap = new TreeMap();

    public Importer addMap(String column, String entityColumn, CellProcessor processor) {
        columnToMap.put(column, entityColumn);
        addMap(processor);
        return this;
    }

    public Importer addMap(CellProcessor processor) {
        CellProcessor[] tmp = new CellProcessor[processors.length+1];
        int i = 0;
        for (i = 0; i < processors.length; i++) {
            tmp[i] = processors[i];
        }
        tmp[i] = processor;
        processors = tmp;
        return this;
    }

    public int readIntoStream(String file, DataTable table, Class<?> clazz, CsvPreference preference) {
        int i = 0;
        ICsvMapReader mapReader = null;
        FileReader fr = null;
        try {
            fr = new FileReader(file);
            mapReader = new CsvMapReader(fr, preference);

            final String[] header = mapReader.getHeader(true);
            Map<String, Object> customerMap = null;
            try {
                customerMap = mapReader.read(header, this.processors);
            }catch (SuperCsvCellProcessorException e){
                e.printStackTrace();
            }
            table.setBuildIndex(true);
            while (customerMap!=null) {
                i++;
                Object obj = clazz.getDeclaredConstructor().newInstance();
                for (Map.Entry<String, Object> oz : customerMap.entrySet()) {
                    String key = oz.getKey();
                    Object val = oz.getValue();
                    Field field = null;
                    if (columnToMap.containsKey(key)) {
                        String newKey = columnToMap.get(key);
                        if(newKey!=null) {
                            field = clazz.getDeclaredField(newKey);
                        }
                    }else {
                        try {
                            field = clazz.getDeclaredField(key);
                        } catch (NoSuchFieldException e) {
                            //e.printStackTrace();
                        }
                    }
                    if (field != null) {
                        field.set(obj, val);
                    }

                }
                customerMap.clear();
                if(table!=null){
                    table.insert(obj);
                }
                try {
                    customerMap = mapReader.read(header, this.processors);
                }catch (SuperCsvCellProcessorException e){
                    e.printStackTrace();
                }
            }
            table.setBuildIndex(false);
            System.out.println("Finished import: "+table.getSize());
        } catch (IOException |
            NoSuchMethodException |
            IllegalAccessException |
            InvocationTargetException |
            NoSuchFieldException |
            InstantiationException e) {
            e.printStackTrace();
        } finally {
            if (mapReader != null) {
                try {

                    mapReader.close();
                    System.gc();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(fr !=null) {
                try{
                    fr.close();
                    System.gc();
                } catch (IOException e) {
                    //e.printStackTrace();
                }
            }
        }
        return i;
    }
}
