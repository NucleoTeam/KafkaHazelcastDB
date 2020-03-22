package com.nucleocore.db.database.index.binary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.nucleocore.db.database.index.IndexTemplate;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Utils;
import java.lang.reflect.Field;
import java.util.*;

public class BinaryIndex extends IndexTemplate {
    private List<DataEntry> entries = Lists.newArrayList();
    private Utils.SortByElement sorter;

    public BinaryIndex indexOn(Field field) {
        super.indexOn(field);
        this.sorter = new Utils.SortByElement(field);
        return this;
    }

    public void add(DataEntry de) {
        synchronized (entries) {
            int val = Collections.binarySearch(entries, de, (a, b) -> {
                try {
                    return Utils.compare(this.getField().get(a), this.getField().get(b));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            });

            if(val<0) {
                entries.add((-val) - 1, de);
            }else {
                entries.add(val, de);
            }
        }
    }
    public boolean delete(DataEntry de){
        synchronized (entries) {
            entries.remove(de);
        }
        return true;
    }
    private void sort(){
        synchronized (entries) {
            Collections.sort(entries, (a, b) -> {
                try {
                    return Utils.compare(this.getField().get(a), this.getField().get(b));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            });
        }
    }
    public List<DataEntry> search(DataEntry de) {
        List<DataEntry> out = Lists.newArrayList();
        synchronized (entries) {
            int val = Collections.binarySearch(entries, de, (a, b)->{
                try {
                    return Utils.compare(this.getField().get(a), this.getField().get(b));
                }catch (Exception e){
                    e.printStackTrace();
                }
                return 0;
            });
            System.out.println("found at "+val);
            int len = entries.size();
            try {
                int startPos = val;
                while (startPos>=0 && startPos<len && Utils.compare(this.getField().get(de), this.getField().get(entries.get(startPos)))==0) {
                    out.add(entries.get(startPos));
                    startPos++;
                }
                int startNeg = val-1;
                while (startNeg>=0 && startNeg<len && Utils.compare(this.getField().get(de), this.getField().get(entries.get(startNeg)))==0) {
                    out.add(entries.get(startNeg));
                    startNeg--;
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return out;
    }

    @Override
    public boolean update(DataEntry entry) {
        return super.update(entry);
    }

    @Override
    public boolean addAll(List<DataEntry> dataEntries) {
        entries.addAll(dataEntries);
        sort();
        return false;
    }

    @Override
    public boolean reset() {
        entries.clear();
        return true;
    }

    public List<DataEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<DataEntry> entries) {
        this.entries = entries;
    }
}
