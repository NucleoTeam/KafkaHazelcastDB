package com.nucleocore.db.database.index;

import com.google.common.collect.Lists;
import com.nucleocore.db.database.index.IndexTemplate;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Utils;
import org.apache.logging.log4j.util.PropertySource;

import java.lang.reflect.Field;
import java.util.*;

public class BinaryIndex extends IndexTemplate  {
    List<DataEntry> entries = Lists.newArrayList();
    Utils.SortByElement sorter;

    public BinaryIndex indexer(Field field) {
        super.indexer(field);
        this.sorter = new Utils.SortByElement(field);
        return this;
    }

    boolean sorted = false;
    public void add(DataEntry de) {
        if(sorted) {
            synchronized (entries) {
                int val = Collections.binarySearch(entries, de, (a, b) -> {
                    try {
                        return Utils.compare(field.get(a), field.get(b));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return 0;
                });
                if(val<0)
                    entries.add((-val) - 1, de);
                else
                    entries.add(val, de);
            }
        }else
            addStartup(de);
    }
    public void addStartup(DataEntry de) {
        synchronized (entries) {
            entries.add(de);
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
                    return Utils.compare(field.get(a), field.get(b));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            });
        }
    }
    public List<DataEntry> search(DataEntry de) {
        List<DataEntry> out = new ArrayList<>();
        synchronized (entries) {
            int val = Collections.binarySearch(entries, de, (a, b)->{
                try {
                    return Utils.compare(field.get(a), field.get(b));
                }catch (Exception e){
                    e.printStackTrace();
                }
                return 0;
            });
            int len = entries.size();
            try {
                int startPos = val;
                while (startPos>=0 && startPos<len && Utils.compare(field.get(de), field.get(entries.get(startPos)))==0) {
                    out.add(entries.get(startPos));
                    startPos++;
                }
                int startNeg = val-1;
                while (startNeg>=0 && startNeg<len && Utils.compare(field.get(de), field.get(entries.get(startNeg)))==0) {
                    out.add(entries.get(startNeg));
                    startNeg--;
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return out;
    }

    public List<DataEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<DataEntry> entries) {
        this.entries = entries;
    }

    public boolean isSorted() {
        return sorted;
    }

    public void setSorted(boolean sorted) {
        this.sorted = sorted;
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
}
