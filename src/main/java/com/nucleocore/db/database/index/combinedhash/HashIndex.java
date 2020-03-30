package com.nucleocore.db.database.index.combinedhash;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.nucleocore.db.database.index.IndexTemplate;
import com.nucleocore.db.database.index.binary.BinaryIndex;
import com.nucleocore.db.database.utils.DataEntry;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Queue;

public class HashIndex extends IndexTemplate {
    Queue<DataEntry> queue = Queues.newArrayDeque();

    public HashIndex(){
        for(int x=0;x<6;x++)
            new Thread(new QueueHandler(this)).start();
    }

    class QueueHandler implements Runnable {
        HashIndex index;
        public QueueHandler(HashIndex index){
            this.index = index;
        }
        @Override
        public void run() {
            while (true) {
                while (!queue.isEmpty()) {
                    if(index.getField()!=null) {
                        DataEntry entry;
                        synchronized (queue) {
                            entry = queue.poll();
                        }
                        if(entry!=null) {
                            try {
                                root.add(entry, index.getField().get(entry).toString());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }else{
                        System.out.println("error");
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    HashObject root;

    @Override
    public void add(DataEntry entry) {
        queue.add(entry);
    }

    @Override
    public boolean update(DataEntry entry) {
        return super.update(entry);
    }

    @Override
    public boolean delete(DataEntry entry) {
        return super.delete(entry);
    }

    @Override
    public IndexTemplate indexOn(Field field) {
        root = new HashObject(field);
        return super.indexOn(field);
    }

    @Override
    public List<DataEntry> search(DataEntry indexCheck) {
        try {
            BinaryIndex bi = root.getIndex(this.getField().get(indexCheck).toString());
            if (bi != null) {
                return bi.search(indexCheck);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean addAll(List<DataEntry> dataEntries) {
        dataEntries.stream().forEach(i -> add(i));
        return true;
    }

    public HashObject getRoot() {
        return root;
    }
}
