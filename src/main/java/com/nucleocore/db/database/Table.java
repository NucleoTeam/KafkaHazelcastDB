package com.nucleocore.db.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.modifications.Create;
import com.nucleocore.db.database.modifications.Delete;
import com.nucleocore.db.database.modifications.Update;
import com.nucleocore.db.database.utils.DataEntry;
import com.nucleocore.db.database.utils.Index;
import com.nucleocore.db.database.utils.Trie;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Table {

    private ProducerHandler producer = null;
    private ConsumerHandler consumer = null;

    private boolean writing = false;

    private Map<String, DataEntry> map;
    private TreeMap<String, TreeMap<Object, Set<DataEntry>>> index = new TreeMap<>();
    private TreeMap<String, Consumer<DataEntry>> consumers = new TreeMap<>();

    public TreeMap<String, Trie> trieIndex = new TreeMap<>();

    private TreeMap<Modification, Set<Consumer<DataEntry>>> listeners = new TreeMap<>();

    private ObjectMapper om = new ObjectMapper();

    public Table(String bootstrap, String table) {
        map = new TreeMap<>();
        if (bootstrap!=null){
            producer = new ProducerHandler(bootstrap, table);
            consumer = new ConsumerHandler(bootstrap, UUID.randomUUID().toString(), this, table);
        }
    }

    private Map<String, DataEntry> getMap() {
        return map;
    }
    public void flush(){
        getMap().clear();
        index.clear();
        consumers.clear();
    }
    private void addIndexEntries(DataEntry e, String restrictTo){
        for(Field f : e.getClass().getDeclaredFields()){
            if(restrictTo!=null && !f.getName().equals(restrictTo))
                continue;
            if(!f.isAnnotationPresent(Index.class)) {
                continue;
            }
            try {
                String name = f.getName();
                if (!index.containsKey(name))
                    index.put(name, new TreeMap<>());
                if (!trieIndex.containsKey(name))
                    trieIndex.put(name, new Trie());
                Object obj = f.get(e);
                TreeMap<Object, Set<DataEntry>> map = index.get(name);
                if(!map.containsKey(obj)){
                    map.put(obj, new HashSet<>());
                }
                System.out.println("<C, "+name+"["+obj+"] size is now "+map.get(obj).size());
                map.get(obj).add(e);
                trieIndex.get(name).add(obj, e);
                System.out.println(">C, "+name+"["+obj+"] size is now "+map.get(obj).size());
            }catch (IllegalAccessException ex){
                ex.printStackTrace();
            }
        }
    }
    private void deleteIndexEntries(DataEntry e, String restrictTo){
        for(Field f : e.getClass().getDeclaredFields()){
            if(restrictTo!=null && !f.getName().equals(restrictTo))
                continue;
            if(!f.isAnnotationPresent(Index.class))
                continue;
            try {
                String name = f.getName();
                Object obj = f.get(e);
                if(trieIndex.containsKey(name)){
                    trieIndex.get(name).remove(obj, e);
                }
                if (index.containsKey(name)) {
                    TreeMap<Object, Set<DataEntry>> map = index.get(name);
                    if(map.containsKey(obj)){
                        System.out.println("<D, "+name+"["+obj+"] size is now "+map.get(obj).size());
                        Object[] rems = (Object[])map.get(obj).parallelStream().filter(i->i.key.equals(e.getKey())).toArray();
                        for(Object rem : rems){
                            map.get(obj).remove(rem);
                        }
                        System.out.println(">D, "+name+"["+obj+"] size is now "+map.get(obj).size());
                        System.out.println(">D, "+name+" total entries "+map.size());
                        if(map.get(obj).size()==0){
                            map.remove(obj);
                            System.out.println("Removed "+obj);
                        }
                        System.out.println(">D, "+name+" total entries "+map.size());
                    }
                }
            }catch (IllegalAccessException ex){
                ex.printStackTrace();
            }
        }
    }
    public <T> T trieIndexSearch(String name, Object obj){
        try {
            if (trieIndex.containsKey(name)) {
                return (T) trieIndex.get(name).search(obj);
            }
        }catch (ClassCastException ex){
            ex.printStackTrace();
        }
        return null;
    }
    public <T> T indexSearch(String name, Object obj){
        try {
            if (index.containsKey(name)) {
                TreeMap<Object, Set<DataEntry>> map = index.get(name);
                if (map.containsKey(obj)) {
                    return (T) map.get(obj);
                }
            }
        }catch (ClassCastException ex){
            ex.printStackTrace();
        }
        return null;
    }

    public Stream<Map.Entry<String, DataEntry>> filterMap(Predicate<? super Map.Entry<String, DataEntry>> m){
        if(writing){
            return null;
        }
        return getMap().entrySet().parallelStream().filter(m);
    }
    public int size(){
        if(writing){
            return -1;
        }
        return getMap().size();
    }


    public boolean cast(Object a, Object b){
        if(a.getClass()==String.class && b.getClass()==String.class){
            return ((String)a).equals((String)b);
        }else if(a.getClass()==Integer.class && b.getClass()==Integer.class){
            return ((Integer)a) == ((Integer)b);
        }else if(a.getClass()==Long.class && b.getClass()==Long.class){
            return ((Long)a) == ((Long)b);
        }
        return false;
    }
    public synchronized boolean save(DataEntry oldEntry, DataEntry newEntry){
        return save(oldEntry, newEntry, null);
    }
    public synchronized boolean save(DataEntry oldEntry, DataEntry newEntry, Consumer<DataEntry> consumer){
        if(oldEntry == null && newEntry != null){
            try {
                Create createEntry = new Create(newEntry.getKey(), newEntry);
                if(consumer!=null){
                    consumers.put(newEntry.getKey(), consumer);
                }
                if(producer!=null)
                    producer.save(createEntry);
                else
                    modify(Modification.CREATE, createEntry);
            }catch (IOException e){
                e.printStackTrace();
            }
        }else if(newEntry == null && oldEntry != null){
            Delete deleteEntry = new Delete(oldEntry.getKey());
            if(consumer!=null){
                consumers.put(oldEntry.getKey(), consumer);
            }
            if(producer!=null)
                producer.save(deleteEntry);
            else
                modify(Modification.DELETE, deleteEntry);
        }else if(newEntry != null && oldEntry != null){
            Update updateEntry = new Update();
            try {
                updateEntry.setKey(newEntry.getKey());
                if(consumer!=null){
                    consumers.put(newEntry.getKey(), consumer);
                }
                boolean changed = false;
                updateEntry.setMasterClass(newEntry.getClass().getName());
                for (Field f : newEntry.getClass().getDeclaredFields()) {
                    if (!cast(f.get(newEntry), f.get(oldEntry))) {
                        updateEntry.getChange().put(f.getName(), f.get(newEntry));
                        changed = true;
                    }
                }
                if(changed) {
                    System.out.println("Changed");
                    if(producer!=null)
                        producer.save(updateEntry);
                    else
                        modify(Modification.UPDATE, updateEntry);
                    return true;
                }
                System.out.println("Nothing changed");
                return false;
            }catch (Exception e){
                e.printStackTrace();
                return false;
            }
        } else
            return false;
        return true;
    }

    public synchronized void modify(Modification mod, Object modification){
        switch(mod){
            case CREATE:
                Create c = (Create) modification;
                //System.out.println("Create statement called");
                if(c!=null){
                    try {
                        getMap().put(c.getKey(), c.getValue());
                        if(consumers.containsKey(c.getValue().getKey())){
                            consumers.remove(c.getValue().getKey()).accept(c.getValue());
                        }
                        addIndexEntries(c.getValue(), null);
                        fireListeners(Modification.CREATE, c.getValue());
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            break;
            case DELETE:
                Delete d = (Delete) modification;
                //System.out.println("Delete statement called");
                if(d!=null){
                    DataEntry de = getMap().remove(d.getKey());
                    if(consumers.containsKey(de.getKey())){
                        consumers.remove(de.getKey()).accept(de);
                    }
                    deleteIndexEntries(de, null);
                    fireListeners(Modification.DELETE, de);
                }
            break;
            case UPDATE:
                Update u = (Update) modification;
                //System.out.println("Update statement called");

                if(u!=null){
                    try {
                        Class clazz = Class.forName(u.getMasterClass());
                        DataEntry obj = getMap().get(u.getKey());
                        if(consumers.containsKey(obj.getKey())){
                            consumers.remove(obj.getKey()).accept(obj);
                        }
                        u.getChange().forEach((String key, Object val)->{
                            try {
                                deleteIndexEntries(obj, key);
                                clazz.getDeclaredField(key).set(obj, val);
                                addIndexEntries(obj, key);
                            } catch (Exception e){
                                e.printStackTrace();
                            }
                        });
                        fireListeners(Modification.UPDATE, obj);
                    } catch (Exception e){

                    }
                }
            break;
        }
    }
    public <T> T get(String key){
        try {
            return (T) getMap().get(key);
        }catch (ClassCastException e){
            e.printStackTrace();
        }
        return null;
    }

    public void fireListeners(Modification m, DataEntry data){
        if(listeners.containsKey(m)) {
            listeners.get(m).forEach(method -> {
                try {
                    method.accept(data);
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
    }

    public void addListener(Modification m, Consumer<DataEntry> method){
        if(!listeners.containsKey(m)){
            listeners.put(m, new HashSet<>());
        }
        listeners.get(m).add(method);
    }

    public boolean isWriting() {
        return writing;
    }

    public void setWriting(boolean writing) {
        this.writing = writing;
    }
}
