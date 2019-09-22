package com.nucleocore.db.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nucleocore.db.database.modifications.Create;
import com.nucleocore.db.database.modifications.Delete;
import com.nucleocore.db.database.modifications.Update;
import com.nucleocore.db.database.utils.*;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataTable implements TableTemplate {

  private ProducerHandler producer = null;
  private ConsumerHandler consumer = null;

  private HashMap<String, DataEntry> map = new HashMap<>();
  private HashMap<String, TreeMap<Object, List<String>>> index = new HashMap<>();
  private HashMap<String, Consumer<DataEntry>> consumers = new HashMap<>();
  public HashMap<String, Trie> trieIndex = new HashMap<>();
  private SetIndex setIndex;

  private HashMap<Modification, Set<Consumer<DataEntry>>> listeners = new HashMap<>();

  private ObjectMapper om = new ObjectMapper();

  private int size = 0;
  private boolean buildIndex = false;
  private Stack<DataEntry> importList = new Stack<>();

  public DataTable(String bootstrap, String table) {
    setIndex = new SetIndex();
    if (bootstrap != null) {
      producer = new ProducerHandler(bootstrap, table);
      consumer = new ConsumerHandler(bootstrap, UUID.randomUUID().toString(), this, table);
    }
  }

  public Map<String, DataEntry> getMap() {
    return map;
  }

  public void flush() {
    try {
      synchronized (map) {
        map.forEach((i, it) -> {
          this.save(it, null);
        });
      }
    }catch (Exception e){
      //e.printStackTrace();
    }
    map = new HashMap<>();
    index = new HashMap<>();
    consumers = new HashMap<>();
    trieIndex = new HashMap<>();
    listeners = new HashMap<>();
    System.gc();
  }

  private void addIndexEntries(DataEntry e, String restrictTo) {
    for (Field f : e.getClass().getDeclaredFields()) {
      if (restrictTo != null && !f.getName().equals(restrictTo))
        continue;
      if (!f.isAnnotationPresent(Index.class)) {
        continue;
      }
      IndexType indexType = ((Index) f.getAnnotation(Index.class)).value();
      try {
        String name = f.getName();
        Object obj = f.get(e);
        if (obj == null) {
          continue;
        }
        switch (indexType) {
          case TRIE:
            synchronized (trieIndex) {
              if (!trieIndex.containsKey(name))
                trieIndex.put(name, new Trie());
            }
            trieIndex.get(name).add(obj, e.getKey());
            break;
          case HASH:
            TreeMap<Object, List<String>> map;
            synchronized (index) {
              if (!index.containsKey(name))
                index.put(name, new TreeMap<>());
              map = index.get(name);
              if (!map.containsKey(obj)) {
                map.put(obj, new ArrayList<>());
              }
              map.get(obj).add(e.getKey());
            }
            //System.out.println("<C, "+name+"["+obj+"] size is now "+map.get(obj).size());

            //System.out.println(">C, "+name+"["+obj+"] size is now "+map.get(obj).size());
            break;
          case SETFULLTEXT:
            synchronized (setIndex) {
              setIndex.add(name, obj.toString(), Integer.valueOf(e.getKey()));
            }
            break;
        }
      } catch (IllegalAccessException ex) {
        ex.printStackTrace();
      }
    }
  }

  private void deleteIndexEntries(DataEntry e, String restrictTo) {
    for (Field f : e.getClass().getDeclaredFields()) {
      if (restrictTo != null && !f.getName().equals(restrictTo))
        continue;
      if (!f.isAnnotationPresent(Index.class))
        continue;
      IndexType indexType = ((Index) f.getAnnotation(Index.class)).value();
      try {
        String name = f.getName();
        Object obj = f.get(e);
        switch (indexType) {
          case TRIE:
            synchronized (trieIndex) {
              if (trieIndex.containsKey(name)) {
                trieIndex.get(name).remove(obj, e.getKey());
              }
            }
            break;
          case HASH:
            synchronized (index) {
              if (index.containsKey(name)) {
                TreeMap<Object, List<String>> map = index.get(name);
                if (map.containsKey(obj)) {
                  //System.out.println("<D, "+name+"["+obj+"] size is now "+map.get(obj).size());
                  Stream<String> stream = map.get(obj).stream();
                  Object[] rems = (Object[]) stream.filter(i -> i.equals(e.getKey())).toArray();
                  stream.close();
                  for (Object rem : rems) {
                    map.get(obj).remove(rem);
                  }
                  //System.out.println(">D, "+name+"["+obj+"] size is now "+map.get(obj).size());
                  //System.out.println(">D, "+name+" total entries "+map.size());
                  if (map.get(obj).size() == 0) {
                    map.remove(obj);
                  }
                  //System.out.println(">D, "+name+" total entries "+map.size());
                }
              }
            }
            break;
          case SETFULLTEXT:
            synchronized (setIndex) {
              setIndex.delete(name, obj.toString(), Integer.valueOf(e.key));
            }
            break;
        }
      } catch (IllegalAccessException ex) {
        ex.printStackTrace();
      }
    }
  }

  public <T> Set<T> search(String name, Object obj, Class clazz) {
    try {
      Field f = clazz.getDeclaredField(name);
      IndexType indexType = ((Index)f.getAnnotation(Index.class)).value();
      switch (indexType) {
        case TRIE:
          synchronized (trieIndex) {
            Set<DataEntry> tmpList = new HashSet<>();
            List<String> listX = trieIndex.get(name).search(obj);
            if (listX != null) {
              for (String key : listX) {
                DataEntry de = map.get(key);
                if (de != null)
                  tmpList.add(de);
              }
            }
            return (Set<T>) tmpList;
          }
        case HASH:
          synchronized (index) {
            TreeMap<Object, List<String>> mapX = index.get(name);
            if(mapX==null){
              return null;
            }
            if (mapX.containsKey(obj)) {
              Set<DataEntry> tmpList = new HashSet<>();
              for (String key : mapX.get(obj)) {
                DataEntry de = map.get(key);
                if (de != null)
                  tmpList.add(de);
              }
              return (Set<T>) tmpList;
            }
          }
          break;
        case SETFULLTEXT:
          synchronized (setIndex) {
            int len = obj.toString().length();
            Set<DataEntry> tmpList = new HashSet<>();
            Set<Integer> mapX = setIndex.search(name, obj.toString(), 0);
            if(mapX==null){
              return null;
            }
            for (Integer key : mapX) {
              DataEntry de = map.get(key + "");
              if (de != null)
                tmpList.add(de);
            }
            Stream stream = tmpList.stream();
            Set<T> data = (Set<T>) stream.filter(x->{
              try {
                return cast(f.get(x), obj);
              }catch (Exception c){}
              return false;
            }).collect(Collectors.toSet());
            stream.close();
            return data;
          }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  public DataEntry searchOne(String name, Object obj, Class clazz) {
    Set<DataEntry> tmp = search(name, obj, clazz);
    if (tmp != null && tmp.size() > 0) {
      return (DataEntry) tmp.toArray()[0];
    }
    return null;
  }

  public <T> Set<T> in(String name, Set<Object> objs, Class clazz) {
    List<DataEntry> tmp = new ArrayList<>();
    try {
      Field f = clazz.getDeclaredField(name);
      IndexType indexType = ((Index)f.getAnnotation(Index.class)).value();
      for (Object obj : objs) {
        if (trieIndex.containsKey(name)) {
          synchronized (trieIndex) {
            List<String> tmpListKeys;
            if ((tmpListKeys = trieIndex.get(name).search(obj)) != null) {
              for (String key : tmpListKeys) {
                DataEntry de = map.get(key);
                if (de != null)
                  tmp.add(de);
              }
            }
          }
        } else if (index.containsKey(name)) {
          synchronized (index) {
            TreeMap<Object, List<String>> mapX = index.get(name);
            if (mapX.containsKey(obj)) {
              List<String> tmpListKeys;
              if ((tmpListKeys = mapX.get(obj)) != null) {
                for (String key : tmpListKeys) {
                  DataEntry de = map.get(key);
                  if (de != null)
                    tmp.add(de);
                }
              }
            }
          }
        }
      }
    } catch (ClassCastException | NoSuchFieldException ex) {
      ex.printStackTrace();
    }
    return (Set<T>) tmp;
  }

  public <T> T inOne(String name, Set<Object> obj, Class clazz) {
    Set<T> tmp = in(name, obj, clazz);
    if (tmp != null && tmp.size() > 0) {
      return (T) tmp.toArray()[0];
    }
    return null;
  }

  public Stream<Map.Entry<String, DataEntry>> filterMap(Predicate<? super Map.Entry<String, DataEntry>> m) {
    synchronized (map) {
      return map.entrySet().stream().filter(m);
    }
  }

  public int size() {
    return size;
  }


  public boolean cast(Object a, Object b) {
    if (a.getClass() == String.class && b.getClass() == String.class) {
      return ((String) a).equals((String) b);
    } else if (a.getClass() == Integer.class && b.getClass() == Integer.class) {
      return ((Integer) a) == ((Integer) b);
    } else if (a.getClass() == Long.class && b.getClass() == Long.class) {
      return ((Long) a) == ((Long) b);
    }
    return false;
  }

  public synchronized boolean save(DataEntry oldEntry, DataEntry newEntry) {
    return save(oldEntry, newEntry, null);
  }

  public synchronized boolean save(DataEntry oldEntry, DataEntry newEntry, Consumer<DataEntry> consumer) {
    if (oldEntry == null && newEntry != null) {
      try {
        Create createEntry = new Create(newEntry.getKey(), newEntry);
        if (consumer != null) {
          consumers.put(newEntry.getKey(), consumer);
        }
        if (producer != null)
          producer.save(createEntry);
        else
          modify(Modification.CREATE, createEntry);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else if (newEntry == null && oldEntry != null) {
      Delete deleteEntry = new Delete(oldEntry.getKey());
      if (consumer != null) {
        consumers.put(oldEntry.getKey(), consumer);
      }
      if (producer != null)
        producer.save(deleteEntry);
      else
        modify(Modification.DELETE, deleteEntry);
    } else if (newEntry != null && oldEntry != null) {
      Update updateEntry = new Update();
      try {
        updateEntry.setKey(newEntry.getKey());
        if (consumer != null) {
          consumers.put(newEntry.getKey(), consumer);
        }
        boolean changed = false;
        updateEntry.setMasterClass(newEntry.getClass().getName());
        for (Field f : newEntry.getClass().getDeclaredFields()) {
          if (!cast(f.get(newEntry), f.get(oldEntry))) {
            if (f.get(newEntry) != null) {
              updateEntry.getChange().put(f.getName(), f.get(newEntry));
              changed = true;
            }
          }
        }
        if (changed) {
          System.out.println("Changed");
          if (producer != null)
            producer.save(updateEntry);
          else
            modify(Modification.UPDATE, updateEntry);
          return true;
        }
        System.out.println("Nothing changed");
        return false;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    } else
      return false;
    return true;
  }

  public synchronized void modify(Modification mod, Object modification) {
    switch (mod) {
      case CREATE:
        Create c = (Create) modification;
        //System.out.println("Create statement called");
        if (c != null) {
          try {
            synchronized (map) {
              map.put(c.getKey(), c.getValue());
            }
            size++;
            if (consumers.containsKey(c.getValue().getKey())) {
              consumers.remove(c.getValue().getKey()).accept(c.getValue());
            }
            addIndexEntries(c.getValue(), null);
            fireListeners(Modification.CREATE, c.getValue());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        break;
      case DELETE:
        Delete d = (Delete) modification;
        //System.out.println("Delete statement called");
        if (d != null) {
          DataEntry de;
          synchronized (map) {
             de = map.remove(d.getKey());
          }
          size--;
          if (consumers.containsKey(de.getKey())) {
            consumers.remove(de.getKey()).accept(de);
          }
          deleteIndexEntries(de, null);
          fireListeners(Modification.DELETE, de);
        }
        break;
      case UPDATE:
        Update u = (Update) modification;
        //System.out.println("Update statement called");

        if (u != null) {
          try {
            Class clazz = Class.forName(u.getMasterClass());
            DataEntry obj = getMap().get(u.getKey());
            if (consumers.containsKey(obj.getKey())) {
              consumers.remove(obj.getKey()).accept(obj);
            }
            u.getChange().forEach((String key, Object val) -> {
              try {
                deleteIndexEntries(obj, key);
                clazz.getDeclaredField(key).set(obj, val);
                addIndexEntries(obj, key);
              } catch (Exception e) {
                e.printStackTrace();
              }
            });
            fireListeners(Modification.UPDATE, obj);
          } catch (Exception e) {

          }
        }
        break;
    }
  }

  public <T> T get(String key) {
    try {
      return (T) getMap().get(key);
    } catch (ClassCastException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void fireListeners(Modification m, DataEntry data) {
    if (listeners.containsKey(m)) {
      listeners.get(m).forEach(method -> {
        try {
          method.accept(data);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }
  }

  public void multiImport(DataEntry newEntry) {
    synchronized (importList) {
      importList.add(newEntry);
    }
  }

  @Override
  public void updateIndex(Class clazz) {

  }

  List<Thread> threads = new ArrayList<>();

  public void startImportThreads() {
    for (int i = 0; i < 25; i++) {
      Thread t = new Thread(() -> {
        while (!Thread.interrupted()) {
          DataEntry de = null;
          synchronized (importList) {
            if (importList.size() > 0)
              de = importList.pop();
          }
          if (de != null) {
            this.save(null, de);
          }
        }
      });
      t.start();
      threads.add(t);
    }
  }

  @Override
  public void resetIndex(Class clazz) {

  }

  @Override
  public void updateIndex(DataEntry de, Class clazz) {

  }

  public void stopImportThreads() {
    while (threads.size() > 0) {
      threads.remove(0).interrupt();
    }
    threads.clear();
  }

  public void addListener(Modification m, Consumer<DataEntry> method) {
    if (!listeners.containsKey(m)) {
      listeners.put(m, new HashSet<>());
    }
    listeners.get(m).add(method);
  }

  @Override
  public boolean isUnsavedIndexModifications() {
    return false;
  }

  @Override
  public void resetIndex() {

  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }
  public boolean isBuildIndex() {
    return buildIndex;
  }

  public void setBuildIndex(boolean buildIndex) {
    this.buildIndex = buildIndex;
  }

  @Override
  public void setUnsavedIndexModifications(boolean unsavedIndexModifications) {

  }
}
