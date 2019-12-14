package com.nucleocore.db.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.nucleocore.db.database.modifications.Create;
import com.nucleocore.db.database.modifications.Delete;
import com.nucleocore.db.database.modifications.Update;
import com.nucleocore.db.database.utils.*;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Consumer;

public class LargeDataTable implements TableTemplate {

    private Queue<Object[]> indexQueue = Queues.newArrayDeque();

    private ProducerHandler producer = null;
    private ConsumerHandler consumer = null;

    private List<DataEntry> entries = Lists.newArrayList();
    private HashMap<String, BinaryIndex> sortedIndex = new HashMap<>();

    private HashMap<String, Consumer<DataEntry>> consumers = new HashMap<>();

    private HashMap<Modification, Set<Consumer<DataEntry>>> listeners = new HashMap<>();


    private boolean unsavedIndexModifications = false;
    private int size = 0;
    private boolean buildIndex = true;

    ObjectMapper om = new ObjectMapper(){{this.enableDefaultTyping();}};

    private String bootstrap;
    private String table;
    private List<Field> fields;
    private Class clazz;
    private Runnable startupCode;
    private boolean inStartup = true;


    public synchronized Object[] getIndex(){
        if(indexQueue.isEmpty())
            return null;
        return indexQueue.poll();
    }

    public LargeDataTable(String bootstrap, String table, Class clazz, Runnable startupCode) {
        this.startupCode = startupCode;
        this.bootstrap = bootstrap;
        this.table = table;
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        AdminClient client = KafkaAdminClient.create(props);
        try {
            if (client.listTopics().names().get().stream().filter(x -> x.equals(table)).count() == 0) {
                client.createTopics(new ArrayList<NewTopic>() {{
                    add(new NewTopic(table, 1, (short) 4));
                }});
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        client.close();
        if (bootstrap != null) {
            producer = new ProducerHandler(bootstrap, table);
            this.clazz = clazz;
            this.fields = new ArrayList<Field>() {{
                addAll(Arrays.asList(clazz.getDeclaredFields()));
                addAll(Arrays.asList(clazz.getSuperclass().getFields()));
            }};
        }
    }

    public void consume() {
        if (bootstrap != null) {
            consumer = new ConsumerHandler(bootstrap, UUID.randomUUID().toString(), this, table);
        }
    }

    public void flush() {
        try {
            synchronized (entries) {
                entries.clear();
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        consumers = new HashMap<>();
        listeners = new HashMap<>();
        System.gc();
    }

    public void display(int start, int end, int pos, int direction, int size) {
        System.out.print("");
        for (int x = 0; x < size; x++) {
            if (pos == x) {
                System.out.print(((direction > 0) ? "➡" : ((direction < 0) ? "⬅" : "❌")));
            } else if (start == x) {
                System.out.print("⏩");
            } else if (end == x) {
                System.out.print("⏪");
            } else {
                System.out.print("⏺");
            }
        }
        if (end == size) {
            System.out.println(" size:" + size);
        } else {
            System.out.println(" size:" + size);
        }
    }

    public void displayInsert(int compared, int pos, int size) {
        System.out.print("");
        for (int x = 0; x < size; x++) {
            if (pos == x) {
                System.out.print("❌");
            } else if (compared == x) {
                System.out.print("⏹");
            } else {
                System.out.print("⏺");
            }
        }
        System.out.println(" size:" + size);
    }

    public <T> List<T> in(String name, List<DataEntry> objs, Class clazz) {
        List<DataEntry> tmp = Lists.newArrayList();
        try {
            for (DataEntry obj : objs) {
                List<DataEntry> de = search(name, obj);
                if (de != null) {
                    tmp.addAll(de);
                }
            }
        } catch (ClassCastException ex) {
            ex.printStackTrace();
        }
        return (List<T>) tmp;
    }

    @Override
    public void startup() {
        inStartup = false;
        resetIndex();
        if(startupCode!=null) {
            startupCode.run();
        }
    }

    private long counter=0;
    private long lastReq=0;
    public void addIndex(DataEntry de) {
        lastReq = System.currentTimeMillis();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Index.class)) {
                String fieldName = f.getName();
                if (!sortedIndex.containsKey(fieldName)) {
                    sortedIndex.put(fieldName, new BinaryIndex(f));
                }
                if(!inStartup) {
                    BinaryIndex index = sortedIndex.get(fieldName);
                    index.add(de);
                }
            }
        }
    }
    public void updateIndex(DataEntry de) {
        lastReq = System.currentTimeMillis();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Index.class)) {
                String fieldName = f.getName();
                if (!sortedIndex.containsKey(fieldName)) {
                    sortedIndex.put(fieldName, new BinaryIndex(f));
                }
                BinaryIndex index = sortedIndex.get(fieldName);
                if(!inStartup) {
                    index.delete(de);
                    index.add(de);
                }
            }
        }
    }
    public void deleteIndex(DataEntry de) {
        lastReq = System.currentTimeMillis();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Index.class)) {
                String fieldName = f.getName();
                if (!sortedIndex.containsKey(fieldName)) {
                    sortedIndex.put(fieldName, new BinaryIndex(f));
                }
                if(!inStartup) {
                    sortedIndex.get(fieldName).delete(de);
                }
            }
        }
    }

    public void resetIndex() {
        lastReq = System.currentTimeMillis();
        for (Field f : fields) {
            if (f.isAnnotationPresent(Index.class)) {
                String fieldName = f.getName();
                if (!sortedIndex.containsKey(fieldName)) {
                    sortedIndex.put(fieldName, new BinaryIndex(f));
                }
                sortedIndex.get(fieldName).getEntries().addAll(getEntries());
                sortedIndex.get(fieldName).sort();
            }
        }
    }

    public <T> List<T> search(String name, Object searchObject) {
        try {
            Object object = clazz.getConstructor().newInstance();
            clazz.getField(name).set(object, searchObject);
            return search(name, (DataEntry) object);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public <T> List<T> search(String name, DataEntry obj) {
        try {
            if (sortedIndex.containsKey(name)) {
                BinaryIndex reduce = sortedIndex.get(name);
                return (List<T>) reduce.find(obj);
            } else {
                System.out.println("NO INDEX FOUND");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ERRR 4");
            System.exit(-1);
        }
        return null;
    }

    public DataEntry searchOne(String name, Object obj) {
        List<DataEntry> entries = search(name, obj);
        if(entries != null && entries.size()>0){
            return entries.get(0);
        }
        return null;
    }

    public int size() {
        return size;
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
                updateEntry.setKey(oldEntry.getKey());
                if (consumer != null) {
                    consumers.put(newEntry.getKey(), consumer);
                }
                boolean changed = false;
                updateEntry.setMasterClass(newEntry.getClass().getName());
                for (Field f : newEntry.getClass().getDeclaredFields()) {
                    if (!Utils.cast(f.get(newEntry), f.get(oldEntry))) {
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

    public void modify(Modification mod, Object modification) {
        switch (mod) {
            case CREATE:
                Create c = (Create) modification;
                //System.out.println("Create statement called");
                if (c != null) {
                    try {
                        synchronized (entries) {
                            entries.add(c.getValue());
                            addIndex(c.getValue());
                        }
                        size++;
                        if (consumers.containsKey(c.getValue().getKey())) {
                            consumers.remove(c.getValue().getKey()).accept(c.getValue());
                        }
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
                    synchronized (entries) {
                        DataEntry de = searchOne("key", d.getKey());
                        if (de != null) {
                            entries.remove(de);
                            size--;
                            deleteIndex(de);
                        }
                    }
                }
                break;
            case UPDATE:
                Update u = (Update) modification;
                //System.out.println("Update statement called");

                if (u != null) {
                    try {
                        Class clazz = Class.forName(u.getMasterClass());
                        DataEntry de = searchOne("key", u.getKey());

                        if (de != null) {
                            if (consumers.containsKey(de.getKey())) {
                                consumers.remove(de.getKey()).accept(de);
                            }
                            u.getChange().forEach((String key, Object val) -> {
                                try {
                                    clazz.getDeclaredField(key).set(de, val);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            });
                            updateIndex(de);
                            fireListeners(Modification.UPDATE, de);
                        }
                    } catch (Exception e) {

                    }
                }
                break;
        }
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
        this.save(null, newEntry);
    }


    List<Thread> threads = new ArrayList<>();

    public void startImportThreads() {

    }

    public void addListener(Modification m, Consumer<DataEntry> method) {
        if (!listeners.containsKey(m)) {
            listeners.put(m, new HashSet<>());
        }
        listeners.get(m).add(method);
    }

    public List<DataEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<DataEntry> entries) {
        this.entries = entries;
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

    public boolean isUnsavedIndexModifications() {
        return unsavedIndexModifications;
    }

    public void setUnsavedIndexModifications(boolean unsavedIndexModifications) {
        this.unsavedIndexModifications = unsavedIndexModifications;
    }
}
