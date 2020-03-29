package com.nucleocore.db.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.nucleocore.db.database.index.Index;
import com.nucleocore.db.database.index.IndexTemplate;
import com.nucleocore.db.database.modifications.Create;
import com.nucleocore.db.database.modifications.Delete;
import com.nucleocore.db.database.modifications.Update;
import com.nucleocore.db.database.utils.*;
import com.nucleocore.db.kafka.ConsumerHandler;
import com.nucleocore.db.kafka.ProducerHandler;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class DataTable implements TableTemplate {

    private Queue<Object[]> indexQueue = Queues.newArrayDeque();

    private ProducerHandler producer = null;
    private ConsumerHandler consumer = null;

    private List<DataEntry> entries = Lists.newArrayList();
    private HashMap<String, IndexTemplate> indexes = new HashMap<>();

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
    private StartupRun startupCode;
    private boolean inStartup = true;


    public synchronized Object[] getIndex(){
        if(indexQueue.isEmpty())
            return null;
        return indexQueue.poll();
    }

    public DataTable(String bootstrap, String table, Class clazz, StartupRun startupCode, boolean startupConsume) {
        this.startupCode = startupCode;
        this.bootstrap = bootstrap;
        this.table = table;
        Properties props = new Properties();
        if(bootstrap!=null) {
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            AdminClient client = KafkaAdminClient.create(props);
            try {
                if (client.listTopics().names().get().stream().filter(x -> x.equals(table)).count() == 0) {
                    try {
                        final NewTopic newTopic = new NewTopic(table, 3, (short)3);
                        final CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
                        createTopicsResult.values().get(table).get();
                    } catch (InterruptedException | ExecutionException e) {
                        if (!(e.getCause() instanceof TopicExistsException)) {
                            throw new RuntimeException(e.getMessage(), e);
                        }
                        // TopicExistsException - Swallow this exception, just means the topic already exists.
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
            try{
                if (client.listTopics().names().get().stream().filter(x -> x.equals(table)).count() == 0) {
                    System.out.println("topic not created");
                    System.exit(-1);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
            if(startupConsume){
                this.consume();
            }
            client.close();
            producer = new ProducerHandler(bootstrap, table);
        }
        this.clazz = clazz;
        this.fields = new ArrayList<Field>() {{
            addAll(Arrays.asList(clazz.getDeclaredFields()));
            addAll(Arrays.asList(clazz.getSuperclass().getFields()));
        }};
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
            startupCode.run(this);
        }
    }

    private long counter=0;
    private long lastReq=0;
    public IndexTemplate getIndex(Field field){
        String fieldName = field.getName();
        if (!indexes.containsKey(fieldName)) {
            try {
                IndexTemplate index = field.getAnnotation(Index.class).value().getIndexType().getClass().getConstructor().newInstance();
                indexes.put(fieldName, index.indexOn(field));
                return index;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            return indexes.get(fieldName);
        }
        return null;
    }
    public void addIndex(DataEntry de) {
        lastReq = System.currentTimeMillis();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Index.class)) {
                IndexTemplate index = getIndex(field);
                index.add(de);
            }
        }
    }
    public void updateIndex(DataEntry de) {
        lastReq = System.currentTimeMillis();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Index.class)) {
                IndexTemplate index = getIndex(field);
                index.update(de);
                index.add(de);
            }
        }
    }
    public void deleteIndex(DataEntry de) {
        lastReq = System.currentTimeMillis();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Index.class)) {
                IndexTemplate index = getIndex(field);
                index.delete(de);
            }
        }
    }

    public void resetIndex() {
        lastReq = System.currentTimeMillis();
        for (Field field : fields) {
            if (field.isAnnotationPresent(Index.class)) {
                String fieldName = field.getName();
                if (!indexes.containsKey(fieldName)) {
                    IndexTemplate index = field.getAnnotation(Index.class).value().getIndexType();
                    indexes.put(fieldName, index.indexOn(field));
                }
                indexes.get(fieldName).addAll(getEntries());
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
            if (indexes.containsKey(name)) {
                IndexTemplate index = indexes.get(name);
                return (List<T>) index.search(obj);
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
                newEntry.versionIncrease();
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
            oldEntry.versionIncrease();
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
                    newEntry.versionIncrease();
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

    public HashMap<String, IndexTemplate> getIndexes() {
        return indexes;
    }

    public void setIndexes(HashMap<String, IndexTemplate> indexes) {
        this.indexes = indexes;
    }

    public boolean isUnsavedIndexModifications() {
        return unsavedIndexModifications;
    }

    public void setUnsavedIndexModifications(boolean unsavedIndexModifications) {
        this.unsavedIndexModifications = unsavedIndexModifications;
    }
}
