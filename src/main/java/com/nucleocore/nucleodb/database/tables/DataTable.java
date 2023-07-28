package com.nucleocore.nucleodb.database.tables;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.nucleocore.nucleodb.database.modifications.Create;
import com.nucleocore.nucleodb.database.modifications.Delete;
import com.nucleocore.nucleodb.database.modifications.Update;
import com.nucleocore.nucleodb.database.utils.*;
import com.nucleocore.nucleodb.database.utils.index.Index;
import com.nucleocore.nucleodb.database.utils.index.TreeIndex;
import com.nucleocore.nucleodb.kafkaLedger.ConsumerHandler;
import com.nucleocore.nucleodb.kafkaLedger.ProducerHandler;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TopicExistsException;
import com.github.fge.jsonpatch.JsonPatch;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DataTable {

    private Queue<Object[]> indexQueue = Queues.newArrayDeque();

    private ProducerHandler producer = null;
    private ConsumerHandler consumer = null;

    private List<DataEntry> entries = Lists.newArrayList();

    private Map<String, Index> indexes = new TreeMap<>();
    private Set<DataEntry> dataEntries = new TreeSet<>();
    private Map<String, DataEntry> keyToEntry = new TreeMap<>();

    private Map<String, Consumer<DataEntry>> consumers = new TreeMap<>();

    private Map<Modification, Set<Consumer<DataEntry>>> listeners = new TreeMap<>();


    private boolean unsavedIndexModifications = false;
    private int size = 0;
    private boolean buildIndex = true;

    private String bootstrap;
    private String table;
    private List<Field> fields;
    private Class clazz;
    private StartupRun startupCode;
    private boolean inStartup = true;


    public synchronized Object[] getIndex() {
        if (indexQueue.isEmpty())
            return null;
        return indexQueue.poll();
    }

    public DataTable(String bootstrap, String table, Class clazz, StartupRun startupCode, String... index) {
        this.startupCode = startupCode;
        if(bootstrap == null)
            bootstrap = "127.0.0.1:29092";
        System.out.println("Connecting to "+bootstrap);
        this.bootstrap = bootstrap;
        this.table = table;
        Properties props = new Properties();
        if (bootstrap != null) {
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            AdminClient client = KafkaAdminClient.create(props);

            try {
                if (client.listTopics().names().get().stream().filter(x -> x.equals(table)).count() == 0) {
                    try {
                        final NewTopic newTopic = new NewTopic(table, 3, (short)1);
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
            try {
                if (client.listTopics().names().get().stream().filter(x -> x.equals(table)).count() == 0) {
                    System.out.println("topic not created");
                    System.exit(-1);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
            Arrays.stream(index).map(i->new TreeIndex(i)).collect(Collectors.toSet()).forEach(i->{
                this.indexes.put(i.getIndexedKey(), i);
            });
            this.consume();
            client.close();
            producer = new ProducerHandler(bootstrap, table);

        }
        this.clazz = clazz;
        this.fields = new ArrayList<Field>() {{
            addAll(Arrays.asList(clazz.getDeclaredFields()));
            addAll(Arrays.asList(clazz.getSuperclass().getFields()));
        }};
        new Thread(new ModQueueHandler()).start();
    }

    public void consume() {
        if (bootstrap != null) {
            String consumer = UUID.randomUUID().toString();
            for(String kafkaBroker : bootstrap.split(",")) {
                new ConsumerHandler(kafkaBroker, consumer, this, table);
            }
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

    public List<DataEntry> in(String key, List<String> values, Class clazz) {
        List<DataEntry> tmp = Lists.newArrayList();
        try {
            for (String val : values) {
                List<DataEntry> de = search(key, val);
                if (de != null) {
                    tmp.addAll(de);
                }
            }
        } catch (ClassCastException ex) {
            ex.printStackTrace();
        }
        return  tmp;
    }

    public void startup() {
        inStartup = false;
        if (startupCode != null) {
            startupCode.run(this);
        }
    }

    private long counter = 0;
    private long lastReq = 0;

    public List<DataEntry> search(String key, String searchObject) {
        try {
            return this.indexes.get(key).search(searchObject).stream().toList();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public DataEntry searchOne(String key, String obj) {
        List<DataEntry> entries = search(key, obj);
        if (entries != null && entries.size() > 0) {
            return entries.get(0);
        }
        return null;
    }

    public int size() {
        return size;
    }

    public boolean insert(Object obj){
        return saveInternalConsumer(new DataEntry(obj), null);
    }
    public boolean insert(Object obj, Consumer<DataEntry> consumer){
        return saveInternalConsumer(new DataEntry(obj), consumer);
    }
    public boolean save(DataEntry entry){
        return saveInternalConsumer(entry, null);
    }
    public boolean save(DataEntry entry, Consumer<DataEntry> consumer){
        return saveInternalConsumer(entry, consumer);
    }

    private boolean saveInternalConsumer(DataEntry entry, Consumer<DataEntry> consumer){
        String changeUUID = UUID.randomUUID().toString();
        if(saveInternal(entry, changeUUID)) {
            if(consumer!=null) {
                consumers.put(changeUUID, consumer);
            }
            return true;
        }
        return false;
    }

    private synchronized boolean saveInternal(DataEntry entry, String changeUUID) {
        if(!dataEntries.contains(entry)){
            try {
                entry.versionIncrease();
                Create createEntry = new Create(changeUUID, entry);
                producer.push(createEntry);
                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else{
            entry.versionIncrease();
            List<JsonOperations> changes = null;
            JsonPatch patch = JsonDiff.asJsonPatch(entry.getReference(), new ObjectMapper().valueToTree(entry.getData()));
            try {
                String json = new ObjectMapper().writeValueAsString(patch);
                changes = new ObjectMapper().readValue(json, List.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            if(changes!=null && changes.size()>0) {
                Update updateEntry = new Update(changeUUID, entry, patch);
                producer.push(updateEntry);
                return true;
            }

        }
        return false;
    }

    class ModificationQueueItem {
        private Modification mod;
        private Object modification;

        public ModificationQueueItem(Modification mod, Object modification) {
            this.mod = mod;
            this.modification = modification;
        }

        public Modification getMod() {
            return mod;
        }

        public Object getModification() {
            return modification;
        }
    }

    Stack<ModificationQueueItem> modqueue = new Stack<>();

    class ModQueueHandler implements Runnable{
        @Override
        public void run() {
            ModificationQueueItem mqi;
            while(true) {
                while (!modqueue.isEmpty()) {
                    System.out.println("SOMETHING FOUND");
                    mqi = modqueue.pop();
                    if(mqi != null) {
                        modify(mqi.getMod(), mqi.getModification());
                    }
                    try {
                        Thread.sleep(10);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(10);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    public void modify(Modification mod, Object modification) {
        switch (mod) {
            case CREATE:
                Create c = (Create) modification;
                //System.out.println("Create statement called");
                if (c != null) {
                    try {
                        DataEntry dataEntry = new DataEntry(c);
                        synchronized (entries) {
                            entries.add(dataEntry);
                        }
                        synchronized (dataEntries) {
                            dataEntries.add(dataEntry);
                        }
                        synchronized (keyToEntry) {
                            keyToEntry.put(dataEntry.getKey(), dataEntry);
                        }
                        for(Index i:this.indexes.values()){
                            try {
                                i.add(dataEntry);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        };
                        size++;
                        if (consumers.containsKey(c.getChangeUUID())) {
                            consumers.remove(c.getChangeUUID()).accept(dataEntry);
                        }
                        fireListeners(Modification.CREATE, dataEntry);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            case DELETE:
                Delete d = (Delete) modification;
                //System.out.println("Delete statement called");
                if (d != null) {
                    DataEntry de = keyToEntry.get(d.getKey());
                    if (de != null) {
                        if (de.getVersion() + 1 != d.getVersion()) {
                            modqueue.add(new ModificationQueueItem(mod, modification));
                        } else {
                            synchronized (entries) {
                                entries.remove(de);
                                keyToEntry.remove(de.getKey());
                                dataEntries.remove(de);
                            }
                            this.indexes.values().forEach(i->i.delete(de));
                            size--;
                            fireListeners(Modification.DELETE, de);
                        }
                    } else {
                        modqueue.add(new ModificationQueueItem(mod, modification));
                    }
                }
                break;
            case UPDATE:
                Update u = (Update) modification;

                //System.out.println("Update statement called");
                if (u != null) {
                    try {
                        DataEntry de = keyToEntry.get(u.getKey());
                        if (de != null) {
                            if (de.getVersion() + 1 != u.getVersion()) {
                                modqueue.add(new ModificationQueueItem(mod, modification));
                            } else {
                                if (consumers.containsKey(de.getKey())) {
                                    consumers.remove(de.getKey()).accept(de);
                                }
                                de.setReference(u.getChangesPatch().apply(de.getReference()));
                                de.setVersion(u.getVersion());
                                de.setData(new ObjectMapper().readValue(de.getReference().toString(), de.getData().getClass()));
                                System.out.println(new ObjectMapper().writeValueAsString(de.getData()));
                                u.getOperations().forEach(op->{

                                    switch(op.getOp()){
                                        case "replace":
                                        case "add":
                                        case "copy":
                                            try {
                                                Index index = this.indexes.get(op.getPath());
                                                if(index!=null){
                                                    index.modify(de);
                                                }
                                            } catch (JsonProcessingException e) {
                                                throw new RuntimeException(e);
                                            }
                                            break;
                                        case "move":
                                            break;
                                        case "remove":
                                            Index index = this.indexes.get(op.getPath());
                                            if(index!=null) index.delete(de);
                                            break;
                                    }
                                });
                                fireListeners(Modification.UPDATE, de);
                            }
                        } else {
                            modqueue.add(new ModificationQueueItem(mod, modification));
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

    public void multiImport(Object newEntry) {
        this.insert(newEntry);
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

    public boolean isUnsavedIndexModifications() {
        return unsavedIndexModifications;
    }

    public void setUnsavedIndexModifications(boolean unsavedIndexModifications) {
        this.unsavedIndexModifications = unsavedIndexModifications;
    }
}
