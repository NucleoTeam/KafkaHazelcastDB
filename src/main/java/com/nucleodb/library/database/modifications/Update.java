package com.nucleodb.library.database.modifications;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.fge.jsonpatch.JsonPatch;
import com.nucleodb.library.database.tables.table.DataEntry;
import com.nucleodb.library.database.utils.JsonOperations;
import com.nucleodb.library.database.utils.Serializer;

import java.time.Instant;
import java.util.List;

public class Update extends Modify{
    public String key;
    public String changeUUID;
    public String changes;
    public long version;
    public Instant time;

    public Update() {

    }

    public Update(String changeUUID, DataEntry entry, String changes) {
        this.changeUUID = changeUUID;
        this.key = entry.getKey();
        this.changes = changes;
        this.version = entry.getVersion();
        this.time = Instant.now();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
    @JsonIgnore
    public JsonPatch getChangesPatch() {
        try {
            return Serializer.getObjectMapper().getOmNonType().readValue(changes, JsonPatch.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonIgnore
    public List<JsonOperations> getOperations() {
        try {
            return Serializer.getObjectMapper().getOmNonType().readValue(changes, new TypeReference<List<JsonOperations>>(){});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String getChanges() {
        return changes;
    }

    public void setChange(String changes) {
        this.changes = changes;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Instant getTime() {
        return time;
    }

    public void setTime(Instant time) {
        this.time = time;
    }

    public String getChangeUUID() {
        return changeUUID;
    }

    public void setChangeUUID(String changeUUID) {
        this.changeUUID = changeUUID;
    }

    public void setChanges(String changes) {
        this.changes = changes;
    }
}
