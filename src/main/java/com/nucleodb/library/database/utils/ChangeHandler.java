package com.nucleodb.library.database.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.diff.JsonDiff;

public class ChangeHandler {
  static ObjectMapper om = new ObjectMapper();
  JsonPatch patch;
  Class clazz;
  public ChangeHandler(Class clazz) {
     this.clazz = clazz;
  }

  public String getPatchJSON() throws JsonProcessingException {
    return om.writeValueAsString(this.patch);
  }

  public ChangeHandler getPatchJSON(String operations) throws JsonProcessingException {
    this.patch = om.readValue(operations, JsonPatch.class);
    return this;
  }

  public ChangeHandler getOperations(Object currObj, Object newObj) throws JsonProcessingException {
    String deploymentNew = om.writeValueAsString(newObj);
    String deploymentOld = om.writeValueAsString(currObj);
    this.patch = JsonDiff.asJsonPatch(om.readTree(deploymentOld), om.readTree(deploymentNew));
    return this;
  }
  public <V> V applyChanges(V obj) throws JsonProcessingException, JsonPatchException {
    return (V) om.readValue(om.writeValueAsString(patch.apply(om.readTree(om.writeValueAsString(obj)))), clazz);
  }
}
