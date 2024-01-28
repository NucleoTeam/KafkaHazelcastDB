package com.nucleodb.library.database.lock;

public class LockQueueAction{
  enum Action {
    REMOVE_LOCK,
    ADD_LOCK,
    LOCK_CHECK
  }
  Action action;
  String request;

  public LockQueueAction(Action action, String request) {
    this.action = action;
    this.request = request;
  }

  public Action getAction() {
    return action;
  }

  public void setAction(Action action) {
    this.action = action;
  }

  public String getRequest() {
    return request;
  }

  public void setRequest(String request) {
    this.request = request;
  }
}
