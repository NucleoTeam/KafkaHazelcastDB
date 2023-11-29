package com.nucleodb.library.database.utils;

public class InvalidConnectionException extends Exception{
  public InvalidConnectionException(String message) {
    super("Invalid Connection Exception, null in connection leaves: "+message);
  }
}
