package com.nucleocore.mysql;

public class MySQLConstants {
  // Server capabilities flags
  public static final int CLIENT_LONG_PASSWORD = 1;
  public static final int CLIENT_FOUND_ROWS = 2;
  public static final int CLIENT_LONG_FLAG = 4;
  public static final int CLIENT_CONNECT_WITH_DB = 8;
  public static final int CLIENT_NO_SCHEMA = 16;
  public static final int CLIENT_COMPRESS = 32;
  public static final int CLIENT_ODBC = 64;
  public static final int CLIENT_LOCAL_FILES = 128;
  public static final int CLIENT_IGNORE_SPACE = 256;
  public static final int CLIENT_PROTOCOL_41 = 512;
  public static final int CLIENT_INTERACTIVE = 1024;
  public static final int CLIENT_SSL = 2048;
  public static final int CLIENT_IGNORE_SIGPIPE = 4096;
  public static final int CLIENT_TRANSACTIONS = 8192;
  public static final int CLIENT_RESERVED = 16384;
  public static final int CLIENT_SECURE_CONNECTION = 32768;
  public static final int CLIENT_MULTI_STATEMENTS = 65536;
  public static final int CLIENT_MULTI_RESULTS = 131072;
  public static final int CLIENT_PS_MULTI_RESULTS = 262144;
  public static final int CLIENT_PLUGIN_AUTH = 524288;
  public static final int CLIENT_CONNECT_ATTRS = 1048576;
  public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 2097152;
  public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 4194304;
  public static final int CLIENT_SESSION_TRACK = 8388608;
  public static final int CLIENT_DEPRECATE_EOF = 16777216;

  // Server status flags
  public static final int SERVER_STATUS_IN_TRANS = 1;
  public static final int SERVER_STATUS_AUTOCOMMIT = 2;
  public static final int SERVER_MORE_RESULTS_EXISTS = 8;
  public static final int SERVER_STATUS_NO_GOOD_INDEX_USED = 16;
  public static final int SERVER_STATUS_NO_INDEX_USED = 32;
  public static final int SERVER_STATUS_CURSOR_EXISTS = 64;
  public static final int SERVER_STATUS_LAST_ROW_SENT = 128;
  public static final int SERVER_STATUS_DB_DROPPED = 256;
  public static final int SERVER_STATUS_NO_BACKSLASH_ESCAPES = 512;
  public static final int SERVER_STATUS_METADATA_CHANGED = 1024;
  public static final int SERVER_QUERY_WAS_SLOW = 2048;
  public static final int SERVER_PS_OUT_PARAMS = 4096;
  public static final int SERVER_STATUS_IN_TRANS_READONLY = 8192;
  public static final int SERVER_SESSION_STATE_CHANGED = 16384;
}