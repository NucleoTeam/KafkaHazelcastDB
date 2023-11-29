package com.nucleodb.library.database.utils;

import java.io.Serializable;

public class JsonOperations implements Serializable{
        String op;
        String path;
        String value;
        String from;

        public String getOp() {
            return op;
        }

        public void setOp(String op) {
            this.op = op;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }