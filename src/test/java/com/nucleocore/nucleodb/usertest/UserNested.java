package com.nucleocore.nucleodb.usertest;

import java.io.Serializable;

public class UserNested implements Serializable{
        public UserNested() {
            nestedValue = "woot";
        }
        private String nestedValue;

        public String getNestedValue() {
            return nestedValue;
        }

        public void setNestedValue(String nestedValue) {
            this.nestedValue = nestedValue;
        }
    }