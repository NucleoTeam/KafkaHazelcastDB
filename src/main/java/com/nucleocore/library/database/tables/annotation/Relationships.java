package com.nucleocore.library.database.tables.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Relationships {
    Relationship[] value();
}