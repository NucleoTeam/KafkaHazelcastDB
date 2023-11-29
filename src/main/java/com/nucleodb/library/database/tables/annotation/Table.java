package com.nucleodb.library.database.tables.annotation;

import com.nucleodb.library.database.tables.table.DataEntry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Table {
  String tableName();
  Class dataEntryClass() default DataEntry.class;
}
