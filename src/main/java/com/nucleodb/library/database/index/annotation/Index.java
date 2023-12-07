package com.nucleodb.library.database.index.annotation;

import com.nucleodb.library.database.index.IndexWrapper;
import com.nucleodb.library.database.index.TreeIndex;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Index {
  String value() default "";
  Class<? extends IndexWrapper> type() default TreeIndex.class;
}