package com.nucleocore.library.utils.field;

import com.nucleocore.library.database.tables.annotation.Index;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldFinder{
  public static class FieldAndPath<T extends Annotation> {
    Field field;
    String path;
    T annotation;

    public FieldAndPath(Field field, String path, T annotation) {
      this.field = field;
      this.path = path;
      this.annotation = annotation;
    }

    public Field getField() {
      return field;
    }

    public void setField(Field field) {
      this.field = field;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public T getAnnotation() {
      return annotation;
    }

    public void setAnnotation(T annotation) {
      this.annotation = annotation;
    }
  }
  public static <T extends Annotation> List<FieldAndPath<T>> getAllAnnotatedFields(Class<?> clazz, Class<T> annotationClass, String prefix) {
    List<FieldAndPath<T>> fieldsWithAnnotation = new ArrayList<>();
    // Process all fields in the class
    for (Field field : clazz.getDeclaredFields()) {
      // Check for the annotation directly on the field
      String fieldName = field.getName();
      if (field.isAnnotationPresent(annotationClass)) {
        T annotation = field.getAnnotation(annotationClass);
        fieldsWithAnnotation.add(new FieldAndPath(field, prefix+fieldName, annotation));
      }

      // Check if the field is a class, an iterable, or a map
      Class<?> fieldType = field.getType();
      if (Iterable.class.isAssignableFrom(fieldType) || Map.class.isAssignableFrom(fieldType)) {
        // Extract generic type and process it
        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType) {
          Type[] actualTypeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
          for (Type typeArg : actualTypeArguments) {
            if (typeArg instanceof Class) {
              fieldsWithAnnotation.addAll(getAllAnnotatedFields((Class<?>) typeArg, annotationClass, prefix+fieldName+"."));
            }
          }
        }
      } else if (fieldType.isMemberClass()) {
        fieldsWithAnnotation.addAll(getAllAnnotatedFields(fieldType, annotationClass, prefix+fieldName+"."));
      }
    }
    // Process nested classes
    for (Class<?> nestedClass : clazz.getDeclaredClasses()) {
      fieldsWithAnnotation.addAll(getAllAnnotatedFields(nestedClass, annotationClass, ""));
    }
    return fieldsWithAnnotation;
  }
}
