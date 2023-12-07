package com.nucleodb.library.utils.field;

public class EqualityCheck{
  public static boolean areEqual(Object obj1, Object obj2) {
    if (obj1 == null || obj2 == null) {
      return obj1 == obj2;
    }

    if (obj1 instanceof Byte && obj2 instanceof Byte) {
      return (Byte) obj1 == (Byte) obj2;
    } else if (obj1 instanceof Short && obj2 instanceof Short) {
      return (Short) obj1 == (Short) obj2;
    } else if (obj1 instanceof Integer && obj2 instanceof Integer) {
      return (Integer) obj1 == (Integer) obj2;
    } else if (obj1 instanceof Long && obj2 instanceof Long) {
      return (Long) obj1 == (Long) obj2;
    } else if (obj1 instanceof Float && obj2 instanceof Float) {
      return (Float) obj1 == (Float) obj2;
    } else if (obj1 instanceof Double && obj2 instanceof Double) {
      return (Double) obj1 == (Double) obj2;
    } else if (obj1 instanceof Character && obj2 instanceof Character) {
      return (Character) obj1 == (Character) obj2;
    } else if (obj1 instanceof Boolean && obj2 instanceof Boolean) {
      return (Boolean) obj1 == (Boolean) obj2;
    } else {
      return obj1.equals(obj2);
    }
  }
}
