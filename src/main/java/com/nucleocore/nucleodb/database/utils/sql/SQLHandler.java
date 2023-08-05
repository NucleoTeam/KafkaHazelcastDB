package com.nucleocore.nucleodb.database.utils.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleocore.nucleodb.NucleoDB;
import com.nucleocore.nucleodb.database.tables.DataTable;
import com.nucleocore.nucleodb.database.utils.DataEntry;
import com.nucleocore.nucleodb.database.utils.Serializer;
import com.nucleocore.nucleodb.database.utils.TreeSetExt;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SQLHandler{
  public static <T> Set<T> handleSelect(Select select, NucleoDB nucleoDB, Class<T> clazz) {
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    String tableName = plainSelect.getFromItem().toString();
    List<SelectItem> selectItems = plainSelect.getSelectItems();

    DataTable table = nucleoDB.getTable(tableName);
    if (table == null) {
      System.out.println("Table not found.");
      return new TreeSet<>();
    }

    // Handle WHERE clauses
    Expression where = plainSelect.getWhere();
    Set<DataEntry> foundEntries;
    if (where == null) {
      foundEntries = table.getDataEntries();
    } else {
      foundEntries = evaluateWhere(where, table);
    }
    List<T> items = new ArrayList<>();
    for (int i = 0; i < foundEntries.size(); i++) {
      try {
        T data = clazz.getConstructor().newInstance();
        items.add(data);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    Set<PropertyDescriptor> keyFields = Arrays.stream(clazz.getDeclaredFields())
        .filter(field->field.isAnnotationPresent(PrimaryKey.class))
        .map(field-> {
          try {
            return new PropertyDescriptor(field.getName(), clazz);
          } catch (IntrospectionException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toSet());
    return (Set<T>) foundEntries.stream().map(f -> {
      try {
        Object o = Serializer.getObjectMapper().getOm().readValue(
            Serializer.getObjectMapper().getOm().writeValueAsString(f.getData()),
            clazz
        );
        keyFields.forEach(propertyDescriptor ->{
          try {
            propertyDescriptor.getWriteMethod().invoke(o, f.getKey());
          }catch (Exception e){
            e.printStackTrace();
          }
        });
        return o;
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }).collect(Collectors.toSet());
  }

  public static Set<DataEntry> evaluateWhere(Expression expr, DataTable table) {
    if (expr instanceof AndExpression) {
      AndExpression andExpression = (AndExpression) expr;
      Set<DataEntry> leftEntries = evaluateWhere(andExpression.getLeftExpression(), table);
      leftEntries.retainAll(evaluateWhere(andExpression.getRightExpression(), table));
      System.out.println("and " + leftEntries.size());
      return leftEntries;
    } else if (expr instanceof OrExpression) {
      OrExpression orExpression = (OrExpression) expr;
      Set<DataEntry> leftEntries = evaluateWhere(orExpression.getLeftExpression(), table);
      leftEntries.addAll(evaluateWhere(orExpression.getRightExpression(), table));
      System.out.println("or " + leftEntries.size());
      return leftEntries;
    } else if (expr instanceof InExpression) {
      InExpression inExpression = (InExpression) expr;
      System.out.println("In");
      ExpressionList expressionList = (ExpressionList) inExpression.getRightItemsList();
      String left = ((Column) inExpression.getLeftExpression()).getFullyQualifiedName();
      List<Object> vals = expressionList.getExpressions().stream().map(f -> {
        if(f instanceof StringValue) {
          return ((StringValue) f).getValue();
        } else if(f instanceof DoubleValue) {
          return ((DoubleValue) f).getValue();
        } else if(f instanceof LongValue) {
          return ((LongValue) f).getValue();
        }
        return null;
      }).collect(Collectors.toList());
      Set<DataEntry> valz = table.in(left, vals);
      try {
        System.out.println(left + " = " + Serializer.getObjectMapper().getOm().writeValueAsString(vals) + " size: " + valz.size());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
      return valz;
    } else if (expr instanceof LikeExpression) {
      BinaryExpression binary = (BinaryExpression) expr;
      String left = ((Column) binary.getLeftExpression()).getFullyQualifiedName();
      String right = ((StringValue) binary.getRightExpression()).getValue();
      Set<DataEntry> vals = table.search(left, right);
      System.out.println(left + " like " + right + " size: " + vals.size());
      return vals;
    } else if (expr instanceof Parenthesis) {
      Parenthesis parenthesis = (Parenthesis) expr;
      return evaluateWhere(parenthesis.getExpression(), table);
    } else if (expr instanceof BinaryExpression) {
      BinaryExpression binary = (BinaryExpression) expr;
      String left = ((Column) binary.getLeftExpression()).getFullyQualifiedName();
      Set<DataEntry> vals = new TreeSetExt<>();
      if (expr instanceof EqualsTo) {
        Expression rightExpression = binary.getRightExpression();
        if(rightExpression instanceof StringValue) {
          String right = ((StringValue) rightExpression).getValue();
          vals = table.get(left, right);
          System.out.println(left + " = " + right + " size: " + vals.size());
        }else if(rightExpression instanceof DoubleValue) {
          Double right = ((DoubleValue) rightExpression ).getValue();
          vals = table.get(left, right);
          System.out.println(left + " = " + right + " size: " + vals.size());
        }else if(rightExpression instanceof LongValue) {
          Long right = ((LongValue) rightExpression ).getValue();
          vals = table.get(left, right);
          System.out.println(left + " = " + right + " size: " + vals.size());
        }
        return vals;
      } else if (expr instanceof NotEqualsTo) {
        String right = ((StringValue) binary.getRightExpression()).getValue();
        System.out.println(left + " != " + right);
        return table.getNotEqual(left, right);
      } else {
        System.out.println(binary.getClass().getName());
        System.out.println(binary.getLeftExpression().getClass().getName());
        System.out.println(binary.getRightExpression().getClass().getName());
      }
      // Other binary expressions can be added similarly
    } else {
      System.out.println(expr.getClass().getName());
    }
    // Other conditions (like OrExpression) can be added similarly
    return new TreeSet<>();
  }

  public static DataEntry handleInsert(Insert sqlStatement, NucleoDB nucleoDB) {
    String tableName = sqlStatement.getTable().getName();
    try {
      DataTable table = nucleoDB.getTable(tableName);
      Object obj = table.getClazz().getConstructor().newInstance();

      String[] columns = sqlStatement.getSetColumns().stream().map(c -> c.getColumnName()).collect(Collectors.toList()).toArray(new String[0]);

      Expression[] expressions = sqlStatement.getSetExpressionList().toArray(new Expression[0]);
      for (int i = 0; i < columns.length; i++) {
        Field field = obj.getClass().getDeclaredField(columns[i]);
        setColumnVal(expressions[i], columns[i], obj, field);
      }
      CountDownLatch countDownLatch = new CountDownLatch(1);
      AtomicReference<DataEntry> dataEntry = new AtomicReference<>();
      table.insert(obj, dataEntryNew -> {
        dataEntry.set(dataEntryNew);
        countDownLatch.countDown();
      });
      countDownLatch.await();
      return dataEntry.get();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void setColumnVal(Expression expression, String column, Object obj, Field field) throws InvocationTargetException, IllegalAccessException {
    try {
      if (expression instanceof StringValue) {
        if (column == null) {
          if (obj instanceof Collection) {
            ((Collection) obj).add(((StringValue) expression).getValue());
          } else {
            System.out.println("lost string");
          }
        } else {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(column, obj.getClass());
          propertyDescriptor.getWriteMethod().invoke(obj, ((StringValue) expression).getValue());
        }
      }else if (expression instanceof LongValue) {
        if(column==null){
          if(obj instanceof Collection){
            if(field != null) {
              ParameterizedType listType = (ParameterizedType) field.getGenericType();
              Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
              if(listValueType == Long.class){
                ((Collection) obj).add(((LongValue) expression).getValue());
              }else if(listValueType == Integer.class){
                ((Collection) obj).add(((LongValue) expression).getValue());
              }else{
                Serializer.log("In LongValue "+listValueType.getName());
              }
            }else {
              ((Collection) obj).add(((LongValue) expression).getValue());
            }
          }else{
            System.out.println("lost long");
          }
        }else {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(column, obj.getClass());
          if(propertyDescriptor.getPropertyType().isAssignableFrom(Integer.class)){
            propertyDescriptor.getWriteMethod().invoke(obj, Long.valueOf(((LongValue) expression).getValue()).intValue());
          }else if(propertyDescriptor.getPropertyType().isAssignableFrom(Long.class)){
            propertyDescriptor.getWriteMethod().invoke(obj, ((LongValue) expression).getValue());
          }
        }
      }else if (expression instanceof DoubleValue) {
        if(column==null){
          if(obj instanceof Collection){
            if(field != null) {
              ParameterizedType listType = (ParameterizedType) field.getGenericType();
              Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
              if(listValueType == Float.class){
                ((Collection) obj).add(Double.valueOf(((DoubleValue) expression).getValue()).floatValue());
              }else if(listValueType == Double.class){
                ((Collection) obj).add(((DoubleValue) expression).getValue());
              }else{
                Serializer.log("In DoubleValue "+listValueType.getName());
              }
            }else {
              ((Collection) obj).add(((DoubleValue) expression).getValue());
            }
          }else{
            System.out.println("lost long");
          }
        }else {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(column, obj.getClass());
          if(propertyDescriptor.getPropertyType().isAssignableFrom(Float.class)){
            propertyDescriptor.getWriteMethod().invoke(obj, Double.valueOf(((DoubleValue) expression).getValue()).floatValue());
          }else if(propertyDescriptor.getPropertyType().isAssignableFrom(Double.class)){
            propertyDescriptor.getWriteMethod().invoke(obj, ((DoubleValue) expression).getValue());
          }
        }
      }else if (expression instanceof RowConstructor) {
        PropertyDescriptor propertyDescriptor = null;
        if (column != null) {
          propertyDescriptor = new PropertyDescriptor(column, obj.getClass());
        }
        RowConstructor rowConstructor = (RowConstructor) expression;
        if (propertyDescriptor != null && propertyDescriptor.getPropertyType().isAssignableFrom(List.class)) {
          List listing = (List) propertyDescriptor.getReadMethod().invoke(obj);
          ParameterizedType listType = (ParameterizedType) obj.getClass().getDeclaredField(column).getGenericType();
          Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
          if (listing == null) {
            listing = Serializer.getObjectMapper().getOm().readValue(
                "[]",
                Serializer.getObjectMapper().getOm().getTypeFactory().constructCollectionType(
                    List.class,
                    listValueType
                )
            );
            propertyDescriptor.getWriteMethod().invoke(obj, listing);
          }
          if (rowConstructor.getExprList() != null) {
            if (rowConstructor.getExprList().getExpressions().size() > 0) {
              List finalListing = listing;
              rowConstructor.getExprList().getExpressions().forEach(expr -> {
                try {
                  Field newField = obj.getClass().getDeclaredField(column);
                  if (expr instanceof StringValue || expr instanceof DoubleValue || expr instanceof LongValue) {
                    setColumnVal(expr, null, finalListing, newField);
                  } else {
                    Object newObject = listValueType.getConstructor().newInstance();
                    finalListing.add(newObject);

                    setColumnVal(expr, null, newObject, newField);
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              });
            }
          }
        } else {
          ((RowConstructor) expression).getExprList().getExpressions().forEach(expr -> {
            try {
              setColumnVal(expr, null, obj, null);
            } catch (Exception e) {
              e.printStackTrace();
            }
          });
        }
      }else if(expression instanceof EqualsTo) {
        String columnName = ((Column) ((EqualsTo) expression).getLeftExpression()).getColumnName();
        PropertyDescriptor propertyDescriptor = new PropertyDescriptor(columnName, obj.getClass());
        if(propertyDescriptor.getPropertyType().isAssignableFrom(List.class)) {

          List listObject = (List) propertyDescriptor.getReadMethod().invoke(obj);
          if(listObject==null){
            ParameterizedType listType = (ParameterizedType) obj.getClass().getDeclaredField(columnName).getGenericType();
            Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
            listObject = Serializer.getObjectMapper().getOm().readValue(
                "[]",
                Serializer.getObjectMapper().getOm().getTypeFactory().constructCollectionType(
                    List.class,
                    listValueType
                )
            );
            propertyDescriptor.getWriteMethod().invoke(obj, listObject);
          }
          Field newField = obj.getClass().getDeclaredField(columnName);
          setColumnVal(((EqualsTo) expression).getRightExpression(), null, listObject, newField);
        }else{
          setColumnVal(((EqualsTo) expression).getRightExpression(), columnName, obj, field);
        }
      }else if(expression instanceof Parenthesis){
        setColumnVal(((Parenthesis) expression).getExpression(), column, obj, field);
        //Serializer.log(.getClass().getName());
      } else {
        System.out.println("last: "+expression.getClass().getName());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
