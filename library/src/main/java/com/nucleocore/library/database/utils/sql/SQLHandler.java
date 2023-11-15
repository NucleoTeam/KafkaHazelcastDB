package com.nucleocore.library.database.utils.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Queues;
import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.table.DataTable;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.utils.Serializer;
import com.nucleocore.library.database.utils.TreeSetExt;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SQLHandler{

  public static class NullComparator implements Comparator<Object>{
    Function function;
    public NullComparator(Function function) {
      this.function = function;
    }

    @Override
    public int compare(Object o1, Object o2) {
      o1 = this.function.apply(o1);
      o2 = this.function.apply(o2);
      if(o1==null && o2==null){
        return 0;
      }
      if(o1==null && o2!=null){
        return 1;
      }
      if(o1!=null && o2==null){
        return -1;
      }
      if(o1 instanceof String && o2 instanceof String) {
        return ((String) o1).compareTo((String)o2);
      }
      if(o1 instanceof Integer && o2 instanceof Integer) {
        return ((Integer) o1).compareTo((Integer)o2);
      }
      if(o1 instanceof Long && o2 instanceof Long) {
        return ((Long) o1).compareTo((Long)o2);
      }
      if(o1 instanceof Double && o2 instanceof Double) {
        return ((Double) o1).compareTo((Double)o2);
      }
      if(o1 instanceof Float && o2 instanceof Float) {
        return ((Float) o1).compareTo((Float)o2);
      }
      if(o1 instanceof Object && o2 instanceof Object) {
        try {
          return Serializer.getObjectMapper().getOm().writeValueAsString(o1).compareTo(Serializer.getObjectMapper().getOm().writeValueAsString(o2));
        } catch (JsonProcessingException e) {
        }
      }
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }
  }
  static Map<Object, Map<String, Object>> cache = new HashMap<>();

  public static Function createComparatorFunction(String column){
    return s -> {
      // handle cache
      Map<String, Object> objCache = cache.get(s);
      if(objCache!=null && objCache.containsKey(column)){
        //Serializer.log("Use cache for "+column +" for "+s.toString());
        return objCache.get(column);
      }
      //Serializer.log("Grab data from  "+column +" for "+s.toString());
      Queue<String> queue = Queues.newLinkedBlockingDeque(Arrays.asList(column.split("\\.")));
      //Serializer.log(queue);
      Object o = s;
      PropertyDescriptor columnField;
      while (!queue.isEmpty()) {
        String next = queue.poll();
        //Serializer.log("going to: "+next);
        if (next.matches("i([0-9]+)")) {
          if (o instanceof List) {
            Pattern pattern = Pattern.compile("i([0-9]+)");
            Matcher matcher = pattern.matcher(next);
            if (matcher.find()) {
              int index = Integer.valueOf(matcher.group(1));
              o = ((List<?>) o).get(index);
            } else {
              o=null;
              break;
            }
          }
        } else {
          try {
            columnField = new PropertyDescriptor(next, o.getClass());
            o = columnField.getReadMethod().invoke(o);
          }catch (Exception e){
            o=null;
            break;
          }
        }
      }
      if(o==s){
        return null;
      }
      objCache = cache.get(s);
      if(objCache==null){
        objCache = new HashMap<>();
        cache.put(s, objCache);
      }
      objCache.put(column, o);
      return o;
    };
  }
  public static Comparator createSorting(List<OrderByElement> orderByElements){
    Comparator comparator = null;
    for (OrderByElement orderByElement : orderByElements) {
      Expression expr = orderByElement.getExpression();
      //Serializer.log(expr.getClass().getName());
      if (expr instanceof Column) {
        Column columnExpression = ((Column) expr);
        String column = "data."+columnExpression.getFullyQualifiedName();
        //Serializer.log("adding sort on "+column);

        Function function = createComparatorFunction(column);

        if (comparator == null) {
          if (!orderByElement.isAsc()) {
            //Serializer.log("reverse");
            comparator = new NullComparator(function).reversed();
          }else{
            comparator = new NullComparator(function);
          }
        } else {

          if (!orderByElement.isAsc()) {
            //Serializer.log("reverse");
            comparator = comparator.thenComparing(new NullComparator(function)).reversed();
          }else{
            comparator = comparator.thenComparing(new NullComparator(function));
          }
        }
      }
    }
    return comparator;
  }
  public static <T> List<T> handleSelect(Select select, NucleoDB nucleoDB, Class<T> clazz) {
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    String tableName = plainSelect.getFromItem().toString();
    List<SelectItem> selectItems = plainSelect.getSelectItems();

    DataTable table = nucleoDB.getTable(tableName);
    if (table == null) {
      System.out.println("Table not found.");
      return new LinkedList<>();
    }

    // Handle WHERE clauses
    Expression where = plainSelect.getWhere();
    Set<DataEntry> foundEntries;
    if (where == null) {
      foundEntries = table.getDataEntries();
    } else {
      foundEntries = evaluateWhere(where, table);
    }
    List<DataEntry> sortedEntries = foundEntries.stream().toList();
    if(plainSelect.getOrderByElements()!=null) {
      sortedEntries = (List<DataEntry>) sortedEntries.stream().sorted(createSorting(plainSelect.getOrderByElements())).collect(Collectors.toList());
    }
    int offset = 0;
    int count = 25;
    if(plainSelect.getLimit()!=null){
      if(plainSelect.getLimit().getOffset()!=null) {
        if(plainSelect.getLimit().getOffset() instanceof LongValue){
          offset = Long.valueOf(((LongValue) plainSelect.getLimit().getOffset()).getValue()).intValue();
        }
      }
      if(plainSelect.getLimit().getRowCount() instanceof LongValue){
        count = Long.valueOf(((LongValue) plainSelect.getLimit().getRowCount()).getValue()).intValue();
      }
    }
    sortedEntries = sortedEntries.subList((offset>sortedEntries.size())?sortedEntries.size():offset, (offset+count>sortedEntries.size())?sortedEntries.size():offset+count);
    Set<PropertyDescriptor> keyFields = Arrays.stream(clazz.getDeclaredFields())
        .filter(field -> field.isAnnotationPresent(PrimaryKey.class))
        .map(field -> {
          try {
            return new PropertyDescriptor(field.getName(), clazz);
          } catch (IntrospectionException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toSet());

    return (List<T>)  sortedEntries.stream().map(f -> {
      try {
        Object o = Serializer.getObjectMapper().getOm().readValue(
            Serializer.getObjectMapper().getOm().writeValueAsString(f.getData()),
            clazz
        );
        keyFields.forEach(propertyDescriptor -> {
          try {
            propertyDescriptor.getWriteMethod().invoke(o, f.getKey());
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
        return o;
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }).collect(Collectors.toList());
  }

  public static Set<DataEntry> evaluateWhere(Expression expr, DataTable table) {
    if (expr instanceof AndExpression) {
      AndExpression andExpression = (AndExpression) expr;
      Set<DataEntry> leftEntries = evaluateWhere(andExpression.getLeftExpression(), table);
      leftEntries.retainAll(evaluateWhere(andExpression.getRightExpression(), table));
      //System.out.println("and " + leftEntries.size());
      return leftEntries;
    } else if (expr instanceof OrExpression) {
      OrExpression orExpression = (OrExpression) expr;
      Set<DataEntry> leftEntries = evaluateWhere(orExpression.getLeftExpression(), table);
      leftEntries.addAll(evaluateWhere(orExpression.getRightExpression(), table));
      //System.out.println("or " + leftEntries.size());
      return leftEntries;
    } else if (expr instanceof InExpression) {
      InExpression inExpression = (InExpression) expr;
      //System.out.println("In");
      ExpressionList expressionList = (ExpressionList) inExpression.getRightItemsList();
      String left = ((Column) inExpression.getLeftExpression()).getFullyQualifiedName();
      List<Object> vals = expressionList.getExpressions().stream().map(f -> {
        if (f instanceof StringValue) {
          return ((StringValue) f).getValue();
        } else if (f instanceof DoubleValue) {
          return ((DoubleValue) f).getValue();
        } else if (f instanceof LongValue) {
          return ((LongValue) f).getValue();
        }
        return null;
      }).collect(Collectors.toList());
      Set<DataEntry> valz = table.in(left, vals);
      /*try {
        System.out.println(left + " = " + Serializer.getObjectMapper().getOm().writeValueAsString(vals) + " size: " + valz.size());
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }*/
      return valz;
    } else if (expr instanceof LikeExpression) {
      BinaryExpression binary = (BinaryExpression) expr;
      String left = ((Column) binary.getLeftExpression()).getFullyQualifiedName();
      String right = ((StringValue) binary.getRightExpression()).getValue();
      Set<DataEntry> vals = table.search(left, right);
//      System.out.println(left + " like " + right + " size: " + vals.size());
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
        if (rightExpression instanceof StringValue) {
          String right = ((StringValue) rightExpression).getValue();
          vals = table.get(left, right);
//          System.out.println(left + " = " + right + " size: " + vals.size());
        } else if (rightExpression instanceof DoubleValue) {
          Double right = ((DoubleValue) rightExpression).getValue();
          vals = table.get(left, right);
//          System.out.println(left + " = " + right + " size: " + vals.size());
        } else if (rightExpression instanceof LongValue) {
          Long right = ((LongValue) rightExpression).getValue();
          vals = table.get(left, right);
//          System.out.println(left + " = " + right + " size: " + vals.size());
        }
        return vals;
      } else if (expr instanceof NotEqualsTo) {
        String right = ((StringValue) binary.getRightExpression()).getValue();
//        System.out.println(left + " != " + right);
        return table.getNotEqual(left, right);
      } else {
        //System.out.println(binary.getClass().getName());
        //System.out.println(binary.getLeftExpression().getClass().getName());
        //System.out.println(binary.getRightExpression().getClass().getName());
      }
      // Other binary expressions can be added similarly
    } else {
      //System.out.println(expr.getClass().getName());
    }
    // Other conditions (like OrExpression) can be added similarly
    return new TreeSet<>();
  }

  public static DataEntry handleInsert(Insert sqlStatement, NucleoDB nucleoDB) {
    String tableName = sqlStatement.getTable().getName();
    try {
      //Serializer.log("looking at table: "+tableName);
      DataTable table = nucleoDB.getTable(tableName);
      Object obj = table.getConfig().getClazz().getConstructor().newInstance();

      String[] columns = sqlStatement.getSetColumns().stream().map(c -> c.getColumnName()).collect(Collectors.toList()).toArray(new String[0]);

      Expression[] expressions = sqlStatement.getSetExpressionList().toArray(new Expression[0]);
      for (int i = 0; i < columns.length; i++) {
        Field field = obj.getClass().getDeclaredField(columns[i]);
        setColumnVal(expressions[i], columns[i], obj, field);
      }
      CountDownLatch countDownLatch = new CountDownLatch(1);
      AtomicReference<DataEntry> dataEntry = new AtomicReference<>();
//      table.savInsert(obj, dataEntryNew -> {
//        dataEntry.set(dataEntryNew);
//        countDownLatch.countDown();
//      });
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
            //System.out.println("lost string");
          }
        } else {

          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(column, obj.getClass());
          propertyDescriptor.getWriteMethod().invoke(obj, ((StringValue) expression).getValue());
        }
      } else if (expression instanceof LongValue) {
        if (column == null) {
          if (obj instanceof Collection) {
            if (field != null) {
              ParameterizedType listType = (ParameterizedType) field.getGenericType();
              Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
              if (listValueType == Long.class) {
                ((Collection) obj).add(((LongValue) expression).getValue());
              } else if (listValueType == Integer.class) {
                ((Collection) obj).add(((LongValue) expression).getValue());
              } else {
                //Serializer.log("In LongValue " + listValueType.getName());
              }
            } else {
              ((Collection) obj).add(((LongValue) expression).getValue());
            }
          } else {
            //System.out.println("lost long");
          }
        } else {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(column, obj.getClass());
          if (propertyDescriptor.getPropertyType().isAssignableFrom(Integer.class)) {
            propertyDescriptor.getWriteMethod().invoke(obj, Long.valueOf(((LongValue) expression).getValue()).intValue());
          } else if (propertyDescriptor.getPropertyType().isAssignableFrom(Long.class)) {
            propertyDescriptor.getWriteMethod().invoke(obj, ((LongValue) expression).getValue());
          }
        }
      } else if (expression instanceof DoubleValue) {
        if (column == null) {
          if (obj instanceof Collection) {
            if (field != null) {
              ParameterizedType listType = (ParameterizedType) field.getGenericType();
              Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
              if (listValueType == Float.class) {
                ((Collection) obj).add(Double.valueOf(((DoubleValue) expression).getValue()).floatValue());
              } else if (listValueType == Double.class) {
                ((Collection) obj).add(((DoubleValue) expression).getValue());
              } else {
                //Serializer.log("In DoubleValue " + listValueType.getName());
              }
            } else {
              ((Collection) obj).add(((DoubleValue) expression).getValue());
            }
          } else {
            //System.out.println("lost long");
          }
        } else {
          PropertyDescriptor propertyDescriptor = new PropertyDescriptor(column, obj.getClass());
          if (propertyDescriptor.getPropertyType().isAssignableFrom(Float.class)) {
            propertyDescriptor.getWriteMethod().invoke(obj, Double.valueOf(((DoubleValue) expression).getValue()).floatValue());
          } else if (propertyDescriptor.getPropertyType().isAssignableFrom(Double.class)) {
            propertyDescriptor.getWriteMethod().invoke(obj, ((DoubleValue) expression).getValue());
          }
        }
      }else if (expression instanceof ValueListExpression){
        ((ValueListExpression) expression).getExpressionList().getExpressions().forEach(expr-> {
          try {
            setColumnVal(expr, column, obj, field);
          } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        });
      } else if (expression instanceof RowConstructor) {
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
      } else if (expression instanceof EqualsTo) {
        String columnName = ((Column) ((EqualsTo) expression).getLeftExpression()).getColumnName();
        PropertyDescriptor propertyDescriptor = new PropertyDescriptor(columnName, obj.getClass());
        if (propertyDescriptor.getPropertyType().isAssignableFrom(List.class)) {

          List listObject = (List) propertyDescriptor.getReadMethod().invoke(obj);
          if (listObject == null) {
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
        } else {
          setColumnVal(((EqualsTo) expression).getRightExpression(), columnName, obj, field);
        }
      } else if (expression instanceof Parenthesis) {
        setColumnVal(((Parenthesis) expression).getExpression(), column, obj, field);
      } else {
        //System.out.println("last: " + expression.getClass().getName());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static boolean handleUpdate(Update sqlStatement, NucleoDB nucleoDB) {
    String tableName = sqlStatement.getTable().getName();
    try {
      DataTable table = nucleoDB.getTable(tableName);

      Set<DataEntry> dataEntries = evaluateWhere(sqlStatement.getWhere(), table);

      for (UpdateSet updateSet : sqlStatement.getUpdateSets()) {
        List<String> columns = updateSet.getColumns().stream().map(col -> col.getFullyQualifiedName()).collect(Collectors.toList());
        List<Expression> expressions = updateSet.getExpressions();
        for (int i = 0; i < columns.size(); i++) {
          applyChangesToEntries(columns.get(i), expressions.get(i), dataEntries);
        }
      }
      AtomicReference<TreeSetExt<DataEntry>> dataEntriesList = new AtomicReference<>();
      dataEntriesList.set(new TreeSetExt<>());
      for (DataEntry dataEntryUnsaved : dataEntries) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        dataEntryUnsaved.versionIncrease();
        table.saveAsync(dataEntryUnsaved, dataEntryNew -> {
          dataEntriesList.get().add(dataEntryNew);
          countDownLatch.countDown();
        });
        countDownLatch.await();
      }
      //Serializer.log("Saved changed "+dataEntriesList.get().size());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public static void applyChangesToEntries(String column, Expression expression, Set<DataEntry> dataEntries) {
    Queue<String> columnPointer = Queues.newArrayDeque();
    dataEntries.forEach(de -> {
      columnPointer.addAll(Arrays.asList(column.split("\\.")));
      Object obj = de.getData();
      Field field = null;
      PropertyDescriptor propertyDescriptor = null;
      Class fieldClazz = null;
      try {
        if(field !=null) fieldClazz = Class.forName(field.getType().getTypeName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      String columnName = null;
      while (!columnPointer.isEmpty()) {
        columnName = columnPointer.poll();
        if (columnName.matches("i([0-9]+)")) {
          Pattern pattern = Pattern.compile("i([0-9]+)");
          Matcher matcher = pattern.matcher(columnName);
          if (matcher.find()) {
            int index = Integer.valueOf(matcher.group(1));
            if (fieldClazz.isAssignableFrom(List.class)) {
              ParameterizedType listType = (ParameterizedType) field.getGenericType();
              try {
                Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
                if (((List) obj).size() > index && !listValueType.isAssignableFrom(String.class)) {
                  obj = ((List) obj).get(index);
                } else {
                  columnName = null;
                }
              } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
              }
            }
          }
        } else if (columnName.equalsIgnoreCase("new")) {
          try {
            if (fieldClazz.isAssignableFrom(List.class)) {
              ParameterizedType listType = (ParameterizedType) field.getGenericType();
              Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
              Object o = listValueType.getConstructor().newInstance();
              ((List) obj).add(o);
              obj = o;
              columnName = null;
            } else {
              Object tmp = field.getDeclaringClass().getConstructor().newInstance();
              field.set(obj, tmp);
              obj = tmp;
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        } else {
          try {
            field = obj.getClass().getDeclaredField(columnName);
            propertyDescriptor = new PropertyDescriptor(columnName, obj.getClass());
            fieldClazz = Class.forName(field.getType().getTypeName());
          } catch (Exception e) {
            e.printStackTrace();
          }
          try {
            if (fieldClazz.isAssignableFrom(List.class)) {
              Object tmp = propertyDescriptor.getReadMethod().invoke(obj);
              ParameterizedType listType = (ParameterizedType) field.getGenericType();
              try {
                if (tmp == null) {
                  Class listValueType = Class.forName(listType.getActualTypeArguments()[0].getTypeName());
                  tmp = Serializer.getObjectMapper().getOm().readValue(
                      "[]",
                      Serializer.getObjectMapper().getOm().getTypeFactory().constructCollectionType(
                          List.class,
                          listValueType
                      )
                  );
                  field.set(obj, tmp);
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
              obj = tmp;
            }
          } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
          }
        }
      }
      try {
        setColumnVal(expression, columnName, obj, field);
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static boolean handleDelete(Delete sqlStatement, NucleoDB nucleoDB) {
    String tableName = sqlStatement.getTable().getName();
    try {
      DataTable table = nucleoDB.getTable(tableName);


      List<DataEntry> dataEntries = evaluateWhere(sqlStatement.getWhere(), table).stream().toList();

      if(sqlStatement.getOrderByElements()!=null) {
        dataEntries = (List<DataEntry>) dataEntries.stream().sorted(createSorting(sqlStatement.getOrderByElements())).collect(Collectors.toList());
      }

      int offset = 0;
      int count = Integer.MAX_VALUE;
      if(sqlStatement.getLimit()!=null){
        if(sqlStatement.getLimit().getOffset()!=null) {
          if(sqlStatement.getLimit().getOffset() instanceof LongValue){
            offset = Long.valueOf(((LongValue) sqlStatement.getLimit().getOffset()).getValue()).intValue();
          }
        }
        if(sqlStatement.getLimit().getRowCount() instanceof LongValue){
          count = Long.valueOf(((LongValue) sqlStatement.getLimit().getRowCount()).getValue()).intValue();
        }
      }

      dataEntries = dataEntries.subList((offset>dataEntries.size())?dataEntries.size():offset, (offset+count>dataEntries.size())?dataEntries.size():offset+count);

      //Serializer.log("TO DELETE");
      //Serializer.log(dataEntries);
      if(dataEntries.size()!=0) {
        CountDownLatch countDownLatch = new CountDownLatch(dataEntries.size());
        dataEntries.forEach(x -> {
          new Thread(() -> {
            if (table.getDataEntries().contains(x)) {
              table.deleteAsync(x, (d) -> {
                //Serializer.log("DELETED ");
                //Serializer.log(d);
                countDownLatch.countDown();
              });
            } else {
              countDownLatch.countDown();
            }
          }).start();
        });
        countDownLatch.await();
      }

      //Serializer.log("Deleted changed "+dataEntries.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }
}
