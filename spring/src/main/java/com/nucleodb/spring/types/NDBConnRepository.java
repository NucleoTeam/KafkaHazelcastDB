package com.nucleodb.spring.types;

import com.nucleocore.library.database.tables.connection.Connection;
import com.nucleocore.library.database.tables.connection.ConnectionProjection;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.utils.Pagination;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface NDBConnRepository<C extends Connection<T, F>, ID extends String, T extends DataEntry, F extends DataEntry> extends CrudRepository<C, ID>{

  Set<C> getByTo(T entity);
  Set<C> getByTo(T entity, Pagination pagination);
  Set<C> getByTo(T entity, Predicate<C> filter);
  Set<C> getByTo(T entity, Pagination pagination, Predicate<C> filter);
  Set<F> getFromByTo(T entity);
  Set<F> getFromByTo(T entity, Pagination pagination);
  Set<F> getFromByTo(T entity, Predicate<C> filter);
  Set<F> getFromByTo(T entity, Pagination pagination, Predicate<C> filter);
  Set<C> getByFrom(F entity);
  Set<C> getByFrom(F entity, Predicate<C> filter);
  Set<C> getByFrom(F entity, Pagination pagination);
  Set<C> getByFrom(F entity, Pagination pagination, Predicate<C> filter);
  Set<T> getToByFrom(F entity);
  Set<T> getToByFrom(F entity, Pagination pagination);
  Set<T> getToByFrom(F entity, Predicate<C> filter);
  Set<T> getToByFrom(F entity, Pagination pagination, Predicate<C> filter);

}
