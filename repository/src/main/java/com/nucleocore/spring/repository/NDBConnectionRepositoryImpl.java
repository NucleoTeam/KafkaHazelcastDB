package com.nucleocore.spring.repository;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.connection.Connection;
import com.nucleocore.library.database.tables.connection.ConnectionHandler;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.tables.table.DataTable;
import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;

public class NDBConnectionRepositoryImpl<C extends Connection, ID extends String> implements NDBRepository<C, ID>{
  private @Nullable ConnectionHandler connectionHandler = null;
  private final NucleoDB nucleoDB;
  private final Class<C> classType;
  private @Nullable Class<?> fromClass = null;
  private @Nullable Class<?> toClass = null;
  public NDBConnectionRepositoryImpl(NucleoDB nucleoDB, Class<C> classType) {
    this.nucleoDB = nucleoDB;
    this.classType = classType;
    Type[] actualTypeArguments = ((ParameterizedType) classType.getGenericSuperclass()).getActualTypeArguments();
    if(actualTypeArguments.length==1) {
      this.fromClass = (Class<?>) actualTypeArguments[0];
      this.toClass = (Class<?>) actualTypeArguments[1];
    }
    this.connectionHandler = nucleoDB.getConnectionHandler(classType);
  }

  @Override
  public <S extends C> List<S> saveAll(Iterable<S> entities) {
    return null;
  }

  @Override
  public List<C> findAll() {
    return null;
  }

  @Override
  public List<C> findAllById(Iterable<ID> iterable) {
    return null;
  }

  @Override
  public List<C> findAll(Sort sort) {
    return null;
  }

  @Override
  public <S extends C> S save(S entity) {
    return null;
  }

  @Override
  public Optional<C> findById(ID id) {
    return Optional.empty();
  }

  @Override
  public boolean existsById(ID id) {
    return false;
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(ID id) {

  }

  @Override
  public void delete(C entity) {

  }

  @Override
  public void deleteAllById(Iterable<? extends ID> ids) {

  }

  @Override
  public void deleteAll(Iterable<? extends C> entities) {

  }

  @Override
  public void deleteAll() {

  }
}
