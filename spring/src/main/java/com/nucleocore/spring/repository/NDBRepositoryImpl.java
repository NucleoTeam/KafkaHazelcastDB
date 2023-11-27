package com.nucleocore.spring.repository;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.table.DataEntry;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;

public class NDBRepositoryImpl<T extends DataEntry, ID extends String> implements NDBRepository<T, ID>{
  NucleoDB nucleoDB;
  Class<T> classType;
  public NDBRepositoryImpl(NucleoDB nucleoDB, Class<T> classType) {
    this.nucleoDB = nucleoDB;
    this.classType = classType;
  }

  @Override
  public <S extends T> S save(S entity) {
    return null;
  }

  @Override
  public <S extends T> List<S> saveAll(Iterable<S> entities) {
    return null;
  }

  @Override
  public Optional<T> findById(ID id) {
    return Optional.empty();
  }

  @Override
  public boolean existsById(ID id) {
    return false;
  }

  @Override
  public List<T> findAll() {
    return (List<T>) nucleoDB.getTable(classType).getEntries().stream().toList();
  }

  @Override
  public List<T> findAllById(Iterable<ID> iterable) {
    return null;
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(ID id) {

  }

  @Override
  public void delete(T entity) {

  }

  @Override
  public void deleteAllById(Iterable<? extends ID> ids) {

  }

  @Override
  public void deleteAll(Iterable<? extends T> entities) {

  }

  @Override
  public void deleteAll() {

  }

  @Override
  public List<T> findAll(Sort sort) {
    return null;
  }
}
