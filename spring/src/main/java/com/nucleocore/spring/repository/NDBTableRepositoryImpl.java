package com.nucleocore.spring.repository;

import com.nucleocore.library.NucleoDB;
import com.nucleocore.library.database.tables.table.DataEntry;
import com.nucleocore.library.database.utils.Serializer;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Repository
@Transactional(readOnly = true)
public class NDBTableRepositoryImpl<T extends DataEntry, String> implements NDBTableRepository<T, String>{
  private final NucleoDB nucleoDB;
  protected NDBTableRepositoryImpl(NucleoDB nucleoDB) {
    this.nucleoDB = nucleoDB;
  }

  @Override
  public <S extends T> List<S> saveAll(Iterable<S> entities) {
    Serializer.log(entities);
    return null;
  }

  @Override
  public List<T> findAll() {
    return null;
  }

  @Override
  public List<T> findAllById(Iterable<String> iterable) {
    return null;
  }

  @Override
  public List<T> findAll(Sort sort) {
    return null;
  }

  @Override
  public <S extends T> S save(S entity) {
    return null;
  }

  @Override
  public Optional<T> findById(String string) {
    return Optional.empty();
  }

  @Override
  public boolean existsById(String string) {
    return false;
  }

  @Override
  public long count() {
    return 0;
  }

  @Override
  public void deleteById(String string) {

  }

  @Override
  public void delete(T entity) {

  }

  @Override
  public void deleteAllById(Iterable<? extends String> strings) {

  }

  @Override
  public void deleteAll(Iterable<? extends T> entities) {

  }

  @Override
  public void deleteAll() {

  }

  @Override
  public Page<T> findAll(Pageable pageable) {
    return null;
  }
}
