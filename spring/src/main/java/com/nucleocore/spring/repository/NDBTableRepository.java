package com.nucleocore.spring.repository;

import org.springframework.data.domain.Sort;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.QueryByExampleExecutor;

import java.util.List;


public interface NDBTableRepository<T, ID> extends PagingAndSortingRepository<T, ID>, CrudRepository<T, ID>{
  @Override
  <S extends T> List<S> saveAll(Iterable<S> entities);

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.CrudRepository#findAll()
   */
  @Override
  List<T> findAll();

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.CrudRepository#findAllById(java.lang.Iterable)
   */
  @Override
  List<T> findAllById(Iterable<ID> iterable);

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
   */
  @Override
  List<T> findAll(Sort sort);

}