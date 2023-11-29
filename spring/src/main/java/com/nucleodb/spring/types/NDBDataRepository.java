package com.nucleodb.spring.types;

import org.springframework.data.domain.Sort;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.NoRepositoryBean;

import java.util.List;


@NoRepositoryBean
public interface NDBDataRepository<T, ID> extends CrudRepository<T, ID>{

  <S extends T> List<S> saveAll(Iterable<S> entities);

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.CrudRepository#findAll()
   */

  List<T> findAll();

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.CrudRepository#findAllById(java.lang.Iterable)
   */

  List<T> findAllById(Iterable<ID> iterable);

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
   */
  List<T> findAll(Sort sort);

}