package com.nucleocore.test.spring.repo;

import com.nucleocore.spring.repository.types.NDBDataRepository;
import com.nucleocore.test.domain.AnimeDE;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public interface AnimeDataRepository extends NDBDataRepository<AnimeDE, String>{
  Set<AnimeDE> findByName(String key);
}
