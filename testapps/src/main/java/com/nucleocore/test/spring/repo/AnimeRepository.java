package com.nucleocore.test.spring.repo;

import com.nucleocore.spring.repository.NDBTableRepository;
import com.nucleocore.test.common.AnimeDE;
import org.springframework.stereotype.Repository;

@Repository
public interface AnimeRepository extends NDBTableRepository<AnimeDE, String>{
  AnimeDE findByKey(String key);
}
