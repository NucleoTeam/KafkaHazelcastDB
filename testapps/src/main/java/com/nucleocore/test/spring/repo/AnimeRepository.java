package com.nucleocore.test.spring.repo;

import com.nucleocore.spring.repository.NDBRepository;
import com.nucleocore.test.common.AnimeDE;
import org.springframework.stereotype.Repository;

@Repository
public interface AnimeRepository extends NDBRepository<AnimeDE, String>{
  AnimeDE findByName(String key);
}
