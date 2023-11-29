package com.nucleocore.test.spring.repo;

import com.nucleocore.spring.repository.types.NDBConnRepository;
import com.nucleocore.test.domain.AnimeDE;
import com.nucleocore.test.domain.UserDE;
import com.nucleocore.test.domain.WatchingConnection;
import org.springframework.stereotype.Repository;

@Repository
public interface WatchingConnectionRepository extends NDBConnRepository<WatchingConnection, String, AnimeDE, UserDE>{
}
