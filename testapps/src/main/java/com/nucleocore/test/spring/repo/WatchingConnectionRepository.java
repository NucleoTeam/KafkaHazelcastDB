package com.nucleocore.test.spring.repo;

import com.nucleocore.spring.repository.NDBRepository;
import com.nucleocore.test.common.AnimeDE;
import com.nucleocore.test.common.WatchingConnection;
import org.springframework.stereotype.Repository;

@Repository
public interface WatchingConnectionRepository extends NDBRepository<WatchingConnection, String> {
}
