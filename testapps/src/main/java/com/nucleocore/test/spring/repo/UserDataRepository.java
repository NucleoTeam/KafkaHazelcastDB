package com.nucleocore.test.spring.repo;

import com.nucleocore.spring.repository.types.NDBDataRepository;
import com.nucleocore.test.domain.UserDE;
import org.springframework.stereotype.Repository;

@Repository
public interface UserDataRepository extends NDBDataRepository<UserDE, String>{
}
