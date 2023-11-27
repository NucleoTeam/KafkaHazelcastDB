package com.nucleocore.test.spring;

import com.nucleocore.test.common.AnimeDE;
import com.nucleocore.test.spring.repo.AnimeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
public class AnimeController{
  @Autowired
  AnimeRepository animeRepository;

  @GetMapping("/")
  public List<AnimeDE> test(){
    return (List<AnimeDE>) animeRepository.findAll();
  }
}
