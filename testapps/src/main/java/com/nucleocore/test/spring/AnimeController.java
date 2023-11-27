package com.nucleocore.test.spring;

import com.nucleocore.test.common.Anime;
import com.nucleocore.test.common.AnimeDE;
import com.nucleocore.test.spring.repo.AnimeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Optional;

@RestController
public class AnimeController{
  @Autowired
  AnimeRepository animeRepository;

  @GetMapping("/")
  public List<AnimeDE> test(){
    return animeRepository.findAll();
  }
  @GetMapping("/save")
  public AnimeDE saveTest(){
    return animeRepository.save(new AnimeDE(new Anime("woot test", 2.00f)));
  }

  @GetMapping("/id")
  public Optional<AnimeDE> idTest(){
    return animeRepository.findById("47cc9cc8-9fd4-4e54-97da-071c5a75fce2");
  }

  @GetMapping("/query")
  public AnimeDE queryTest(){
    return animeRepository.findByName("test");
  }
}
