package com.nucleocore.test.spring;

import com.nucleocore.test.spring.repo.AnimeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AnimeController{
  @Autowired
  AnimeRepository animeRepository;

  @GetMapping("/")
  public String test(){
    return animeRepository.findAll().stream().findFirst().get().getKey();
  }
}
