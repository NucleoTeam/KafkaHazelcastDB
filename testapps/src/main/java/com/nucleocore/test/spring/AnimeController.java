package com.nucleocore.test.spring;

import com.nucleocore.test.domain.Anime;
import com.nucleocore.test.domain.AnimeDE;
import com.nucleocore.test.domain.User;
import com.nucleocore.test.domain.UserDE;
import com.nucleocore.test.domain.WatchingConnection;
import com.nucleocore.test.spring.repo.AnimeDataRepository;
import com.nucleocore.test.spring.repo.UserDataRepository;
import com.nucleocore.test.spring.repo.WatchingConnectionRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@RestController
public class AnimeController{
  @Autowired
  AnimeDataRepository animeRepository;
  @Autowired
  WatchingConnectionRepository watchingConnectionRepository;
  @Autowired
  UserDataRepository userRepository;

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
  public Set<AnimeDE> queryTest(){
    return animeRepository.findByName("test");
  }

  @GetMapping("/watch")
  public WatchingConnection getWatching(){
    UserDE firestar = userRepository.save(new UserDE(new User("Nathaniel Davidson", "firestar")));
    AnimeDE hellsingUltimate = animeRepository.save(new AnimeDE(new Anime("Hellsing Ultimate", 8.35f)));
    return watchingConnectionRepository.save(new WatchingConnection(firestar, hellsingUltimate));
  }

  @GetMapping("/watch/id/{id}")
  public Optional<WatchingConnection> get(@PathVariable String id){
    return watchingConnectionRepository.findById(id);
  }

  @GetMapping("/watch/from/{id}")
  public Set<WatchingConnection> getByFrom(@PathVariable String id){
    Optional<UserDE> byId = userRepository.findById(id);
    if(byId.isPresent()){
      return watchingConnectionRepository.getByFrom(byId.get());
    }
    return null;
  }
  @GetMapping("/watch/from/{id}/to")
  public Set<AnimeDE> getByFromReturnTo(@PathVariable String id){
    Optional<UserDE> byId = userRepository.findById(id);
    if(byId.isPresent()){
      return watchingConnectionRepository.getToByFrom(byId.get());
    }
    return null;
  }
  @GetMapping("/watch/to/{id}")
  public Set<WatchingConnection> getByTo(@PathVariable String id){
    Optional<AnimeDE> byId = animeRepository.findById(id);
    if(byId.isPresent()){
      return watchingConnectionRepository.getByTo(byId.get());
    }
    return null;
  }

  @GetMapping("/watch/to/{id}/from")
  public Set<UserDE> getByToReturnFrom(@PathVariable String id){
    Optional<AnimeDE> byId = animeRepository.findById(id);
    if(byId.isPresent()){
      return watchingConnectionRepository.getFromByTo(byId.get());
    }
    return null;
  }

}
