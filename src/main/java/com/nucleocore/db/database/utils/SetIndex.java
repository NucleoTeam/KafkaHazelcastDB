package com.nucleocore.db.database.utils;

import com.google.common.collect.Sets;
import com.nucleocore.db.database.Table;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SetIndex {
  public class CharacterIndex {
    private char character;
    private String variable;
    private Set<Integer> entries;
    public CharacterIndex(char id) {
      this.character = id;
      this.entries = new LinkedHashSet<>();
    }
  }
  private Table table;
  private TreeMap<Character, CharacterIndex> characters = new TreeMap<>();
  public SetIndex(Table table) {
    this.table = table;
  }
  public void add(String variable, String word, Integer id){
    for(char c :word.toCharArray()){
      CharacterIndex ci = getCharacterIndex(c);
      if(ci==null){
        ci = new CharacterIndex(c);
      }
      ci.entries.add(id);
    }
  }
  public CharacterIndex getCharacterIndex(char x){
    List<CharacterIndex> chars = characterList.parallelStream().filter(y->y.character==x).collect(Collectors.toList());
    if(chars.size()>0){
      return chars.get(0);
    }
    return null;
  }
  public Set<Integer> search(String variable, String word){
    List<CharacterIndex> ciList;
    if(characters.containsKey(variable)){
      ciList = characters.get(variable);
    }else{
      return null;
    }
    Set<Integer> entries = null;
    for(char c :word.toCharArray()){
      CharacterIndex ci = getCharacterIndex(c, ciList);
      if(ci!=null){
        if(entries!=null){
          entries = Sets.intersection(ci.entries, entries);
        }else{
          entries = ci.entries;
        }
      }else{
        return null;
      }
    }
    return entries.parallelStream().filter(e->{
      try {
        if (table.getMap().containsKey("" + e)) {
          DataEntry de = table.getMap().get("" + e);
          return ((String) de.getClass().getField(variable).get(de)).contains(word);
        }
      }catch (NoSuchFieldException | IllegalAccessException x){
      }
      return false;
    }).collect(Collectors.toSet());
  }
}
