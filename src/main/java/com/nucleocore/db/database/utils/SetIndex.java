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
  private HashMap<String, TreeMap<Integer, List<CharacterIndex>>> characters = new HashMap<>();
  public SetIndex(Table table) {
    this.table = table;
  }
  public void add(String field, String word, Integer id){
    int x = 0;
    for(char c :word.toCharArray()){
      CharacterIndex ci = getCharacterIndex(field, x, c, true);
      ci.entries.add(id);
      x++;
    }
  }
  public CharacterIndex getCharacterIndex(String field, int position, char x, boolean create){
    synchronized (characters) {
      TreeMap<Integer, List<CharacterIndex>> charFieldIndex = null;
      if (characters.containsKey(field)) {
        charFieldIndex = characters.get(field);
      } else if (create) {
        characters.put(field, new TreeMap<>());
        charFieldIndex = characters.get(field);
      } else {
        return null;
      }
      if (charFieldIndex.containsKey(position)) {
        Optional<CharacterIndex> characterList = charFieldIndex.get(position).parallelStream().filter(y -> y.character == x).findFirst();
        if (characterList.isPresent()) {
          return characterList.get();
        }else{
          CharacterIndex ci = new CharacterIndex(x);
          charFieldIndex.get(position).add(ci);
          return ci;
        }
      } else if (create) {
        CharacterIndex ci = new CharacterIndex(x);
        charFieldIndex.put(position, new LinkedList() {{
          add(ci);
        }});
        return ci;
      }
    }
    return null;
  }
  public Set<Integer> search(String variable, String word){
    Set<Integer> entries = null;
    int x = 0;
    for(char c :word.toCharArray()){
      CharacterIndex ci = getCharacterIndex(variable, x, c, false);
      if(ci!=null){
        if(entries!=null){
          entries = Sets.intersection(ci.entries, entries);
        }else{
          entries = ci.entries;
        }
      }else{
        return null;
      }
      x++;
    }
    return entries.parallelStream().filter(e->{
      try {
        if (table.getMap().containsKey("" + e)) {
          DataEntry de = table.getMap().get("" + e);
          return ((String) de.getClass().getField(variable).get(de)).contains(word);
        }
      }catch (NoSuchFieldException | IllegalAccessException a){
      }
      return false;
    }).collect(Collectors.toSet());
  }
}
