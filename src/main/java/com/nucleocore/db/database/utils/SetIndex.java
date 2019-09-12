package com.nucleocore.db.database.utils;

import com.google.common.collect.Sets;
import com.google.common.collect.Maps;
import java.util.*;
import java.util.stream.Stream;

public class SetIndex {
  public class CharacterIndex {
    private char character;
    private String variable;
    private Set<Integer> entries;
    public CharacterIndex(char id) {
      this.character = id;
      this.entries = Sets.newTreeSet();
    }
  }
  private HashMap<String, TreeMap<Integer, List<CharacterIndex>>> characters = new HashMap<>();
  public SetIndex() {

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
        characters.put(field, Maps.newTreeMap());
        charFieldIndex = characters.get(field);
      } else {
        return null;
      }
      if (charFieldIndex.containsKey(position)) {
        Stream<CharacterIndex> stream = charFieldIndex.get(position).stream();
        Optional<CharacterIndex> characterList = stream.filter(y -> y.character == x).findFirst();
        stream.close();
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
  public Set<Integer> search(String variable, String word, int startIndex){
    Set<Integer> entries = null;
    int x = startIndex;
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
    return entries;
  }
  public void deleteCharIndex(String field, int position, char character){
    synchronized (characters) {
      TreeMap<Integer, List<CharacterIndex>> charFieldIndex = null;
      if (characters.containsKey(field)) {
        charFieldIndex = characters.get(field);
        if (charFieldIndex.containsKey(position)) {
          List<CharacterIndex> ci = charFieldIndex.get(position);
          Stream<CharacterIndex> stream = ci.stream();
          Optional<CharacterIndex> characterList = stream.filter(y -> y.character == character).findFirst();
          stream.close();
          if (characterList.isPresent()) {
            ci.remove(characterList.get());
            if(ci.size()==0){
              charFieldIndex.remove(position);
            }
          }
        }
      }
    }
  }
  public void delete(String variable, String word, Integer id){
    int position = 0;
    for(char character :word.toCharArray()){
      CharacterIndex ci = getCharacterIndex(variable, position, character, false);
      ci.entries.remove(id);
      if(ci.entries.size()==0){
        deleteCharIndex(variable, position, character);
      }
      position++;
    }
  }
}
