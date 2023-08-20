package com.nucleocore.library.test.sql;

import java.io.Serializable;
import java.util.List;

public class VoiceActor implements Serializable{
  private static final long serialVersionUID = 1;
  String name;
  String character;

  List<String> tags;

  public VoiceActor() {
  }

  public VoiceActor(String name) {
    this.name = name;
  }

  public VoiceActor(String name, String character) {
    this.name = name;
    this.character = character;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCharacter() {
    return character;
  }

  public void setCharacter(String character) {
    this.character = character;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }
}
