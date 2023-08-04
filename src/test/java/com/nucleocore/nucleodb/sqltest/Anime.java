package com.nucleocore.nucleodb.sqltest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Anime implements Serializable{
  private static final long serialVersionUID = 1;
  String name;
  List<String> tags = new ArrayList<>();
  List<VoiceActor> actors;
  String image;

  public Anime() {
  }

  public Anime(String name, List<String> tags) {
    this.name = name;
    this.tags = tags;
  }

  public Anime(String name, List<String> tags, List<VoiceActor> actors) {
    this.name = name;
    this.tags = tags;
    this.actors = actors;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public List<VoiceActor> getActors() {
    return actors;
  }

  public void setActors(List<VoiceActor> actors) {
    this.actors = actors;
  }
}
