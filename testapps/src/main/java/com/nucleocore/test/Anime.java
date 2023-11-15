package com.nucleocore.test;

import com.nucleocore.library.database.tables.annotation.Index;
import com.nucleocore.library.database.tables.annotation.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Table("anime")
public class Anime implements Serializable{
  private static final long serialVersionUID = 1;
  @Index()
  String name;
  List<String> tags = new ArrayList<>();
  List<VoiceActor> actors;
  String image;

  String owner;

  Float rating;

  List<Float> votes;

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

  public Anime(String name, Float rating) {
    this.name = name;
    this.rating = rating;
  }

  public Anime(String name, List<String> tags, Float rating) {
    this.name = name;
    this.tags = tags;
    this.rating = rating;
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

  public Float getRating() {
    return rating;
  }

  public void setRating(Float rating) {
    this.rating = rating;
  }

  public List<Float> getVotes() {
    return votes;
  }

  public void setVotes(List<Float> votes) {
    this.votes = votes;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }
}
