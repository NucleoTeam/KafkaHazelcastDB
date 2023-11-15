package com.nucleocore.test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.nucleocore.library.database.utils.sql.PrimaryKey;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class AnimeDTO{
  public static class CustomActorNameDeserializer
      extends StdDeserializer<List<String>>{
    public CustomActorNameDeserializer() {
      this(null);
    }
    public CustomActorNameDeserializer(Class<?> vc) {
      super(vc);
    }
    @Override
    public List<String> deserialize(JsonParser jsonparser, DeserializationContext context) throws IOException, JacksonException {
      List<VoiceActor> actorz = jsonparser.readValueAs( new TypeReference<List<VoiceActor>>(){});
      return actorz.stream().map(a->a.name).collect(Collectors.toList());
    }
  }

  @PrimaryKey
  String uniqueKey;
  @JsonProperty("name")
  String name;

  @JsonProperty("actors")
  @JsonDeserialize(using = CustomActorNameDeserializer.class)
  List<String> actors = new LinkedList<>();

  @JsonProperty("rating")
  Float rating;



  public AnimeDTO() {
  }

  public String getUniqueKey() {
    return uniqueKey;
  }

  public void setUniqueKey(String uniqueKey) {
    this.uniqueKey = uniqueKey;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getActors() {
    return actors;
  }

  public void setActors(List<String> actors) {
    this.actors = actors;
  }

  public Float getRating() {
    return rating;
  }

  public void setRating(Float rating) {
    this.rating = rating;
  }
}
