package com.nucleocore.db.database.utils;

import java.sql.Date;
import java.sql.Timestamp;

public class Player extends DataEntry {

    @Index()
    public long playerId; // legacy player id

    @Index(IndexType.SETFULLTEXT)
    public String name;


    @Index()
    public String id;

    public Player(){
        super();
    }
    public Player(long playerId, String name,  String id) {
        super();
        this.playerId = playerId;
        this.name = name;
        this.id = id;
    }

    public long getPlayerId() {
        return playerId;
    }

    public void setPlayerId(long playerId) {
        this.playerId = playerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
