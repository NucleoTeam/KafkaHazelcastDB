package com.nucleocore.db.database.utils;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

public class User extends DataEntry {

    @Index(IndexType.TRIE)
    public long id;

    @Index(IndexType.TRIE)
    public String user;

    public String pass;
    public String email;
    public String apiKey;
    public Date joined;

    @Index(IndexType.TRIE)
    public long player;

    public boolean mod;
    public String ip;
    public boolean premium;
    public int level;
    public String position;
    public String registerip;
    public Integer disputeclosurecount;
    public String staffFlags;
    public boolean recentiplock;
    public boolean iplock;
    public long lastAction;
    public String reports;
    public int invalidReports;

    public User(){
        super(UUID.randomUUID().toString());
    }

    public User(long id, String user, String pass, String email, String apiKey, Timestamp joined, long player, boolean mod, String ip, boolean premium, int level, String position, String registerip, Integer disputeclosurecount, String staffFlags, boolean recentiplock, boolean iplock, long lastAction, String reports, int invalidReports) {
        this.id = id;
        this.user = user;
        this.pass = pass;
        this.email = email;
        this.apiKey = apiKey;
        this.joined = joined;
        this.player = player;
        this.mod = mod;
        this.ip = ip;
        this.premium = premium;
        this.level = level;
        this.position = position;
        this.registerip = registerip;
        this.disputeclosurecount = disputeclosurecount;
        this.staffFlags = staffFlags;
        this.recentiplock = recentiplock;
        this.iplock = iplock;
        this.lastAction = lastAction;
        this.reports = reports;
        this.invalidReports = invalidReports;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getApiKey() {
        return apiKey;
    }

    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public Date getJoined() {
        return joined;
    }

    public void setJoined(Date joined) {
        this.joined = joined;
    }

    public long getPlayer() {
        return player;
    }

    public void setPlayer(long player) {
        this.player = player;
    }

    public boolean getMod() {
        return mod;
    }

    public void setMod(boolean mod) {
        this.mod = mod;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public boolean isPremium() {
        return premium;
    }

    public void setPremium(boolean premium) {
        this.premium = premium;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getRegisterip() {
        return registerip;
    }

    public void setRegisterip(String registerip) {
        this.registerip = registerip;
    }

    public Integer getDisputeclosurecount() {
        return disputeclosurecount;
    }

    public void setDisputeclosurecount(Integer disputeclosurecount) {
        this.disputeclosurecount = disputeclosurecount;
    }

    public String getStaffFlags() {
        return staffFlags;
    }

    public void setStaffFlags(String staffFlags) {
        this.staffFlags = staffFlags;
    }

    public boolean isRecentiplock() {
        return recentiplock;
    }

    public void setRecentiplock(boolean recentiplock) {
        this.recentiplock = recentiplock;
    }

    public boolean isIplock() {
        return iplock;
    }

    public void setIplock(boolean iplock) {
        this.iplock = iplock;
    }

    public long getLastAction() {
        return lastAction;
    }

    public void setLastAction(long lastAction) {
        this.lastAction = lastAction;
    }

    public String getReports() {
        return reports;
    }

    public void setReports(String reports) {
        this.reports = reports;
    }

    public int getInvalidReports() {
        return invalidReports;
    }

    public void setInvalidReports(int invalidReports) {
        this.invalidReports = invalidReports;
    }
}
