package com.nucleocore.nucleodb.negotiator.decision.hash.responses;

import java.io.Serializable;

public class ReasonResponse implements Serializable {

  public static class Memory implements Serializable{
    public long total, used, actualUsed, free, actualFree;

    public Memory(long total, long used, long actualUsed, long free, long actualFree) {
      this.total = total;
      this.used = used;
      this.actualUsed = actualUsed;
      this.free = free;
      this.actualFree = actualFree;
    }
    public Memory() {
    }
  }
  public static class CPUPercent implements Serializable{
    public double combined, idle, user, sys, nice;
    public CPUPercent(double combined, double idle, double user, double sys, double nice) {
      this.combined = combined;
      this.idle = idle;
      this.user = user;
      this.sys = sys;
      this.nice = nice;
    }
    public CPUPercent() {
    }
  }
  long openSlots;
  long hits;
  double[] load;
  CPUPercent[] cpuPercent;
  Memory memory;


  public ReasonResponse(long openSlots, long hits, double[] load, CPUPercent[] cpuPercent, Memory memory) {
    this.openSlots = openSlots;
    this.hits = hits;
    this.load = load;
    this.cpuPercent = cpuPercent;
    this.memory = memory;
  }

  public long getOpenSlots() {
    return openSlots;
  }

  public void setOpenSlots(long openSlots) {
    this.openSlots = openSlots;
  }

  public long getHits() {
    return hits;
  }

  public void setHits(long hits) {
    this.hits = hits;
  }

  public double[] getLoad() {
    return load;
  }

  public void setLoad(double[] load) {
    this.load = load;
  }

  public CPUPercent[] getCpuPercent() {
    return cpuPercent;
  }

  public void setCpuPercent(CPUPercent[] cpuPercent) {
    this.cpuPercent = cpuPercent;
  }

  public Memory getMemory() {
    return memory;
  }

  public void setMemory(Memory memory) {
    this.memory = memory;
  }
}
