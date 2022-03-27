package com.nucleocore.nucleodb;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.nucleocore.nucleodb.negotiator.decision.hash.responses.ReasonResponse;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NucleoDBNode {
  String uniqueId = UuidCreator.getTimeBasedWithRandom().toString();

  AtomicLong hits = new AtomicLong(0);

  long slots = 0;
  AtomicLong usedSlots = new AtomicLong(0);

  private List<Long> recentActions = new LinkedList<>();

  public int getAdjustedResources(){
    synchronized (recentActions){
      recentActions.removeIf(c->c<System.currentTimeMillis());
    }
    return recentActions.size();
  }
  public void insertTempAdjustment(){
    synchronized (recentActions){
      recentActions.add(System.currentTimeMillis()+3000);
      usedSlots.incrementAndGet();
    }
  };

  public NucleoDBNode(long slots) {
    this.slots = slots+getAdjustedResources();
  }


  public long getHits() {
    return hits.get();
  }
  public long getOpenSlots(){
    return slots - usedSlots.get();
  }

  public double[] getLoad() {
    Sigar sigar=new Sigar();
    try {
      return sigar.getLoadAverage();
    }catch (SigarException e){
      e.printStackTrace();
    }
    return null;
  }
  public ReasonResponse.CPUPercent[] getCPUPercent() {
    Sigar sigar=new Sigar();
    try {
      CpuPerc[] cpuPercs = sigar.getCpuPercList();
      int len = cpuPercs.length;
      ReasonResponse.CPUPercent[] cpuPercVals = new ReasonResponse.CPUPercent[len];
      double adjustment = getAdjustedResources()*0.02;
      System.out.println("cpu: "+adjustment);
      for(int i=0;i<len;i++){
        cpuPercVals[i] = new ReasonResponse.CPUPercent(cpuPercs[i].getCombined()+adjustment, cpuPercs[i].getIdle()-adjustment, cpuPercs[i].getUser()+adjustment, cpuPercs[i].getSys(), cpuPercs[i].getNice());
      }
      return cpuPercVals;
    }catch (SigarException e){
      e.printStackTrace();
    }
    return null;
  }
  public ReasonResponse.Memory getMemory() {
    Sigar sigar=new Sigar();
    try {
      Mem mem = sigar.getMem();
      long adjustment = getAdjustedResources()*1024*1;
      System.out.println("mem: "+adjustment);
      return new ReasonResponse.Memory(mem.getTotal(), mem.getUsed()+adjustment, mem.getActualUsed()+adjustment, mem.getFree()-adjustment, mem.getActualFree()-adjustment);
    }catch (SigarException e){
      e.printStackTrace();
    }
    return null;
  }

  public String getUniqueId() {
    return uniqueId;
  }

  public void setUniqueId(String uniqueId) {
    this.uniqueId = uniqueId;
  }

  public long getSlots() {
    return slots;
  }

  public long getUsedSlots() {
    return usedSlots.get();
  }
}
