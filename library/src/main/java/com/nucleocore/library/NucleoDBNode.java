package com.nucleocore.library;

import com.github.f4b6a3.uuid.UuidCreator;
import com.nucleocore.library.negotiator.decision.hash.responses.ReasonResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
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
      recentActions.add(System.currentTimeMillis()+2000);
      usedSlots.incrementAndGet();
    }
  };

  public NucleoDBNode(long slots) {
    this.slots = slots;
    this.execute();
  }


  public long getHits() {
    return hits.get();
  }
  public long getOpenSlots(){
    return slots - (usedSlots.get()+getAdjustedResources());
  }

  public double[] getLoad() {
    return load;
  }
  public ReasonResponse.CPUPercent[] getCPUPercent() {
//    int len = cpuPercs.length;
//    ReasonResponse.CPUPercent[] cpuPercVals = new ReasonResponse.CPUPercent[len];
//    double adjustment = getAdjustedResources()*0.03;
//    for(int i=0;i<len;i++){
//      cpuPercVals[i] = new ReasonResponse.CPUPercent(cpuPercs[i].getCombined()+adjustment, cpuPercs[i].getIdle()-adjustment, cpuPercs[i].getUser()+adjustment, cpuPercs[i].getSys(), cpuPercs[i].getNice());
//    }
    return null;
  }
  public ReasonResponse.Memory getMemory() {
//    long adjustment = getAdjustedResources()*256*7;
//    return new ReasonResponse.Memory(mem.getTotal(), mem.getUsed()+adjustment, mem.getActualUsed()+adjustment, mem.getFree()-adjustment, mem.getActualFree()-adjustment);
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

  //Mem mem;
  //CpuPerc[] cpuPercs;
  double[] load;

  public void execute() {
//    Executors.newFixedThreadPool(1).execute(()-> {
//      Sigar sigar = new Sigar();
//      while (true) {
//        try {
//          mem = sigar.getMem();
//          load = sigar.getLoadAverage();
//          cpuPercs = sigar.getCpuPercList();
//        } catch (SigarException e) {
//          e.printStackTrace();
//        }
//        try {
//          Thread.sleep(400);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//      }
//    });
  }

  public void setData(Map<String, Object> objects) {
    objects.put("load", this.getLoad());
    objects.put("cpu", this.getCPUPercent());
    objects.put("hits", this.getHits());
    objects.put("slots", this.getOpenSlots());
    objects.put("memory", this.getMemory());
  }
}
