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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class NucleoDBNode {
  String uniqueId = UuidCreator.getTimeBasedWithRandom().toString();

  AtomicLong hits = new AtomicLong(0);

  long slots = 0;
  AtomicLong usedSlots = new AtomicLong(0);

  private static LoadingCache<String, String> tempApplier = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .expireAfterWrite(15, TimeUnit.MILLISECONDS)
    .build(
      new CacheLoader<>() {
        @Override
        public String load(String key) {
          return null;
        }
      }
    );

  public static int getAdjustedResources(){
    return Long.valueOf(tempApplier.size()).intValue();
  }
  public static void insertTempAdjustment(String hash){
    tempApplier.put(UUID.randomUUID().toString(), hash);
  };

  public NucleoDBNode(long slots) {
    this.slots = slots;
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
      for(int i=0;i<len;i++){
        cpuPercVals[i] = new ReasonResponse.CPUPercent(cpuPercs[i].getCombined(), cpuPercs[i].getIdle(), cpuPercs[i].getUser(), cpuPercs[i].getSys(), cpuPercs[i].getNice());
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
      return new ReasonResponse.Memory(mem.getTotal(), mem.getUsed(), mem.getActualUsed(), mem.getFree(), mem.getActualFree());
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
