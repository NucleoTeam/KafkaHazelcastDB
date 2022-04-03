package com.nucleocore.nucleodb.negotiator.examples;

import com.nucleocore.nucleodb.NucleoDBNode;
import com.nucleocore.nucleodb.negotiator.GroupNegotiator;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GNExample {
  public static void main(String[] args) {
    NucleoDBNode node = new NucleoDBNode(500);
    GroupNegotiator groupNegotiator = new GroupNegotiator(node, "main", "192.168.122.80:9092,192.168.122.206:9092,192.168.122.196:9092");
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    executorService.scheduleAtFixedRate(() -> groupNegotiator.initial(UUID.randomUUID().toString().substring(0,6), 3), 0, 3, TimeUnit.SECONDS);
  }
}
