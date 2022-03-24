package com.nucleocore.nucleodb.negotiator.decision.hash;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.nucleocore.nucleodb.NucleoDBNode;
import com.nucleocore.nucleodb.negotiator.decision.hash.responses.ConsensusResponse;
import com.nucleocore.nucleodb.negotiator.decision.hash.responses.HashArgumentResponseTrack;
import com.nucleocore.nucleodb.negotiator.decision.hash.responses.NodeSortWeight;
import com.nucleocore.nucleodb.negotiator.decision.hash.responses.ReasonResponse;
import com.nucleocore.nucleodb.negotiator.decision.support.*;
import org.apache.commons.collections4.map.HashedMap;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HashProcess implements ArgumentProcess, Serializable {

  static LoadingCache<String, HashArgument> hashArguments = CacheBuilder.newBuilder()
    .maximumSize(10000)
    .expireAfterWrite(10, TimeUnit.SECONDS)
    .build(
      new CacheLoader<>() {
        @Override
        public HashArgument load(String key) {
          return null;
        }
      }
    );

  int timeout = 10;

  public HashProcess(int timeout) {
    this.timeout = timeout;
  }

  void newArgument(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    // Filter out this node before joining the argument and giving a reason.
    hashArguments.put(hashMeta.getHashPrefix(), new HashArgument(node.getUniqueId()));
    runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.CLAIM, new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas())));
  }

  double CPUPercentageThreshold = 0.7;

  boolean cpuThreshold(HashArgumentResponseTrack track) {
    ReasonResponse.CPUPercent[] cpuParts = track.getReasonResponse().getCpuPercent();
    int x = 0;
    for (int i = 0; i < cpuParts.length; i++)
      if (cpuParts[i].combined <= CPUPercentageThreshold) x++;
    return x > 0;
  }

  long minimumFreeRam = 25 * 1024;

  boolean memoryThreshold(HashArgumentResponseTrack track) {
    return track.getReasonResponse().getMemory().actualFree >= minimumFreeRam;
  }

  double calculateDistanceToZero(double... positions) {
    return Math.sqrt(Arrays.stream(positions).map(a -> Math.pow(a, 2)).sum());
  }

  // weights for sorting
  double hitWeight = 0.25, cpuWeight = 0.45, memoryWeight = 0.75, cpuLoad1Weight = 0.90, cpuLoad5Weight = 0.35;


  private ConsensusResponse calculateConsensus(Map<String, HashArgumentResponseTrack> nodeResponses) {
    List<Map.Entry<String, HashArgumentResponseTrack>> validOptions = new ArrayList<>();

    // calculate max values for resources.
    long maxHits = 0;
    for (Map.Entry<String, HashArgumentResponseTrack> nodeResponseStream : nodeResponses.entrySet()) {
      if (cpuThreshold(nodeResponseStream.getValue()) && memoryThreshold(nodeResponseStream.getValue())) {
        validOptions.add(nodeResponseStream);
      }
      if (maxHits < nodeResponseStream.getValue().getReasonResponse().getHits())
        maxHits = nodeResponseStream.getValue().getReasonResponse().getHits();
    }
    // sort based on weighted free resource on each node
    long finalMaxHits = maxHits;
    List<NodeSortWeight> sortedWithWeights = validOptions.stream().map(trackEntry -> {
      HashArgumentResponseTrack track = trackEntry.getValue();
      NodeSortWeight nodeSortWeight = new NodeSortWeight(trackEntry.getKey());

      // get remaining slots. (may need to adjust to account for importance)
      double hits = 0;
      if (finalMaxHits != 0) {
        hits = Math.cos(((double) track.getReasonResponse().getHits()) / ((double) finalMaxHits));
        System.out.println("hits " + hits);
      }
      double cpuAverage = Arrays.stream(track.getReasonResponse().getCpuPercent()).map(c -> c.idle / (c.combined + c.idle)).reduce((a, b) -> a + b).get() / track.getReasonResponse().getCpuPercent().length;
      System.out.println("cpuAverage " + cpuAverage);
      double memoryAverage = (double) track.getReasonResponse().getMemory().actualFree / (double) track.getReasonResponse().getMemory().total;
      System.out.println("memoryAverage " + memoryAverage);
      double cpuLoad1Minute = track.getReasonResponse().getLoad()[0] / track.getReasonResponse().getCpuPercent().length;
      System.out.println("cpuLoad1Minute " + cpuLoad1Minute);
      double cpuLoad5Minute = track.getReasonResponse().getLoad()[1] / track.getReasonResponse().getCpuPercent().length;
      System.out.println("cpuLoad5Minute " + cpuLoad5Minute);

      // now get the distance of free resources from zero. using pythagorean theorem.


      nodeSortWeight.weight = Double.valueOf(Math.ceil(calculateDistanceToZero(
        hits * hitWeight,
        cpuAverage * cpuWeight,
        memoryAverage * memoryWeight,
        cpuLoad1Minute * cpuLoad1Weight,
        cpuLoad5Minute * cpuLoad5Weight
      ) * 10000000)).intValue();
      System.out.println(nodeSortWeight.weight);

      return nodeSortWeight;
    }).sorted((a, b) -> Long.valueOf(a.weight - b.weight).intValue()).collect(Collectors.toList());
    //now use the sorted list of arguments
    return new ConsensusResponse(sortedWithWeights.stream().map(a -> a.node).collect(Collectors.toSet()), sortedWithWeights);
  }

  class NodeVote implements Serializable {
    String node;
    Integer votes = 0;

    public NodeVote(String node) {
      this.node = node;
    }
  }

  private Map<String, NodeVote> calculateVotes(Map<String, HashArgumentResponseTrack> responses, int replicas) {
    Map<String, NodeVote> votes = new HashedMap<>();
    for (Map.Entry<String, HashArgumentResponseTrack> e : responses.entrySet()) {
      e.getValue().getConsensusResponse().getNodes().stream().limit(replicas).forEach(node -> {
        if (!votes.containsKey(node)) {
          votes.put(node, new NodeVote(node));
        }
        votes.get(node).votes++;
      });
    }
    return votes;
  }

  void claimReceived(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if(hashArgument!=null) {
      hashArgument.addNode(hashMeta.getNode());
      if (hashMeta.getNode().equals(node.getUniqueId())) {
        // get this nodes data
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas());
        meta.setNodeStatus(node);
        runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.REASON, meta));
      }
    }
  }

  void reasonReceived(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if (hashArgument != null) {
      HashArgumentResponseTrack track = hashArgument.getResponses().get(hashMeta.getNode());
      if (track != null) {
        track.setReasonResponse(hashMeta.getReason());
      }
    }
    if (hashArgument != null && hashArgument.getExecutor() == null) {
      ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
      HashArgument finalHashArgument = hashArgument;
      hashArgument.setExecutor(executorService.schedule(() -> {
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas());
        meta.getObjects().put("consensus", calculateConsensus(finalHashArgument.getResponses()));
        finalHashArgument.setExecutor(null);
        runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.CONSENSUS, meta));
      }, timeout, TimeUnit.MILLISECONDS));
    } else if (hashArgument != null && hashArgument.getExecutor() != null && hashArgument.getResponses().size()==hashArgument.getResponses().values().stream().filter(c->c.getReasonResponse()!=null).count()) {
      hashArgument.getExecutor().cancel(false);
      if (hashArgument.getExecutor().isCancelled()) {
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas());
        meta.getObjects().put("consensus", calculateConsensus(hashArgument.getResponses()));
        hashArgument.setExecutor(null);
        runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.CONSENSUS, meta));
      }
    }

  }

  void consensusReceived(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    // handle consensus
    System.out.println("test1");
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if (hashArgument != null) {
      System.out.println("test2");
      HashArgumentResponseTrack track = hashArgument.getResponses().get(hashMeta.getNode());
      ConsensusResponse consensusResponse = hashMeta.getConsensus();

      if (consensusResponse != null && track != null) {
        System.out.println("test3");
        track.setConsensusResponse(consensusResponse);
      }
    }
    System.out.println("test4");
    if (hashArgument != null && hashArgument.getExecutor() == null) {
      System.out.println("test5");
      ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
      HashArgument finalHashArgument = hashArgument;
      hashArgument.setExecutor(executorService.schedule(() -> {
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas());
        System.out.println("test6 timeout");
        meta.getObjects().put("vote_result", calculateVotes(finalHashArgument.getResponses(), hashMeta.getReplicas()));
        finalHashArgument.setExecutor(null);
        runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.ACTION, meta));
      }, timeout, TimeUnit.MILLISECONDS));

    } else if (hashArgument != null && hashArgument.getExecutor() != null  && hashArgument.getResponses().size()==hashArgument.getResponses().values().stream().filter(c->c.getConsensusResponse()!=null).count()) {
      hashArgument.getExecutor().cancel(false);
      if(hashArgument.getExecutor().isCancelled()){
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas());
        System.out.println("test6 early");
        meta.getObjects().put("vote_result", calculateVotes(hashArgument.getResponses(), hashMeta.getReplicas()));
        hashArgument.setExecutor(null);
        runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.ACTION, meta));
      }
    }
  }


  class VoteResultComparison implements Serializable {
    public int count = 0;
    public String hash;
    public List<String> nodes;

    public VoteResultComparison(String hash, List<String> nodes, int count) {
      this.count = count;
      this.hash = hash;
      this.nodes = nodes;
    }
  }

  void actionReceived(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if (hashArgument != null) {
      HashArgumentResponseTrack track = hashArgument.getResponses().get(hashMeta.getNode());
      List<String> voteResponse = hashMeta.getVoteResult();
      if (voteResponse != null && track != null) {
        track.setNodes(voteResponse);
      }
    }
    if (hashArgument != null && hashArgument.getExecutor() == null) {
      ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
      HashArgument finalHashArgument = hashArgument;
      hashArgument.setExecutor(executorService.schedule(() -> {
        System.out.println("executing after timeout");
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas());
        finalHashArgument.setExecutor(null);
        Map<String, VoteResultComparison> voteResultComparisonMap = Maps.newHashMap();
        for (Map.Entry<String, HashArgumentResponseTrack> track : finalHashArgument.getResponses().entrySet()) {
          String key = track.getValue().getNodes().stream().collect(Collectors.joining("-"));
          System.out.println("Key " + key);
          if (voteResultComparisonMap.containsKey(key)) {
            voteResultComparisonMap.get(key).count++;
          } else {
            voteResultComparisonMap.put(key, new VoteResultComparison(meta.getHashPrefix(), track.getValue().getNodes(), 1));
          }
        }
        System.out.println(voteResultComparisonMap.keySet().stream().collect(Collectors.joining()));
        Optional<VoteResultComparison> voteResultComparisonTrack = voteResultComparisonMap.values().stream().sorted(Comparator.comparingInt(a -> a.count)).limit(1).findFirst();
        if (voteResultComparisonTrack.isPresent()) {
          VoteResultComparison voteResultComparison = voteResultComparisonTrack.get();
          if (voteResultComparison.nodes.contains(node.getUniqueId())) {
            runner.callback(ArgumentAction.RUN_FINAL_ACTION, new ArgumentResult(voteResultComparison));
          }
        }
      }, timeout, TimeUnit.MILLISECONDS));
    } else if (hashArgument != null && hashArgument.getExecutor() != null && hashArgument.getResponses().size()==hashArgument.getResponses().values().stream().filter(c->!c.getNodes().isEmpty()).count()) {
      System.out.println("executing early");
      hashArgument.getExecutor().cancel(false);
      if (hashArgument.getExecutor().isCancelled()) {
        System.out.println("Action Received Executing");
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas());
        hashArgument.setExecutor(null);
        Map<String, VoteResultComparison> voteResultComparisonMap = Maps.newHashMap();
        for (Map.Entry<String, HashArgumentResponseTrack> track : hashArgument.getResponses().entrySet()) {
          String key = track.getValue().getNodes().stream().collect(Collectors.joining("-"));
          System.out.println("Key " + key);
          if (voteResultComparisonMap.containsKey(key)) {
            voteResultComparisonMap.get(key).count++;
          } else {
            voteResultComparisonMap.put(key, new VoteResultComparison(meta.getHashPrefix(), track.getValue().getNodes(), 1));
          }
        }
        System.out.println(voteResultComparisonMap.keySet().stream().collect(Collectors.joining()));
        Optional<VoteResultComparison> voteResultComparisonTrack = voteResultComparisonMap.values().stream().sorted(Comparator.comparingInt(a -> a.count)).limit(1).findFirst();
        if (voteResultComparisonTrack.isPresent()) {
          VoteResultComparison voteResultComparison = voteResultComparisonTrack.get();
          if (voteResultComparison.nodes.contains(node.getUniqueId())) {
            runner.callback(ArgumentAction.RUN_FINAL_ACTION, new ArgumentResult(voteResultComparison));
          }
        }
      }
    }
  }

  @Override
  public void action(ArgumentResult argumentResult) {
    Object obj = argumentResult.getResultObject();
    if (obj != null && obj instanceof VoteResultComparison) {
      VoteResultComparison voteResultComparison = (VoteResultComparison) obj;
      System.out.println("I WON THE ELECTION " + voteResultComparison.hash);
    }
  }

  @Override
  public void process(NucleoDBNode node, ArgumentStep argumentType, ArgumentMessageData argumentMessageData, ArgumentCallback<Object> runner) {
    if (argumentMessageData instanceof HashMeta) {
      HashMeta hashMeta = (HashMeta) argumentMessageData;
      switch (argumentType) {
        case NEW:
          newArgument(node, hashMeta, runner);
          break;
        case CLAIM:
          claimReceived(node, hashMeta, runner);
          break;
        case REASON:
          reasonReceived(node, hashMeta, runner);
          break;
        case CONSENSUS:
          consensusReceived(node, hashMeta, runner);
          break;
        case ACTION:
          actionReceived(node, hashMeta, runner);
          break;
      }
    }
  }
}
