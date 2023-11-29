package com.nucleocore.library.negotiator.decision.hash;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.nucleocore.library.NucleoDBNode;
import com.nucleocore.library.negotiator.decision.hash.responses.ConsensusResponse;
import com.nucleocore.library.negotiator.decision.hash.responses.HashArgumentResponseTrack;
import com.nucleocore.library.negotiator.decision.hash.responses.NodeSortWeight;
import com.nucleocore.library.negotiator.decision.hash.responses.ReasonResponse;
import com.nucleocore.library.negotiator.decision.support.ArgumentAction;
import com.nucleocore.library.negotiator.decision.support.ArgumentCallback;
import com.nucleocore.library.negotiator.decision.support.ArgumentErrorResponse;
import com.nucleocore.library.negotiator.decision.support.ArgumentErrorType;
import com.nucleocore.library.negotiator.decision.support.ArgumentKafkaMessage;
import com.nucleocore.library.negotiator.decision.support.ArgumentMessageData;
import com.nucleocore.library.negotiator.decision.support.ArgumentProcess;
import com.nucleocore.library.negotiator.decision.support.ArgumentResult;
import com.nucleocore.library.negotiator.decision.support.ArgumentStep;
import org.apache.commons.collections4.map.HashedMap;

import java.awt.*;
import java.io.Serializable;
import java.util.*;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HashProcess implements ArgumentProcess, Serializable {

  static Set<String> handledHash = new HashSet<>();

  static Set<String> testHash = new HashSet<>();

  static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(200);
  static LoadingCache<String, HashArgument> hashArguments = CacheBuilder.newBuilder()
    .maximumSize(100)
    .expireAfterWrite(300, TimeUnit.MILLISECONDS)
    .build(
      new CacheLoader<>() {
        @Override
        public HashArgument load(String key) {
          return null;
        }
      }
    );
  static

  int timeout = 10;

  public HashProcess(int timeout) {
    this.timeout = timeout;
  }

  void newArgument(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    // Filter out this node before joining the argument and giving a reason.
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if ((hashArgument == null || hashArgument.getRetry()<hashMeta.getRetry())  && !handledHash.contains(hashMeta.getHashPrefix())) {
      if ((hashArgument!=null && hashArgument.getRetry()<hashMeta.getRetry()) || hashMeta.getRetry()>0) {
        System.out.println(toColoredStr(
          "retrying " + hashMeta.getHashPrefix(),
          getIntFromColor(Color.decode("#"+hashMeta.getHashPrefix()))
        ));
        testHash.add(hashMeta.getHashPrefix());
      }
      if(hashArgument!=null){
        hashArgument.setRetry(9999);
        hashArgument.getNodeExecutor().cancel(true);
        hashArgument.getReasonExecutor().cancel(true);
        hashArgument.getConsensusExector().cancel(true);
      }
      hashArguments.put(hashMeta.getHashPrefix(), new HashArgument(hashMeta.getHashPrefix(), hashMeta.getRetry()));
      runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.CLAIM, new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry())));
    }
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
        //System.out.println("hits " + hits);
      }
      double cpuAverage = Arrays.stream(track.getReasonResponse().getCpuPercent()).map(c -> c.idle / (c.combined + c.idle)).reduce((a, b) -> a + b).get() / track.getReasonResponse().getCpuPercent().length;
      //System.out.println("cpuAverage " + cpuAverage);
      double memoryAverage = (double) track.getReasonResponse().getMemory().actualFree / (double) track.getReasonResponse().getMemory().total;
      //System.out.println("memoryAverage " + memoryAverage);
      double cpuLoad1Minute = track.getReasonResponse().getLoad()[0] / track.getReasonResponse().getCpuPercent().length;
      //System.out.println("cpuLoad1Minute " + cpuLoad1Minute);
      double cpuLoad5Minute = track.getReasonResponse().getLoad()[1] / track.getReasonResponse().getCpuPercent().length;
      //System.out.println("cpuLoad5Minute " + cpuLoad5Minute);

      // now get the distance of free resources from zero. using pythagorean theorem.


      nodeSortWeight.weight = Double.valueOf(Math.ceil(calculateDistanceToZero(
        hits * hitWeight,
        cpuAverage * cpuWeight,
        memoryAverage * memoryWeight,
        cpuLoad1Minute * cpuLoad1Weight,
        cpuLoad5Minute * cpuLoad5Weight
      ) * 10000000)).intValue();
      //System.out.println(nodeSortWeight.weight);

      return nodeSortWeight;
    }).sorted((a, b) -> Long.valueOf(b.weight - a.weight).intValue()).collect(Collectors.toList());
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
    if (hashArgument != null) {
      if(hashArgument.getRetry() != hashMeta.getRetry())
        return;
      hashArgument.addNode(hashMeta.getNode());
      if (hashMeta.getNode().equals(node.getUniqueId())) { // respond only if this server.
        // get this nodes data
        HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry());
        meta.setNodeStatus(node);
        runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.REASON, meta));
      }
    }
  }

  void reasonReceived(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if (hashArgument == null)
      return;

    if(hashArgument.getRetry() != hashMeta.getRetry())
      return;

    HashArgumentResponseTrack nodeTrack = hashArgument.getResponses().get(hashMeta.getNode());
    ReasonResponse reasonResponse = hashMeta.getReason();

    if (nodeTrack != null && nodeTrack.getReasonResponse() != null) {
      //error(node, hashMeta, runner, "Already sent reason for this node : "+hashMeta.getNode());
      return;
    }

    if (nodeTrack != null && reasonResponse != null)
      nodeTrack.setReasonResponse(reasonResponse);

    synchronized (hashArgument) {
      if (hashArgument.getReasonExecuted().get())
        return;

      if (hashArgument != null && hashArgument.getReasonExecutor() == null) {
        HashArgument finalHashArgument = hashArgument;
        hashArgument.setReasonExecutor(executorService.schedule(() -> {
          hashArgument.getReasonExecuted().set(true);
          HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry());
          meta.getObjects().put("consensus", calculateConsensus(finalHashArgument.getResponses()));
          runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.CONSENSUS, meta));
        }, timeout, TimeUnit.MILLISECONDS));
      } else if (hashArgument != null && hashArgument.getReasonExecutor() != null && hashArgument.getResponses().values().stream().filter(c -> c.getReasonResponse() != null).count() == hashArgument.getResponses().size()) {
        hashArgument.getReasonExecuted().set(true);
        hashArgument.getReasonExecutor().cancel(false);
        if (hashArgument.getReasonExecutor().isCancelled()) {
          HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry());
          meta.getObjects().put("consensus", calculateConsensus(hashArgument.getResponses()));
          runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.CONSENSUS, meta));
        }else{
          /*System.out.println(toColoredStr(
            hashMeta.getHashPrefix() + ": Error in reason.",
            getIntFromColor(Color.decode("#dd0040"))
          ));*/
        }
      }
    }

  }

  void consensusReceived(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    // handle consensus
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if (hashArgument == null)
      return;

    if(hashArgument.getRetry() != hashMeta.getRetry())
      return;

    HashArgumentResponseTrack nodeTrack = hashArgument.getResponses().get(hashMeta.getNode());
    ConsensusResponse consensusResponse = hashMeta.getConsensus();

    if (nodeTrack != null && nodeTrack.getConsensusResponse() != null) {
      //error(node, hashMeta, runner, "Already sent consensus from this node : "+hashMeta.getNode());
      return;
    }

    if (consensusResponse != null && nodeTrack != null)
      nodeTrack.setConsensusResponse(consensusResponse);

    synchronized (hashArgument) {
      if (hashArgument.getConsensusExecuted().get())
        return;

      if (hashArgument != null && hashArgument.getConsensusExector() == null) {
        HashArgument finalHashArgument = hashArgument;
        hashArgument.setConsensusExector(executorService.schedule(() -> {
          hashArgument.getConsensusExecuted().set(true);
          HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry());
          meta.getObjects().put("vote_result", calculateVotes(finalHashArgument.getResponses(), hashMeta.getReplicas()));
          runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.ACTION, meta));
        }, timeout, TimeUnit.MILLISECONDS));
      } else if (hashArgument != null && hashArgument.getConsensusExector() != null && hashArgument.getResponses().values().parallelStream().filter(c -> c.getConsensusResponse() != null).count() == hashArgument.getResponses().values().parallelStream().filter(c -> c.getReasonResponse() != null).count()) {
        hashArgument.getConsensusExecuted().set(true);
        hashArgument.getConsensusExector().cancel(false);
        if (hashArgument.getConsensusExector().isCancelled()) {
          HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry());
          meta.getObjects().put("vote_result", calculateVotes(hashArgument.getResponses(), hashMeta.getReplicas()));
          runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.ACTION, meta));
        }
      }else{
        /*System.out.println(toColoredStr(
          hashMeta.getHashPrefix() + ": Error in consensus.",
          getIntFromColor(Color.decode("#dd0040"))
        ));*/
      }
    }
  }

  void error(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner, ArgumentErrorResponse argumentErrorResponse) {
    HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry());
    meta.getObjects().put("error", argumentErrorResponse);
    runner.callback(ArgumentAction.SEND_TO_TOPIC, new ArgumentKafkaMessage(ArgumentStep.ERROR, meta));
  }


  class VoteResultComparison implements Serializable {
    public int count = 0;
    public String hash;
    public Set<String> nodes;

    public VoteResultComparison(String hash, Set<String> nodes, int count) {
      this.count = count;
      this.hash = hash;
      this.nodes = nodes;
    }
  }

  void actionExecute(NucleoDBNode node, HashMeta hashMeta, HashArgument hashArgument, ArgumentCallback<Object> runner) {
    hashArguments.invalidate(hashArgument.getHashPrefix());
    hashArgument.getNodeExecuted().set(true);
    //System.out.println("executing after timeout");
    HashMeta meta = new HashMeta(node.getUniqueId(), hashMeta.getHashPrefix(), hashMeta.getReplicas(), hashMeta.getRetry());
    Map<String, VoteResultComparison> voteResultComparisonMap = Maps.newHashMap();
    for (Map.Entry<String, HashArgumentResponseTrack> track : hashArgument.getResponses().entrySet()) {
      String key = track.getValue().getNodes().stream().collect(Collectors.joining("-"));
      //System.out.println("Key " + key);
      if (voteResultComparisonMap.containsKey(key)) {
        voteResultComparisonMap.get(key).count++;
      } else {
        voteResultComparisonMap.put(key, new VoteResultComparison(meta.getHashPrefix(), track.getValue().getNodes(), 1));
      }
    }
    //System.out.println(hashMeta.getHashPrefix()+": list = "+voteResultComparisonMap.keySet().stream().collect(Collectors.joining()));
    Optional<VoteResultComparison> voteResultComparisonTrack = voteResultComparisonMap.values().stream().sorted((a, b) -> b.count - a.count).findFirst();
    if (voteResultComparisonTrack.isPresent()) {

      VoteResultComparison voteResultComparison = voteResultComparisonTrack.get();
      int replicasLeft = hashMeta.getReplicas() - voteResultComparison.nodes.size();

      if (replicasLeft != 0) {
        ArgumentErrorResponse argumentError = new ArgumentErrorResponse("Error, not all replica assignments met. #" + replicasLeft, ArgumentErrorType.REPLICA_UNFULFILLED);
        argumentError.getObjects().put("replicas", replicasLeft);
        error(node, hashMeta, runner, argumentError);
      }

      if (voteResultComparison.nodes.contains(node.getUniqueId())) {
        runner.callback(ArgumentAction.RUN_FINAL_ACTION, new ArgumentResult(voteResultComparison));
      }

    } else {
      ArgumentErrorResponse argumentError = new ArgumentErrorResponse("Error, no available nodes: " + hashMeta.getNode(), ArgumentErrorType.NO_NODES_AVAILABLE);
      argumentError.getObjects().put("replicas", hashMeta.getReplicas());
      error(node, hashMeta, runner, argumentError);
    }
  }

  void actionReceived(NucleoDBNode node, HashMeta hashMeta, ArgumentCallback<Object> runner) {
    HashArgument hashArgument = hashArguments.getIfPresent(hashMeta.getHashPrefix());
    if (hashArgument == null)
      return;

    if(hashArgument.getRetry() != hashMeta.getRetry())
      return;

    HashArgumentResponseTrack nodeTrack = hashArgument.getResponses().get(hashMeta.getNode());
    Set<String> voteResponse = hashMeta.getVoteResult();

    if (nodeTrack != null && nodeTrack.getNodes() != null) {
      if (nodeTrack.getNodes().isEmpty()) {
        //error(node, hashMeta, runner, "No node votes sent from: "+hashMeta.getNode());
        return;
      }
      //error(node, hashMeta, runner, "Error, already set node list: "+hashMeta.getNode());
      return;
    }

    if (voteResponse != null && nodeTrack != null) {
      nodeTrack.setNodes(voteResponse);
    }
    synchronized (hashArgument) {
      if (hashArgument.getNodeExecuted().get())
        return;

      if (hashArgument != null && hashArgument.getNodeExecutor() == null) {
        HashArgument finalHashArgument = hashArgument;
        hashArgument.setNodeExecutor(executorService.schedule(() -> {
          actionExecute(node, hashMeta, finalHashArgument, runner);
        }, timeout, TimeUnit.MILLISECONDS));
      } else if (hashArgument != null && hashArgument.getNodeExecutor() != null && hashArgument.getResponses().values().stream().filter(c -> c.getNodes() != null).count() == hashArgument.getResponses().values().stream().filter(c -> c.getConsensusResponse() != null).count()) {
        hashArgument.getNodeExecuted().set(true);
        hashArgument.getNodeExecutor().cancel(false);
        if (hashArgument.getNodeExecutor().isCancelled()) {
          actionExecute(node, hashMeta, hashArgument, runner);
        }
      }else{
        /*System.out.println(toColoredStr(
          hashMeta.getHashPrefix() + ": Error in action.",
          getIntFromColor(Color.decode("#dd0040"))
        ));*/
      }
    }
  }

  private static final String ESC_STR = "\u001B";
  private static final String RESET_ESC_SEQUENCE_STR = "\u001B[0m";

  private String toEscSequenceStr(int color) {
    int foreground = 30;
    foreground += color & 0x07;
    color >>= 3;
    foreground += 60 * (color & 0x01);
    color >>= 1;

    int background = 40;
    background += color & 0x07;
    color >>= 3;
    background += 60 * (color & 0x01);

    return String.format("%s[%d;%dm", ESC_STR, foreground, background);
  }

  private String toColoredStr(String text, int color) {
    return String.format("%s%s%s", toEscSequenceStr(color), text, RESET_ESC_SEQUENCE_STR);
  }

  public int getIntFromColor(Color c) {
    int R = Math.round(255 * c.getRed());
    int G = Math.round(255 * c.getGreen());
    int B = Math.round(255 * c.getBlue());

    R = (R << 16) & 0x00FF0000;
    G = (G << 8) & 0x0000FF00;
    B = B & 0x000000FF;

    return 0xFF000000 | R | G | B;
  }

  @Override
  public void action(NucleoDBNode node, ArgumentResult argumentResult) {
    Object obj = argumentResult.getResultObject();
    if (obj != null && obj instanceof VoteResultComparison) {
      VoteResultComparison voteResultComparison = (VoteResultComparison) obj;
      if (testHash.contains(voteResultComparison.hash)) {
        System.out.println(toColoredStr(
          "Fulfilled: " + voteResultComparison.hash,
          getIntFromColor(Color.decode("#"+voteResultComparison.hash))
        ));
      }
      handledHash.add(voteResultComparison.hash);
      System.out.println(
        toColoredStr(
          "I WON THE ELECTION " + voteResultComparison.hash,
          getIntFromColor(Color.decode("#"+voteResultComparison.hash))
        )
      );
      node.insertTempAdjustment();
    }
  }

  static ScheduledExecutorService retryThreads = Executors.newScheduledThreadPool(500);

  @Override
  public void process(NucleoDBNode node, ArgumentStep argumentType, ArgumentMessageData argumentMessageData, ArgumentCallback<Object> runner) {
    if (argumentMessageData instanceof HashMeta) {

      HashMeta hashMeta = (HashMeta) argumentMessageData;
      //System.out.println(hashMeta.getHashPrefix()+": "+argumentType);
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
        case ERROR:
          ArgumentErrorResponse error = hashMeta.get("error");
          switch (error.getArgumentError()) {
            case NO_NODES_AVAILABLE:
            case REPLICA_UNFULFILLED:
              retryThreads.schedule(() -> {
                runner.callback(
                  ArgumentAction.SEND_TO_TOPIC,
                  new ArgumentKafkaMessage(
                    ArgumentStep.NEW,
                    new HashMeta(
                      node.getUniqueId(),
                      hashMeta.getHashPrefix(),
                      error.get("replicas"),
                      hashMeta.getRetry()+1
                    )
                  )
                );
              }, 1500*(hashMeta.getRetry()+1), TimeUnit.MILLISECONDS); // retry this
              /*System.out.println(toColoredStr(
                hashMeta.getHashPrefix() + ": " + error.getMessage(),
                getIntFromColor(Color.decode("#dd0040"))
              ));*/
              break;
          }

          break;
      }
    }
  }
}
