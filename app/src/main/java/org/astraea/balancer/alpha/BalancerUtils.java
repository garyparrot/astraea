package org.astraea.balancer.alpha;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.Utils;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.cost.HasPartitionCost;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionCost;
import org.astraea.cost.PartitionInfo;
import org.astraea.cost.TopicPartition;
import org.astraea.metrics.HasBeanObject;
import org.astraea.topic.Replica;
import org.astraea.topic.TopicAdmin;

public class BalancerUtils {

  public static ClusterLogAllocation currentAllocation(
      TopicAdmin topicAdmin, ClusterInfo clusterInfo) {
    return new ClusterLogAllocation(
        topicAdmin.replicas(clusterInfo.topics()).entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .map(Replica::broker)
                            .collect(Collectors.toUnmodifiableList())))
            .collect(Collectors.groupingBy(entry -> entry.getKey().topic()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    x ->
                        x.getValue().stream()
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    y -> y.getKey().partition(), Map.Entry::getValue)))));
  }

  /** create a fake cluster info based on given proposal */
  public static ClusterInfo clusterInfoFromProposal(
      ClusterInfo clusterInfo, RebalancePlanProposal proposal) {
    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return clusterInfo.nodes();
      }

      @Override
      public List<PartitionInfo> availablePartitions(String topic) {
        return partitions(topic).stream()
            .filter(x -> x.leader() != null)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Set<String> topics() {
        return proposal
            .rebalancePlan()
            .map(clusterLogAllocation -> clusterLogAllocation.allocation().keySet())
            .orElseGet(clusterInfo::topics);
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        return proposal
            .rebalancePlan()
            .map(
                clusterLogAllocation ->
                    clusterLogAllocation.allocation().get(topic).entrySet().stream()
                        .map(
                            entry -> {
                              var collect =
                                  entry.getValue().stream()
                                      .map(x -> NodeInfo.of(x, "", 0))
                                      .collect(Collectors.toUnmodifiableList());
                              return PartitionInfo.of(
                                  topic, entry.getKey(), collect.get(0), collect, null, null);
                            })
                        .collect(Collectors.toUnmodifiableList()))
            .orElse(clusterInfo.partitions(topic));
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return clusterInfo.beans(brokerId);
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return clusterInfo.allBeans();
      }
    };
  }

  public static void printTopicPartitionReplicaCost(
      Map<?, Map<TopicPartitionReplica, Double>> tprScores) {
    tprScores.forEach(
        (key, value) -> {
          System.out.printf("[%s]%n", key.getClass().getSimpleName());
          value.entrySet().stream()
              .sorted(
                  Comparator.comparing(
                          (Map.Entry<TopicPartitionReplica, Double> x) -> x.getKey().topic())
                      .thenComparing(
                          (Map.Entry<TopicPartitionReplica, Double> x) -> x.getKey().partition())
                      .thenComparing(
                          (Map.Entry<TopicPartitionReplica, Double> x) -> x.getKey().brokerId()))
              .forEachOrdered(
                  entry ->
                      System.out.printf(
                          " TPR %s: %f%n", entry.getKey().toString(), entry.getValue()));
          System.out.println();
        });
  }

  public static void printBrokerCost(Map<HasBrokerCost, BrokerCost> brokerCost) {
    brokerCost.forEach(
        (costFunction, cost) -> {
          System.out.println("[" + costFunction.getClass().getSimpleName() + "]");
          cost.value()
              .forEach(
                  (brokerId, score) -> {
                    System.out.printf(" broker # %d : %f%n", brokerId, score);
                  });
        });
  }

  public static void printPartitionCost(
      Map<HasPartitionCost, PartitionCost> partitionCost, List<NodeInfo> nodeInfo) {
    partitionCost.forEach(
        (costFunction, cost) -> {
          System.out.println("[" + costFunction.getClass().getSimpleName() + "]");
          nodeInfo.forEach(
              node -> {
                var sortedKey =
                    cost.value(node.id()).keySet().stream()
                        .sorted(
                            Comparator.comparing(TopicPartition::topic)
                                .thenComparing(TopicPartition::partition))
                        .collect(Collectors.toList());
                System.out.println(" broker # " + node.id());
                sortedKey.forEach(
                    key ->
                        System.out.println(
                            key.topic()
                                + "-"
                                + key.partition()
                                + ": "
                                + cost.value(node.id()).get(key)));
              });
        });
  }

  public static void describeProposal(
      RebalancePlanProposal proposal, ClusterLogAllocation currentAllocation) {
    if (proposal.isPlanGenerated()) {
      System.out.printf("[New Rebalance Plan Generated %s]%n", LocalDateTime.now());

      final var balanceAllocation = proposal.rebalancePlan().orElseThrow();
      if (balanceAllocation.allocation().size() > 0) {
        balanceAllocation
            .allocation()
            .forEach(
                (topic, partitionMap) -> {
                  System.out.printf(" Topic \"%s\":%n", topic);
                  partitionMap.forEach(
                      (partitionId, replicaAllocation) -> {
                        final var originalState =
                            currentAllocation.allocation().get(topic).get(partitionId);
                        final var finalState =
                            balanceAllocation.allocation().get(topic).get(partitionId);

                        final var noChange =
                            originalState.stream()
                                .filter(finalState::contains)
                                .sorted()
                                .collect(Collectors.toUnmodifiableList());
                        final var toDelete =
                            originalState.stream()
                                .filter(id -> !finalState.contains(id))
                                .sorted()
                                .collect(Collectors.toUnmodifiableList());
                        final var toReplicate =
                            finalState.stream()
                                .filter(id -> !originalState.contains(id))
                                .sorted()
                                .collect(Collectors.toUnmodifiableList());

                        boolean noChangeAtAll = toDelete.size() == 0 && toReplicate.size() == 0;
                        if (!noChangeAtAll) {
                          System.out.printf("   Partition #%d%n", partitionId);
                          System.out.println("       no change: " + noChange);
                          System.out.println("       to delete: " + toDelete);
                          System.out.println("       to replicate: " + toReplicate);
                        }
                      });
                });
      } else {
        System.out.println(" No topic in the cluster.");
      }
    } else {
      System.out.printf("[No Rebalance Plan Generated %s]\n", LocalDateTime.now());
    }
    System.out.println();

    // print info, warnings, exceptions
    System.out.println("[Information]");
    proposal.info().forEach(info -> System.out.printf(" * %s%n", info));
    System.out.println((proposal.info().size() == 0 ? " No Information Given.\n" : "\n"));
    System.out.println("[Warnings]");
    proposal.warnings().forEach(warning -> System.out.printf(" * %s%n", warning));
    System.out.println((proposal.warnings().size() == 0 ? " No Warning.\n" : "\n"));

    IntStream.range(0, proposal.exceptions().size())
        .forEachOrdered(
            index -> {
              System.out.printf("[Exception %d/%d]%n", index + 1, proposal.exceptions().size());
              proposal.exceptions().get(index).printStackTrace();
            });
    System.out.println();
  }

  public static Map<TopicPartition, List<Integer>> diffAllocation(
      ClusterLogAllocation proposal, ClusterLogAllocation currentAllocation) {
    final var balanceAllocation = proposal;
    if (balanceAllocation.allocation().size() > 0) {
      final var diff = new HashMap<TopicPartition, List<Integer>>();

      balanceAllocation
          .allocation()
          .forEach(
              (topic, partitionMap) -> {
                partitionMap.forEach(
                    (partitionId, replicaAllocation) -> {
                      final var originalState =
                          currentAllocation.allocation().get(topic).get(partitionId);
                      final var finalState =
                          balanceAllocation.allocation().get(topic).get(partitionId);

                      final var toReplicate =
                          finalState.stream()
                              .filter(id -> !originalState.contains(id))
                              .sorted()
                              .collect(Collectors.toUnmodifiableList());

                      diff.put(TopicPartition.of(topic, partitionId), toReplicate);
                    });
              });

      return diff;
    } else {
      return Map.of();
    }
  }

  public static ClusterInfo clusterSnapShot(TopicAdmin topicAdmin) {
    return clusterSnapShot(topicAdmin, Set.of());
  }

  public static ClusterInfo clusterSnapShot(TopicAdmin topicAdmin, Set<String> topicToIgnore) {
    final var nodeInfo =
        Utils.handleException(() -> topicAdmin.adminClient().describeCluster().nodes().get())
            .stream()
            .map(NodeInfo::of)
            .collect(Collectors.toUnmodifiableList());
    final var nodeInfoMap =
        nodeInfo.stream().collect(Collectors.toUnmodifiableMap(NodeInfo::id, Function.identity()));
    final var topics =
        topicAdmin.topicNames().stream()
            .filter(topic -> !topicToIgnore.contains(topic))
            .collect(Collectors.toUnmodifiableSet());
    final var partitionInfo =
        topicAdmin.replicas(topics).entrySet().stream()
            .map(
                entry -> {
                  var leaderReplica =
                      entry.getValue().stream().filter(Replica::leader).findFirst().orElseThrow();
                  var leaderNode = nodeInfoMap.get(leaderReplica.broker());
                  var allNodes =
                      entry.getValue().stream()
                          .map(x -> nodeInfoMap.get(x.broker()))
                          .collect(Collectors.toUnmodifiableList());
                  // TODO: fix null
                  return PartitionInfo.of(
                      entry.getKey().topic(),
                      entry.getKey().partition(),
                      leaderNode,
                      allNodes,
                      null,
                      null);
                })
            .collect(Collectors.toUnmodifiableList());

    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return nodeInfo;
      }

      @Override
      public List<PartitionInfo> availablePartitions(String topic) {
        return partitions(topic).stream()
            .filter(x -> x.leader() != null)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Set<String> topics() {
        return topics;
      }

      @Override
      public List<PartitionInfo> partitions(String topic) {
        return partitionInfo.stream()
            .filter(x -> x.topic().equals(topic))
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return allBeans().get(brokerId);
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return Map.of();
      }
    };
  }

  public static Runnable generationWatcher(int totalIteration, AtomicInteger finishedIteration) {
    return () -> {
      var fancyIndicator = new char[] {'/', '-', '\\', '|'};
      var count = 0;
      var startedTime = System.currentTimeMillis();
      while (!Thread.currentThread().isInterrupted()) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignored) {
          // no need to print anything since this watcher is not really important after all
          return;
        }
        var finishedIterationCount = finishedIteration.get();
        var passedSecond = (System.currentTimeMillis() - startedTime) / 1000;
        System.out.printf(
            "Progress [%d/%d], %d second%s passed %s%n",
            finishedIterationCount,
            totalIteration,
            passedSecond,
            (passedSecond == 1) ? "" : "s",
            fancyIndicator[(count++) % fancyIndicator.length]);
      }
    };
  }

  public static double coefficientOfVariance(Collection<Double> values) {
    final var average = values.stream().mapToDouble(x -> x).average().orElseThrow();
    final var variance =
        values.stream().mapToDouble(x -> x).map(x -> (x - average) * (x - average)).sum()
            / values.size();
    final var deviation = Math.sqrt(variance);
    return (average != 0) ? (deviation / average) : 0;
  }

  public static Set<String> privateTopics(TopicAdmin topicAdmin) {
    final Set<String> topic = new HashSet<>(topicAdmin.topicNames());
    topic.removeAll(topicAdmin.publicTopicNames());
    return topic;
  }
}
