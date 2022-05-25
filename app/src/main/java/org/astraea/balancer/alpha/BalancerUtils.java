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
import org.astraea.Utils;
import org.astraea.admin.Admin;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.RebalancePlanProposal;
import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.balancer.log.LogPlacement;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.cost.HasPartitionCost;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.PartitionCost;
import org.astraea.cost.ReplicaInfo;
import org.astraea.metrics.HasBeanObject;

public class BalancerUtils {

  /** create a fake cluster info based on given proposal */
  public static ClusterInfo clusterInfoFromProposal(
      ClusterInfo clusterInfo, RebalancePlanProposal proposal) {

    // TODO: validate if the proposal is suitable for given clusterInfo, for example: every topic in
    // the proposal exists in the clusterInfo

    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return clusterInfo.nodes();
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        return clusterInfo.dataDirectories(brokerId);
      }

      @Override
      public List<ReplicaInfo> availablePartitionLeaders(String topic) {
        return partitions(topic).stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availablePartitions(String topic) {
        return partitions(topic).stream()
            .filter(x -> !x.isOfflineReplica())
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Set<String> topics() {
        return clusterInfo.topics();
      }

      @Override
      public List<ReplicaInfo> partitions(String topic) {
        final var nodes =
            nodes().stream().collect(Collectors.toUnmodifiableMap(NodeInfo::id, x -> x));

        return proposal
            .rebalancePlan()
            .map(
                allocation -> {
                  // TODO: Extend the ClusterLogAllocation structure to support log directory info
                  return allocation
                      .topicPartitionStream()
                      .flatMap(
                          topicPartition -> {
                            final var logAllocation = allocation.logPlacements(topicPartition);

                            return IntStream.range(0, logAllocation.size())
                                .mapToObj(
                                    index ->
                                        ReplicaInfo.of(
                                            topicPartition.topic(),
                                            topicPartition.partition(),
                                            nodes.get(logAllocation.get(index).broker()),
                                            index == 0,
                                            false,
                                            false));
                          })
                      .collect(Collectors.toUnmodifiableList());
                })
            .orElse(List.of());
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
    if (proposal.rebalancePlan().isPresent()) {
      System.out.printf("[New Rebalance Plan Generated %s]%n", LocalDateTime.now());

      final var balanceAllocation = proposal.rebalancePlan().orElseThrow();
      if (balanceAllocation.topicPartitionStream().findAny().isPresent()) {
        balanceAllocation
            .topicPartitionStream()
            .forEach(
                (topicPartition) -> {
                  System.out.printf(" \"%s\":%n", topicPartition);
                  // TODO: add support to show difference in data directory
                  final var originalState = currentAllocation.logPlacements(topicPartition);
                  final var finalState = balanceAllocation.logPlacements(topicPartition);
                  final var noChange =
                      originalState.stream()
                          .filter(
                              srcLog ->
                                  finalState.stream()
                                      .anyMatch(finLog -> srcLog.broker() == finLog.broker()))
                          .sorted()
                          .collect(Collectors.toUnmodifiableList());
                  final var toDelete =
                      originalState.stream()
                          .filter(
                              srcLog ->
                                  finalState.stream()
                                      .noneMatch(finLog -> srcLog.broker() == finLog.broker()))
                          .sorted()
                          .collect(Collectors.toUnmodifiableList());
                  final var toReplicate =
                      finalState.stream()
                          .filter(
                              finLog ->
                                  originalState.stream()
                                      .noneMatch(srcLog -> srcLog.broker() == finLog.broker()))
                          .sorted()
                          .collect(Collectors.toUnmodifiableList());

                  boolean noChangeAtAll = toDelete.size() == 0 && toReplicate.size() == 0;
                  if (!noChangeAtAll) {
                    System.out.println("       no change: " + noChange);
                    System.out.println("       to delete: " + toDelete);
                    System.out.println("       to replicate: " + toReplicate);
                  }
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
    System.out.println();
  }

  public static Map<TopicPartition, List<LogPlacement>> diffAllocation(
      ClusterLogAllocation proposal, ClusterLogAllocation currentAllocation) {
    final var balanceAllocation = proposal;
    if (balanceAllocation.topicPartitionStream().findAny().isPresent()) {
      final var diff = new HashMap<TopicPartition, List<LogPlacement>>();
      balanceAllocation
          .topicPartitionStream()
          .forEach(
              (topicPartition) -> {
                System.out.printf(" \"%s\":%n", topicPartition);
                // TODO: add support to show difference in data directory
                final var originalState = currentAllocation.logPlacements(topicPartition);
                final var finalState = balanceAllocation.logPlacements(topicPartition);

                final var toReplicate =
                    finalState.stream()
                        .filter(
                            finLog ->
                                originalState.stream()
                                    .noneMatch(srcLog -> srcLog.broker() == finLog.broker()))
                        .sorted()
                        .collect(Collectors.toUnmodifiableList());

                diff.put(topicPartition, toReplicate);
              });
      return diff;
    } else {
      return Map.of();
    }
  }

  public static ClusterInfo clusterSnapShot(Admin topicAdmin) {
    return clusterSnapShot(topicAdmin, Set.of());
  }

  public static ClusterInfo clusterSnapShot(Admin topicAdmin, Set<String> topicToIgnore) {
    final var nodeInfo =
        topicAdmin.brokerIds().stream()
            .map(id -> NodeInfo.of(id, "fix me", 66666))
            .collect(Collectors.toUnmodifiableList());
    final var nodeInfoMap =
        nodeInfo.stream().collect(Collectors.toUnmodifiableMap(NodeInfo::id, Function.identity()));
    final var topics =
        topicAdmin.topicNames().stream()
            .filter(topic -> !topicToIgnore.contains(topic))
            .collect(Collectors.toUnmodifiableSet());
    final var partitions =
        Utils.handleException(() -> topicAdmin.replicas(topics)).entrySet().stream()
            .flatMap(
                entry -> {
                  // TODO: there is a bug in here. topicAdmin.replicas doesn't return the full
                  // information of each replica if there are some offline here. might be fix in
                  // #308?
                  final var topicPartition = entry.getKey();
                  final var replicas = entry.getValue();

                  return replicas.stream()
                      .map(
                          replica ->
                              ReplicaInfo.of(
                                  topicPartition.topic(),
                                  topicPartition.partition(),
                                  nodeInfoMap.get(replica.broker()),
                                  replica.leader(),
                                  replica.inSync(),
                                  // TODO: fix the isOfflineReplica flag once the #308 is merged
                                  false,
                                  replica.path()));
                })
            .collect(Collectors.groupingBy(ReplicaInfo::topic));
    final var dataDirectories =
        topicAdmin
            .brokerFolders(
                nodeInfo.stream().map(NodeInfo::id).collect(Collectors.toUnmodifiableSet()))
            .entrySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> Set.copyOf(x.getValue())));

    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return nodeInfo;
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        return dataDirectories.get(brokerId);
      }

      @Override
      public List<ReplicaInfo> availablePartitionLeaders(String topic) {
        return partitions.get(topic).stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availablePartitions(String topic) {
        return partitions.get(topic).stream()
            .filter((ReplicaInfo x) -> !x.isOfflineReplica())
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Set<String> topics() {
        return topics;
      }

      @Override
      public List<ReplicaInfo> partitions(String topic) {
        return partitions.get(topic);
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        return List.of();
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        return nodeInfo.stream()
            .collect(Collectors.toUnmodifiableMap(NodeInfo::id, ignore -> List.of()));
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

  public static Set<String> privateTopics(Admin topicAdmin) {
    final Set<String> topic = new HashSet<>(topicAdmin.topicNames());
    throw new UnsupportedOperationException("Implement this");
    // topic.removeAll(topicAdmin.publicTopicNames());
    // return topic;
  }
}
