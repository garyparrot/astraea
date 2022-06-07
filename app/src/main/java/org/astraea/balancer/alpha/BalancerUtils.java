/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.HasPartitionCost;
import org.astraea.app.cost.NodeInfo;
import org.astraea.app.cost.PartitionCost;
import org.astraea.app.cost.ReplicaInfo;
import org.astraea.app.metrics.HasBeanObject;

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
      public List<ReplicaInfo> availableReplicaLeaders(String topic) {
        return replicas(topic).stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availableReplicas(String topic) {
        return replicas(topic).stream()
            .filter(x -> !x.isOfflineReplica())
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Set<String> topics() {
        return clusterInfo.topics();
      }

      @Override
      public List<ReplicaInfo> replicas(String topic) {
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

  @Deprecated
  public static ClusterInfo clusterSnapShot(Admin topicAdmin) {
    return clusterSnapShot(topicAdmin, Set.of());
  }

  @Deprecated
  public static ClusterInfo clusterSnapShot(Admin admin, Set<String> topicToIgnore) {
    return admin.clusterInfo(
        admin.topicNames().stream()
            .filter(name -> !topicToIgnore.contains(name))
            .collect(Collectors.toUnmodifiableSet()));
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
