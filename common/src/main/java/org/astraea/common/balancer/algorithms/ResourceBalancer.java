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
package org.astraea.common.balancer.algorithms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.cost.ResourceUsage;
import org.astraea.common.cost.ResourceUsageHint;
import org.astraea.common.metrics.ClusterBean;

public class ResourceBalancer implements Balancer {

  @Override
  public Optional<Plan> offer(AlgorithmConfig config) {
    var runtime = config.timeout().toMillis() + System.currentTimeMillis();
    var initialClusterInfo = config.clusterInfo();
    var initialCost =
        config.clusterCostFunction().clusterCost(config.clusterInfo(), config.clusterBean());

    var algorithm = new AlgorithmContext(config, runtime);
    var proposalClusterInfo = algorithm.execute();
    var proposalCost =
        config.clusterCostFunction().clusterCost(proposalClusterInfo, config.clusterBean());
    var moveCost =
        config
            .moveCostFunction()
            .moveCost(initialClusterInfo, proposalClusterInfo, config.clusterBean());

    if (proposalCost.value() > initialCost.value() || moveCost.overflow()) return Optional.empty();
    else
      return Optional.of(
          new Plan(initialClusterInfo, initialCost, proposalClusterInfo, proposalCost));
  }

  static class AlgorithmContext {

    private final AlgorithmConfig config;
    private final ClusterInfo sourceCluster;
    private final ClusterBean clusterBean;

    private final List<ResourceUsageHint> usageHints;
    private final List<Replica> orderedReplicas;
    private final Predicate<ResourceUsage> feasibleUsage;

    private final long deadline;

    private AlgorithmContext(AlgorithmConfig config, long deadline) {
      this.config = config;
      this.sourceCluster = config.clusterInfo();
      this.clusterBean = config.clusterBean();
      this.deadline = deadline;

      // hints to estimate the resource usage of replicas
      this.usageHints =
          Stream.of(
                  config.clusterCostFunction().clusterResourceHint(sourceCluster, clusterBean),
                  config.moveCostFunction().movementResourceHint(sourceCluster, clusterBean))
              .flatMap(Collection::stream)
              .toList();

      // replicas are ordered by their resource usage, we tweak the most heavy resource first
      this.orderedReplicas =
          sourceCluster.topicPartitions().stream()
              .filter(tp -> BalancerUtils.eligiblePartition(sourceCluster.replicas(tp)))
              .flatMap(tp -> sourceCluster.replicas(tp).stream())
              .sorted(
                  usageDominationComparator(
                      usageHints,
                      (r) ->
                          ResourceUsage.EMPTY.mergeUsage(
                              usageHints.stream()
                                  .map(hint -> hint.evaluateReplicaResourceUsage(r)))))
              .toList();

      this.feasibleUsage =
          this.usageHints.stream()
              .map(ResourceUsageHint::usageValidityPredicate)
              .reduce(Predicate::and)
              .orElse((u) -> true);
    }

    ClusterInfo execute() {
      var clusterResourceUsage =
          ResourceUsage.EMPTY.mergeUsage(
              sourceCluster.replicas().stream().flatMap(this::evaluateReplicaUsage));

      var bestAllocation = new AtomicReference<ClusterInfo>();
      var bestAllocationScore = new AtomicReference<Double>();
      Consumer<List<Replica>> updateAnswer =
          (replicas) -> {
            var newCluster =
                ClusterInfo.of(
                    sourceCluster.clusterId(),
                    sourceCluster.nodes(),
                    sourceCluster.topics(),
                    replicas);
            var clusterCost = config.clusterCostFunction().clusterCost(newCluster, clusterBean);
            var moveCost =
                config.moveCostFunction().moveCost(sourceCluster, newCluster, clusterBean);

            // if movement constraint failed, reject answer
            if (moveCost.overflow()) {
              System.out.println("Overflow Score: " + clusterCost.value());
              return;
            }
            // if cluster cost is better, accept answer
            if (bestAllocationScore.get() == null
                || clusterCost.value() < bestAllocationScore.get()) {
              bestAllocation.set(newCluster);
              bestAllocationScore.set(clusterCost.value());
              System.out.println("New Best Score: " + bestAllocationScore.get());
              System.out.println("New Best Cost: " + clusterCost);
            } else {
              System.out.println("New Score: " + clusterCost.value());
              System.out.println(clusterCost);
            }
          };

      // TODO: the recursion might overflow the stack under large number of replicas. use stack
      //  instead.
      var currentAllocation =
          sourceCluster.topicPartitions().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      tp -> tp, tp -> (List<Replica>) new ArrayList<>(sourceCluster.replicas(tp))));
      search(updateAnswer, 0, orderedReplicas, currentAllocation, clusterResourceUsage);

      return bestAllocation.get();
    }

    private int trials(int level) {
      // TODO: customize this
      if (0 <= level && level < 3) return 8;
      if (level < 6) return 2;
      else return 1;
    }

    private void search(
        Consumer<List<Replica>> updateAnswer,
        int next,
        List<Replica> originalReplicas,
        Map<TopicPartition, List<Replica>> currentAllocation,
        ResourceUsage currentResourceUsage) {
      if (System.currentTimeMillis() > deadline) return;
      if (originalReplicas.size() == next) {
        // if this is a complete answer, call update function and return
        updateAnswer.accept(
            currentAllocation.entrySet().stream().flatMap(x -> x.getValue().stream()).toList());
      } else {
        var nextReplica = originalReplicas.get(next);

        List<Map.Entry<ResourceUsage, Tweak>> possibleTweaks =
            tweaks(currentAllocation, nextReplica).stream()
                .map(
                    tweaks -> {
                      var usageAfterTweaked =
                          currentResourceUsage
                              .mergeUsage(
                                  tweaks.toReplace.stream().flatMap(this::evaluateReplicaUsage))
                              .removeUsage(
                                  tweaks.toRemove.stream().flatMap(this::evaluateReplicaUsage));
                      return Map.entry(usageAfterTweaked, tweaks);
                    })
                .filter(e -> feasibleUsage.test(e.getKey()))
                .sorted(
                    Map.Entry.comparingByKey(
                        usageIdealnessDominationComparator2(currentResourceUsage, this.usageHints)))
                .limit(trials(next))
                .toList();

        for (Map.Entry<ResourceUsage, Tweak> entry : possibleTweaks) {
          // the tweak we are going to use
          var newResourceUsage = entry.getKey();
          var tweaks = entry.getValue();

          // replace the replicas
          tweaks.toRemove.stream()
              .filter(replica -> !currentAllocation.get(replica.topicPartition()).remove(replica))
              .forEach(
                  nonexistReplica -> {
                    throw new IllegalStateException(
                        "Attempt to remove "
                            + nonexistReplica.topicPartitionReplica()
                            + " but it does not exists");
                  });
          tweaks.toReplace.forEach(
              replica -> currentAllocation.get(replica.topicPartition()).add(replica));

          // start next search stage
          search(updateAnswer, next + 1, originalReplicas, currentAllocation, newResourceUsage);

          // undo the tweak, restore the previous state
          tweaks.toReplace.stream()
              .filter(replica -> !currentAllocation.get(replica.topicPartition()).remove(replica))
              .forEach(
                  nonexistReplica -> {
                    throw new IllegalStateException(
                        "Attempt to remove "
                            + nonexistReplica.topicPartitionReplica()
                            + " but it does not exists");
                  });
          tweaks.toRemove.forEach(
              replica -> currentAllocation.get(replica.topicPartition()).add(replica));
        }
      }
    }

    private List<Tweak> puts(Replica replica) {
      return sourceCluster.brokers().stream()
          .flatMap(
              broker ->
                  sourceCluster.brokerFolders().get(broker.id()).stream()
                      .map(path -> Replica.builder(replica).nodeInfo(broker).path(path).build()))
          .map(newReplica -> new Tweak(List.of(), List.of(newReplica)))
          .toList();
    }

    private List<Tweak> tweaks(
        Map<TopicPartition, List<Replica>> currentAllocation, Replica replica) {
      // 1. no change
      var noMovement = List.of(new Tweak(List.of(), List.of()));

      // 2. leadership change
      var leadership =
          currentAllocation.get(replica.topicPartition()).stream()
              .filter(r -> r.isPreferredLeader() != replica.isPreferredLeader())
              .map(
                  switchTarget -> {
                    var toRemove = List.of(replica, switchTarget);
                    var toReplace =
                        List.of(
                            Replica.builder(replica)
                                .isLeader(!replica.isPreferredLeader())
                                .isPreferredLeader(!replica.isPreferredLeader())
                                .build(),
                            Replica.builder(switchTarget)
                                .isLeader(replica.isPreferredLeader())
                                .isPreferredLeader(replica.isPreferredLeader())
                                .build());

                    return new Tweak(toRemove, toReplace);
                  })
              .toList();

      // 3. move to other data-dir at the same broker
      var dataFolderMovement =
          this.sourceCluster.brokerFolders().get(replica.nodeInfo().id()).stream()
              .filter(folder -> !folder.equals(replica.path()))
              .map(
                  newFolder ->
                      new Tweak(
                          List.of(replica),
                          List.of(Replica.builder(replica).path(newFolder).build())))
              .toList();

      // 4. move to other brokers/data-dirs
      var interBrokerMovement =
          this.sourceCluster.brokers().stream()
              .filter(b -> b.id() != replica.nodeInfo().id())
              .flatMap(
                  b ->
                      b.dataFolders().stream()
                          // TODO: add data folder back once the framework is ready to deduplicate
                          // the similar resource usage among tweaks
                          .limit(1)
                          .map(
                              folder ->
                                  new Tweak(
                                      List.of(replica),
                                      List.of(
                                          Replica.builder(replica)
                                              .nodeInfo(b)
                                              .path(folder.path())
                                              .build()))))
              .toList();

      // TODO: add data folder back once the framework is ready to deduplicate the similar resource
      // usage among tweaks
      return Stream.of(noMovement, leadership, interBrokerMovement)
          .flatMap(Collection::stream)
          .toList();
    }

    private Stream<ResourceUsage> evaluateReplicaUsage(Replica replica) {
      return this.usageHints.stream().map(hint -> hint.evaluateClusterResourceUsage(replica));
    }

    static Comparator<Replica> usageDominationComparator(
        List<ResourceUsageHint> usageHints, Function<Replica, ResourceUsage> replicaResourceUsage) {
      var cmp =
          Comparator.<ResourceUsage>comparingDouble(
              u -> usageHints.stream().mapToDouble(c -> c.importance(u)).average().orElseThrow());

      return Comparator.comparing(replicaResourceUsage, cmp);
    }

    static Comparator<ResourceUsage> usageIdealnessDominationComparator2(
        ResourceUsage baseUsage, List<ResourceUsageHint> usageHints) {
      var baseIdealness = usageHints.stream().map(hint -> hint.idealness(baseUsage)).toList();

      return (lhs, rhs) -> {
        var idealnessVectorL =
            IntStream.range(0, usageHints.size())
                .mapToObj(index -> usageHints.get(index).idealness(lhs) - baseIdealness.get(index))
                .toList();
        var idealnessVectorR =
            IntStream.range(0, usageHints.size())
                .mapToObj(index -> usageHints.get(index).idealness(rhs) - baseIdealness.get(index))
                .toList();

        var sumL = idealnessVectorL.stream().mapToDouble(x -> x).sum();
        var sumR = idealnessVectorR.stream().mapToDouble(x -> x).sum();

        return Double.compare(sumL, sumR);
      };
    }

    private record Tweak(List<Replica> toRemove, List<Replica> toReplace) {}

    // `static Comparator<Replica> usageDominationComparator(
    // `    Function<Replica, ResourceUsage> usageHints) {
    // `  // TODO: implement the actual dominant sort
    // `  return (lhs, rhs) -> {
    // `    // var resourceL = usageHints.apply(lhs);
    // `    // var resourceR = usageHints.apply(rhs);

    // `    // var dominatedByL =
    // `    //     resourceL.usage().entrySet().stream()
    // `    //         .filter(e -> e.getValue() > resourceR.usage().getOrDefault(e.getKey(), 0.0))
    // `    //         .count();
    // `    // var dominatedByR =
    // `    //     resourceR.usage().entrySet().stream()
    // `    //         .filter(e -> e.getValue() > resourceL.usage().getOrDefault(e.getKey(), 0.0))
    // `    //         .count();

    // `    // // reverse the order intentionally, we want the most dominated replica at the
    // beginning of
    // `    // // list.
    // `    // int compare = Long.compare(dominatedByL, dominatedByR);
    // `    // return -compare;

    // `    double lsum = usageHints.apply(lhs).usage().values().stream().mapToDouble(x -> x).sum();
    // `    double rsum = usageHints.apply(rhs).usage().values().stream().mapToDouble(x -> x).sum();
    // `    return -Double.compare(lsum, rsum);
    // `  };
    // `}

    // static Comparator<ResourceUsage> usageIdealnessDominationComparator(
    //     ResourceUsage base, List<ResourceUsageHint> usageHints) {
    //   var comparators =
    //       usageHints.stream()
    //           .map(ResourceUsageHint::usageIdealnessComparator)
    //           .collect(Collectors.toUnmodifiableSet());

    //   Comparator<ResourceUsage> dominatedCmp =
    //       (lhs, rhs) -> {
    //         var dominatedByL = comparators.stream().filter(e -> e.compare(lhs, rhs) <=
    // 0).count();
    //         var dominatedByR = comparators.stream().filter(e -> e.compare(rhs, lhs) <=
    // 0).count();

    //         return -Long.compare(dominatedByL, dominatedByR);
    //       };

    //   // return usageIdealnessDominationComparator(resourceCapacities)
    //   //     .thenComparingDouble(usage -> resourceCapacities.stream()
    //   //         .mapToDouble(ca -> ca.idealness(usage))
    //   //         .average()
    //   //         .orElseThrow());
    //   return dominatedCmp.thenComparingDouble(
    //       usage ->
    //           usageHints.stream().mapToDouble(ca ->
    // ca.idealness(usage)).average().orElseThrow());
    // }
  }
}
