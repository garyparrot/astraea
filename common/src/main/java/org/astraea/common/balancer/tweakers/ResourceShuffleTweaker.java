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
package org.astraea.common.balancer.tweakers;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.astraea.common.EnumInfo;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.ClusterInfoBuilder;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.algorithms.BalancerUtils;
import org.astraea.common.cost.ResourceCapacity;
import org.astraea.common.cost.ResourceUsage;
import org.astraea.common.cost.ResourceUsageHint;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@link ResourceShuffleTweaker} proposes a new log placement based on the current log placement, but
 * with a few random placement changes. <br>
 * <br>
 * The following operations are considered as a valid shuffle action:
 *
 * <ol>
 *   <li>Change the leader/follower of a partition to a member of this replica set, the original
 *       leader/follower becomes a follower/leader.
 *   <li>Remove a replica from the replica set, then add another broker(must not be part of the
 *       replica set before this action) into the replica set.
 * </ol>
 */
public class ResourceShuffleTweaker {

  private final Supplier<Integer> numberOfShuffle;
  private final Predicate<String> allowedTopics;
  private final Collection<ResourceUsageHint> hints;
  private final ClusterInfo sourceCluster;
  private final ClusterBean clusterBean;

  public ResourceShuffleTweaker(Supplier<Integer> numberOfShuffle, Predicate<String> allowedTopics, ClusterInfo sourceCluster, ClusterBean clusterBean, Collection<ResourceUsageHint> hints) {
    this.numberOfShuffle = numberOfShuffle;
    this.allowedTopics = allowedTopics;
    this.sourceCluster = sourceCluster;
    this.clusterBean = clusterBean;
    this.hints = hints;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Stream<ClusterInfo> generate(ClusterInfo baseAllocation) {
    // There is no broker
    if (baseAllocation.nodes().isEmpty()) return Stream.of();

    // No non-ignored topic to working on.
    if (baseAllocation.topicPartitions().isEmpty()) return Stream.of();

    // Only one broker & one folder exists, unable to do any log migration
    if (baseAllocation.nodes().size() == 1
        && baseAllocation.brokerFolders().values().stream().findFirst().orElseThrow().size() == 1)
      return Stream.of();

    final var baseAllocationUsage = new ResourceUsage();
    baseAllocation
        .replicas()
        .forEach(replica -> hints
            .forEach(hint -> baseAllocationUsage.mergeUsage(hint.evaluateClusterResourceUsage(
                sourceCluster,
                clusterBean,
                replica))));

    final List<ResourceCapacity> capacities = this.hints.stream()
        .flatMap(hint -> hint.evaluateClusterResourceCapacity(sourceCluster, clusterBean).stream())
        .collect(Collectors.toUnmodifiableList());

    final var partitionOrder =
        baseAllocation.topicPartitions().stream()
            .filter(tp -> this.allowedTopics.test(tp.topic()))
            .map(tp -> Map.entry(tp, baseAllocation.replicas(tp)
                .stream()
                .mapToDouble(replica -> {
                  var resourceUsage = new ResourceUsage();
                  hints.stream()
                      .map(hint -> hint.evaluateReplicaResourceUsage(sourceCluster, clusterBean, replica))
                      .forEach(usage -> resourceUsage.mergeUsage(usage));
                  return resourceUsage.usage().values().stream().mapToDouble(x -> x).sum();
                  // return capacities.stream()
                  //     .mapToDouble(c -> c.idealness(resourceUsage))
                  //     .map(e -> 1 - e)
                  //     .average()
                  //     .orElseThrow();
                })
                .average()
                .orElseThrow()))
            .map(e -> Pair.create(e.getKey(), e.getValue()))
            .collect(Collectors.toUnmodifiableList());

    final var dist = new EnumeratedDistribution<>(partitionOrder);

    return Stream.generate(
        () -> {
          final var shuffleCount = numberOfShuffle.get();

          var currentUsage = new ResourceUsage(baseAllocationUsage.usage());
          var currentAllocation = baseAllocation.topicPartitions()
              .stream()
              .collect(Collectors.toMap(
                  tp -> tp,
                  tp -> baseAllocation.replicas(tp).stream().collect(Collectors.toList())));
          var usagePredicate = capacities.stream()
              .map(ResourceCapacity::usageValidnessPredicate)
              .reduce(Predicate::and)
              .orElse((a) -> true);

          for (int i = 0, shuffled = 0; i < partitionOrder.size() && shuffled < shuffleCount; i++) {
            final var tp = dist.sample();
            if (!BalancerUtils.eligiblePartition(baseAllocation.replicas(tp))) continue;
            // List all possible tweaks. Calculate the ResourceUsage after each tweak.
            // Rank the idealness of the ResourceUsage associated with each tweak.
            // Apply the tweak with the best idealness. Update the current ResourceUsage. So we can carry this RS to next stage.
            var randomReplica = baseAllocation.replicas(tp).get(
                ThreadLocalRandom.current().nextInt(0, baseAllocation.replicas(tp).size()));

            final var current = currentUsage;
            Map.Entry<ResourceUsage, Tweak> entry = tweaks(baseAllocation, currentAllocation, randomReplica)
                .stream()
                .map(tweak -> {
                  var usageAfterTweak = new ResourceUsage(current.usage());
                  tweak.toReplace.stream()
                      .flatMap(replica -> hints.stream().map(hint -> hint.evaluateClusterResourceUsage(sourceCluster, clusterBean, replica)))
                      .forEach(usageAfterTweak::mergeUsage);
                  tweak.toRemove.stream()
                      .flatMap(replica -> hints.stream().map(hint -> hint.evaluateClusterResourceUsage(sourceCluster, clusterBean, replica)))
                      .forEach(usageAfterTweak::removeUsage);

                  return Map.entry(usageAfterTweak, tweak);
                })
                .filter(e -> usagePredicate.test(e.getKey()))
                .min(Map.Entry.comparingByKey(usageIdealnessDominationComparator(capacities)))
                .orElseThrow();

            currentUsage = entry.getKey();
            entry.getValue().toRemove.forEach(remove -> currentAllocation.get(remove.topicPartition()).remove(remove));
            entry.getValue().toReplace.forEach(replace -> currentAllocation.get(replace.topicPartition()).add(replace));
            shuffled++;
          }

          return ClusterInfo.of(
              baseAllocation.clusterId(),
              baseAllocation.nodes(),
              baseAllocation.topics(),
              currentAllocation.values().stream()
                  .flatMap(Collection::stream)
                  .collect(Collectors.toUnmodifiableList()));
        });
  }

  private static <T> T randomElement(Collection<T> collection) {
    return collection.stream()
        .skip(ThreadLocalRandom.current().nextInt(0, collection.size()))
        .findFirst()
        .orElseThrow();
  }

  enum Operation implements EnumInfo {
    LEADERSHIP_CHANGE,
    REPLICA_LIST_CHANGE;

    private static final List<Operation> OPERATIONS =
        Arrays.stream(Operation.values()).collect(Collectors.toUnmodifiableList());

    public static Operation random() {
      return OPERATIONS.get(ThreadLocalRandom.current().nextInt(OPERATIONS.size()));
    }

    public static Operation ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Operation.class, alias);
    }

    @Override
    public String alias() {
      return name();
    }

    @Override
    public String toString() {
      return alias();
    }
  }

  public static class Builder {

    private Supplier<Integer> numberOfShuffle = () -> ThreadLocalRandom.current().nextInt(1, 5);
    private Predicate<String> allowedTopics = (name) -> true;

    private Builder() {}

    public Builder numberOfShuffle(Supplier<Integer> numberOfShuffle) {
      this.numberOfShuffle = numberOfShuffle;
      return this;
    }

    public Builder allowedTopics(Predicate<String> allowedTopics) {
      this.allowedTopics = allowedTopics;
      return this;
    }

    public ResourceShuffleTweaker build() {
      throw new UnsupportedOperationException();
    }
  }

  static Comparator<ResourceUsage> usageIdealnessDominationComparator(List<ResourceCapacity> resourceCapacities) {
    var comparators =
        resourceCapacities.stream()
            .map(ResourceCapacity::usageIdealnessComparator)
            .collect(Collectors.toUnmodifiableSet());

    Comparator<ResourceUsage> dominatedCmp = (lhs, rhs) -> {
      var dominatedByL = comparators.stream().filter(e -> e.compare(lhs, rhs) <= 0).count();
      var dominatedByR = comparators.stream().filter(e -> e.compare(rhs, lhs) <= 0).count();

      return -Long.compare(dominatedByL, dominatedByR);
    };

    return dominatedCmp.thenComparingDouble(usage -> resourceCapacities.stream()
        .mapToDouble(ca -> ca.idealness(usage))
        .average()
        .orElseThrow());
  }

  private List<Tweak> tweaks(
      ClusterInfo cluster,
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
            .collect(Collectors.toUnmodifiableList());

    // 3. move to other data-dir at the same broker
    var dataFolderMovement =
        cluster.brokerFolders().get(replica.nodeInfo().id()).stream()
            .filter(folder -> !folder.equals(replica.path()))
            .map(
                newFolder ->
                    new Tweak(
                        List.of(replica),
                        List.of(Replica.builder(replica).path(newFolder).build())))
            .collect(Collectors.toUnmodifiableList());

    // 4. move to other brokers/data-dirs
    var interBrokerMovement =
        cluster.brokers().stream()
            .filter(b -> b.id() != replica.nodeInfo().id())
            .flatMap(
                b ->
                    b.dataFolders().stream()
                        // TODO: add data folder back once the framework is ready to deduplicate the similar resource usage among tweaks
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
            .collect(Collectors.toUnmodifiableList());

    // TODO: add data folder back once the framework is ready to deduplicate the similar resource usage among tweaks
    return Stream.of(noMovement, leadership, interBrokerMovement)
        .flatMap(Collection::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  private static class Tweak {
    private final List<Replica> toRemove;
    private final List<Replica> toReplace;

    private Tweak(List<Replica> toRemove, List<Replica> toReplace) {
      this.toRemove = toRemove;
      this.toReplace = toReplace;
    }
  }
}
