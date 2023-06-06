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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.astraea.common.MathUtils;
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
          new Plan(
              config.clusterBean(),
              initialClusterInfo,
              initialCost,
              proposalClusterInfo,
              proposalCost));
  }

  static class AlgorithmContext {

    private final AlgorithmConfig config;
    private final ClusterInfo sourceCluster;
    private final ClusterBean clusterBean;

    private final List<ResourceUsageHint> usageHints;
    private final List<Replica> orderedReplicas;
    private final Map<Replica, Integer> branchFactor;
    private final Predicate<ResourceUsage> feasibleUsage;

    private final ForkJoinPool pool;

    private final long deadline;

    private AlgorithmContext(AlgorithmConfig config, long deadline) {
      this.config = config;
      this.sourceCluster = config.clusterInfo();
      this.clusterBean = config.clusterBean();
      this.deadline = deadline;


      System.out.println("Running");

      // TODO: recycle this service
      this.pool = new ForkJoinPool();

      // hints to estimate the resource usage of replicas
      this.usageHints =
          Stream.of(
                  config.clusterCostFunction().clusterResourceHint(sourceCluster, clusterBean),
                  config.moveCostFunction().movementResourceHint(sourceCluster, clusterBean))
              .flatMap(Collection::stream)
              .toList();
      System.out.println("Running");

      // resource usage of each replica
      var replicaUsage =
          sourceCluster.topicPartitions().stream()
              .filter(tp -> BalancerUtils.eligiblePartition(sourceCluster.replicas(tp)))
              .flatMap(tp -> sourceCluster.replicas(tp).stream())
              .collect(
                  Collectors.toUnmodifiableMap(
                      r -> r,
                      r ->
                          ResourceUsage.EMPTY.mergeUsage(
                              usageHints.stream()
                                  .map(hint -> hint.evaluateReplicaResourceUsage(r)))));

      // replicas are ordered by their resource usage, we tweak the most heavy resource first
      this.orderedReplicas =
          sourceCluster.topicPartitions().stream()
              .filter(tp -> BalancerUtils.eligiblePartition(sourceCluster.replicas(tp)))
              .flatMap(tp -> sourceCluster.replicas(tp).stream())
              .sorted(usageDominationComparator(usageHints, replicaUsage::get))
              .toList();
      System.out.println("Running");

      // perform clustering based on replica resource usage
      var usageDimension =
          replicaUsage.values().stream()
              .map(x -> x.usage().keySet())
              .flatMap(Collection::stream)
              .distinct()
              .toList();
      var clusters = Math.min(100, sourceCluster.replicas().size());
      var clustering =
          MathUtils.kMeans(
              clusters,
              5,
              this.orderedReplicas,
              (r) -> {
                var usage = replicaUsage.get(r);
                var vector = new double[usageDimension.size()];
                for (int i = 0; i < vector.length; i++)
                  vector[i] = usage.usage().getOrDefault(usageDimension.get(i), 0.0);
                return vector;
              });
      var clusterBranchFactor = sourceCluster.brokers().size();

      this.branchFactor =
          clustering.stream()
              .flatMap(
                  cluster -> {
                    int size = cluster.size();
                    int avgBranchFactor = Math.max(1, clusterBranchFactor / size);
                    return cluster.stream().map(r -> Map.entry(r, avgBranchFactor));
                  })
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      double reduce =
          this.branchFactor.values().stream().mapToDouble(x -> x).reduce(1.0, (a, b) -> a * b);
      System.out.println("Total Branch Factor: " + reduce);

      this.feasibleUsage =
          this.usageHints.stream()
              .map(ResourceUsageHint::usageValidityPredicate)
              .reduce(Predicate::and)
              .orElse((u) -> true);
      System.out.println("Running");
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
                    sourceCluster.brokers(),
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
            // TODO: Update to lock style
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

      var currentAllocation =
          sourceCluster.topicPartitions().stream()
              .collect(
                  Collectors.toUnmodifiableMap(
                      tp -> tp, tp -> (List<Replica>) new ArrayList<>(sourceCluster.replicas(tp))));
      stackSearch(updateAnswer, orderedReplicas, currentAllocation, clusterResourceUsage);

      return bestAllocation.get();
    }

    // [drives trials by doing k-means]
    // 0. Currently, user has to specify the value k.
    // 1. Doing k-means, find the best clustering for replicas.
    // 2. Replicas within a same cluster will split the allocation factor (default to folder count).
    // 3. use the allocation assignment as the trials of each replica.

    private int trials(Replica replica) {
      return branchFactor.get(replica);
    }

    private void stackSearch(
        Consumer<List<Replica>> updateAnswer,
        List<Replica> replicas,
        Map<TopicPartition, List<Replica>> currentAllocation,
        ResourceUsage initialResourceUsage) {
      record ResourceAndTweak(ResourceUsage usage, Tweak tweak) {}
      var SearchUtils =
          new Object() {
            Iterator<ResourceAndTweak> tweaks(
                Replica replica, int branch, ResourceUsage currentUsage) {
              var deduplication = new HashSet<ResourceUsage>();
              return AlgorithmContext.this.tweaks(currentAllocation, replica).stream()
                  .map(
                      tweaks -> {
                        var usageAfterTweaked =
                            currentUsage
                                .mergeUsage(
                                    tweaks.toReplace.stream()
                                        .flatMap(AlgorithmContext.this::evaluateReplicaUsage))
                                .removeUsage(
                                    tweaks.toRemove.stream()
                                        .flatMap(AlgorithmContext.this::evaluateReplicaUsage));
                        return new ResourceAndTweak(usageAfterTweaked, tweaks);
                      })
                  .filter(rat -> feasibleUsage.test(rat.usage))
                  .filter(rat -> !deduplication.contains(rat.usage))
                  .peek(rat -> deduplication.add(rat.usage))
                  .sorted(
                      Comparator.comparing(
                          ResourceAndTweak::usage,
                          usageIdealnessComparator(currentUsage, AlgorithmContext.this.usageHints)))
                  .limit(branch)
                  .iterator();
            }
          };
      var permutations = new ArrayList<Iterator<ResourceAndTweak>>();
      var usedTweaks = new ArrayList<ResourceAndTweak>();

      // add first tweaks here
      permutations.add(
          SearchUtils.tweaks(
              replicas.get(0), trials(orderedReplicas.get(0)), initialResourceUsage));

      if (!permutations.get(0).hasNext()) throw new IllegalStateException("illegal start");

      Restart:
      do {
        // discard all empty iterators at stack end
        while(!permutations.isEmpty() && !permutations.get(permutations.size() - 1).hasNext()) {
          if (!usedTweaks.isEmpty()) {
            var revert = usedTweaks.remove(usedTweaks.size() - 1);
            revert.tweak.toReplace.forEach(
                r -> currentAllocation.get(r.topicPartition()).remove(r));
            revert.tweak.toRemove.forEach(r -> currentAllocation.get(r.topicPartition()).add(r));
          }
          permutations.remove(permutations.size() - 1);
        }
        // break this loop if no job remained.
        if(permutations.isEmpty())
          break;

        // keep applying tweak & adding iterator until the last replica
        while (usedTweaks.size() < permutations.size()) {
          int head = usedTweaks.size();
          var iter = permutations.get(head);
          if (!iter.hasNext()) continue Restart;
          var rat = iter.next();
          rat.tweak.toRemove.forEach(r -> currentAllocation.get(r.topicPartition()).remove(r));
          rat.tweak.toReplace.forEach(r -> currentAllocation.get(r.topicPartition()).add(r));
          usedTweaks.add(rat);

          // push new iterators for all replicas
          if (usedTweaks.size() == permutations.size() && permutations.size() < replicas.size())
            permutations.add(
                SearchUtils.tweaks(
                    replicas.get(permutations.size()),
                    trials(orderedReplicas.get(permutations.size())),
                    rat.usage));
        }

        // in the end, we will have a complete answer, update result.
        updateAnswer.accept(
            currentAllocation.values().stream().flatMap(Collection::stream).toList());
        var rat = usedTweaks.remove(usedTweaks.size() - 1);
        rat.tweak.toReplace.forEach(r -> currentAllocation.get(r.topicPartition()).remove(r));
        rat.tweak.toRemove.forEach(r -> currentAllocation.get(r.topicPartition()).add(r));
      } while (!permutations.isEmpty() && System.currentTimeMillis() < deadline);
    }

    private List<Tweak> puts(Replica replica) {
      return sourceCluster.brokers().stream()
          .flatMap(
              broker ->
                  sourceCluster.brokerFolders().get(broker.id()).stream()
                      .map(
                          path ->
                              Replica.builder(replica).brokerId(broker.id()).path(path).build()))
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
          this.sourceCluster.brokerFolders().get(replica.brokerId()).stream()
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
              .filter(b -> b.id() != replica.brokerId())
              .flatMap(
                  b ->
                      b.dataFolders().stream()
                          .map(
                              folder ->
                                  new Tweak(
                                      List.of(replica),
                                      List.of(
                                          Replica.builder(replica)
                                              .brokerId(b.id())
                                              .path(folder)
                                              .build()))))
              .toList();

      // usage among tweaks
      return Stream.of(noMovement, leadership, interBrokerMovement, dataFolderMovement)
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

    static Comparator<ResourceUsage> usageIdealnessComparator(
        ResourceUsage baseUsage, List<ResourceUsageHint> usageHints) {
      var baseIdealness = usageHints.stream().map(hint -> hint.idealness(baseUsage)).toList();

      return Comparator.comparingDouble(
          usage ->
              IntStream.range(0, usageHints.size())
                  .mapToDouble(
                      index -> usageHints.get(index).idealness(usage) - baseIdealness.get(index))
                  .sum());
    }

    private record Tweak(List<Replica> toRemove, List<Replica> toReplace) {}
  }
}
