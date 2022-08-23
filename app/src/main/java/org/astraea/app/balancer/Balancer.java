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
package org.astraea.app.balancer;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.ClusterBean;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.NodeInfo;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.executor.RebalanceAdmin;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasClusterCost;
import org.astraea.app.cost.HasMoveCost;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.partitioner.Configuration;

public class Balancer implements AutoCloseable {

  private final BalancerConfigs balancerConfigs;
  private final List<CostFunction> costFunctions;
  private final RebalancePlanGenerator planGenerator;
  private final RebalancePlanExecutor planExecutor;
  private final Predicate<String> topicFilter;
  private final Admin admin;
  private final MetricSource metricSource;
  private final Map<Object, IdentifiedFetcher> fetcherOwnership;
  private final AtomicBoolean isClosed;
  private final AtomicInteger runCount;

  public Balancer(Configuration configuration) {
    this.balancerConfigs = new BalancerConfigs(configuration);
    this.balancerConfigs.sanityCheck();
    this.costFunctions =
        balancerConfigs.costFunctionClasses().stream()
            .map(x -> Utils.constructCostFunction(x, configuration))
            .collect(Collectors.toUnmodifiableList());
    this.planGenerator =
        Utils.constructGenerator(balancerConfigs.rebalancePlanGeneratorClass(), configuration);
    this.planExecutor =
        Utils.constructExecutor(balancerConfigs.rebalancePlanExecutorClass(), configuration);
    this.topicFilter =
        (topic) -> {
          if (!balancerConfigs.allowedTopics().isEmpty())
            return balancerConfigs.allowedTopics().contains(topic)
                && !balancerConfigs.ignoredTopics().contains(topic);
          else return !balancerConfigs.ignoredTopics().contains(topic);
        };
    this.admin = Admin.of(balancerConfigs.bootstrapServers());

    this.fetcherOwnership =
        costFunctions.stream()
            .filter(cf -> cf.fetcher().isPresent())
            .collect(
                Collectors.toMap(
                    cf -> cf, cf -> new IdentifiedFetcher(cf.fetcher().orElseThrow())));

    this.metricSource =
        Utils.constructMetricSource(
            balancerConfigs.metricSourceClass(),
            balancerConfigs.asConfiguration(),
            fetcherOwnership.values());

    this.isClosed = new AtomicBoolean(false);
    this.runCount = new AtomicInteger(0);
  }

  /** Run balancer */
  public void run() {
    // run
    var maxRun = balancerConfigs.balancerRunCount();
    while (!Thread.currentThread().isInterrupted() && runCount.getAndIncrement() < maxRun) {
      boolean shouldDrainMetrics = false;
      // let metric warm up
      System.out.println("Warmup metrics");
      var t = BalancerUtils.progressWatch("Warm Up Metrics", 1, metricSource::warmUpProgress);
      t.start();
      metricSource.awaitMetricReady();
      t.interrupt();
      Utils.packException(() -> t.join());
      System.out.println("Metrics warmed");

      // ensure not working
      var topics =
          admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
      var migrationInProgress =
          admin.replicas(topics).entrySet().stream()
              .flatMap(x -> x.getValue().stream().map(y -> Map.entry(x.getKey(), y)))
              .filter(r -> !r.getValue().inSync() || r.getValue().isFuture())
              .map(
                  r ->
                      TopicPartitionReplica.of(
                          r.getKey().topic(), r.getKey().partition(), r.getValue().broker()))
              .collect(Collectors.toUnmodifiableList());
      if (!migrationInProgress.isEmpty()) {
        throw new IllegalStateException(
            "There are some migration in progress... " + migrationInProgress);
      }

      try {
        // calculate the score of current cluster
        var clusterInfo = newClusterInfo();
        var clusterMetrics = metricSource.allBeans();
        var currentClusterScore = evaluateCost(clusterInfo, clusterMetrics);
        System.out.println("Run " + planGenerator.getClass().getName());
        if (currentClusterScore >= 1.0) continue;
        var bestProposal = seekingRebalancePlan(currentClusterScore, clusterInfo, clusterMetrics);
        System.out.println(bestProposal);
        var bestCluster = BalancerUtils.merge(clusterInfo, bestProposal.rebalancePlan());
        var bestScore = evaluateCost(bestCluster, clusterMetrics);
        System.out.printf(
            "Current cluster score: %.8f, Proposed cluster score: %.8f%n",
            currentClusterScore, bestScore);
        if (!isPlanExecutionWorth(clusterInfo, bestProposal, currentClusterScore, bestScore)) {
          System.out.println("The proposed plan is rejected due to no worth improvement");
          continue;
        }
        System.out.println("Run " + planExecutor.getClass().getName());
        shouldDrainMetrics = true;
        executePlan(bestProposal);
      } catch (Exception e) {
        if(e.getMessage().contains("No sufficient")) {
          System.out.println("[Metrics not ready] " + e.getClass().getSimpleName());
          Utils.sleep(Duration.ofSeconds(1));
        } else {
          e.printStackTrace();
        }
      } finally {
        // drain old metrics, these metrics is probably invalid after the rebalance operation
        // performed.
        if (shouldDrainMetrics) metricSource.drainMetrics();
      }
    }
  }

  /** Retrieve a new {@link ClusterInfo}, with info only related to the permitted topics. */
  private ClusterInfo newClusterInfo() {
    var topics =
        admin.topicNames().stream().filter(topicFilter).collect(Collectors.toUnmodifiableSet());
    return admin.clusterInfo(topics);
  }

  private RebalancePlanProposal seekingRebalancePlan(
      double currentScore,
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> clusterMetrics) {
    var tries = balancerConfigs.rebalancePlanSearchingIteration();
    var counter = new LongAdder();
    var thread = BalancerUtils.progressWatch("Searching for Good Rebalance Plan", tries, counter::doubleValue);
    try {
      thread.start();
      var bestMigrationProposals =
          planGenerator
              .generate(admin.brokerFolders(), ClusterLogAllocation.of(clusterInfo))
              .parallel()
              .limit(tries)
              .peek(ignore -> counter.increment())
              .map(
                  plan -> {
                    var allocation = plan.rebalancePlan();
                    var mockedCluster = BalancerUtils.merge(clusterInfo, allocation);
                    var score = evaluateCost(mockedCluster, clusterMetrics);
                    return Map.entry(score, plan);
                  })
              .filter(x -> x.getKey() < currentScore)
              .sorted(Map.Entry.comparingByKey())
              .limit(100)
              .collect(Collectors.toUnmodifiableList());

      // Find the plan with the smallest move cost
      System.out.println("[Evaluate Movement Cost]");
      var bestMigrationProposal =
          bestMigrationProposals.stream()
              .min(
                  Comparator.comparing(
                      entry -> {
                        var proposal = entry.getValue();
                        var allocation = proposal.rebalancePlan();
                        var mockedCluster = BalancerUtils.merge(clusterInfo, allocation);
                        return evaluateMoveCost(mockedCluster, clusterMetrics);
                      }));
      System.out.println("[Evaluate Movement Cost Done]");

      // find the target with the highest score, return it
      return bestMigrationProposal
          .map(Map.Entry::getValue)
          .orElseThrow(() -> new NoSuchElementException("No Better Plan Found"));
    } finally {
      thread.interrupt();
      Utils.packException(() -> thread.join());
    }
  }

  private void executePlan(RebalancePlanProposal proposal) {
    // prepare context
    var allocation = proposal.rebalancePlan();
    try (Admin newAdmin = Admin.of(balancerConfigs.bootstrapServers())) {
      var rebalanceAdmin = RebalanceAdmin.of(newAdmin, topicFilter);

      // execute
      planExecutor.run(rebalanceAdmin, allocation);
    }
  }

  // visible for testing
  boolean isPlanExecutionWorth(
      ClusterInfo currentCluster,
      RebalancePlanProposal proposal,
      double currentScore,
      double proposedScore) {
    return currentScore > proposedScore;
  }

  private double evaluateCost(
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var scores =
        costFunctions.stream()
            .filter(x -> !(x instanceof HasMoveCost))
            .filter(x -> x instanceof HasClusterCost)
            .map(x -> (HasClusterCost) x)
            .map(
                cf -> {
                  var fetcher = fetcherOwnership.get(cf);
                  var theMetrics = metrics.get(fetcher);
                  var clusterBean = ClusterBean.of(theMetrics);
                  return Map.entry(cf, this.costScore(clusterInfo, clusterBean, cf));
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  private double evaluateMoveCost(
      ClusterInfo clusterInfo,
      Map<IdentifiedFetcher, Map<Integer, Collection<HasBeanObject>>> metrics) {
    var currentCluster = newClusterInfo();
    var scores =
        costFunctions.stream()
            .filter(func -> func instanceof HasMoveCost)
            .map(func -> (HasMoveCost) func)
            .map(
                cf -> {
                  var fetcher = fetcherOwnership.get(cf);
                  var theMetrics = metrics.get(fetcher);
                  var clusterBean = ClusterBean.of(theMetrics);
                  return Map.entry(cf, this.moveCostScore(currentCluster, clusterInfo, clusterBean, cf));
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return aggregateFunction(scores);
  }

  /** the lower, the better. */
  private <T extends CostFunction> double aggregateFunction(Map<T, Double> scores) {
    // use the simple summation result, treat every cost equally.
    return scores.values().stream().mapToDouble(x -> x).sum();
  }

  private <T extends HasClusterCost> double costScore(
      ClusterInfo clusterInfo, ClusterBean clusterBean, T costFunction) {
    return costFunction.clusterCost(clusterInfo, clusterBean).value();
  }

  private <T extends HasMoveCost> double moveCostScore(
      ClusterInfo currentCluster, ClusterInfo clusterInfo, ClusterBean clusterBean, T movementCostFunction) {
    if (movementCostFunction.overflow(currentCluster, clusterInfo, clusterBean))
      return 99999999.0;

    return movementCostFunction
        .clusterCost(currentCluster, clusterInfo, clusterBean)
        .value();
  }

  @Override
  public void close() {
    // avoid being closed twice
    if (isClosed.getAndSet(true)) return;
    admin.close();
    metricSource.close();
  }
}
