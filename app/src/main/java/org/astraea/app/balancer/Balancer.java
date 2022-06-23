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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.astraea.app.admin.Admin;
import org.astraea.app.balancer.executor.RebalanceAdmin;
import org.astraea.app.balancer.executor.RebalanceExecutionContext;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.metrics.IdentifiedFetcher;
import org.astraea.app.balancer.metrics.JmxMetricSampler;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.HasPartitionCost;
import org.astraea.app.cost.NodeInfo;
import org.astraea.app.cost.Normalizer;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.partitioner.Configuration;

public class Balancer implements AutoCloseable {

  private final BalancerConfigs balancerConfigs;
  private final List<CostFunction> costFunctions;
  private final RebalancePlanGenerator planGenerator;
  private final RebalancePlanExecutor planExecutor;
  private final Set<String> ignoredTopics;
  private final Admin admin;
  private final MetricSource metricSource;
  private final Map<Object, IdentifiedFetcher> fetcherOwnership;
  private final AtomicBoolean isClosed;

  public Balancer(Configuration configuration) {
    this.balancerConfigs = new BalancerConfigs(configuration);
    this.costFunctions =
        balancerConfigs.costFunctionClasses().stream()
            .map(x -> BalancerUtils.constructCostFunction(x, configuration))
            .collect(Collectors.toUnmodifiableList());
    this.planGenerator =
        BalancerUtils.constructGenerator(
            balancerConfigs.rebalancePlanGeneratorClass(), configuration);
    this.planExecutor =
        BalancerUtils.constructExecutor(
            balancerConfigs.rebalancePlanExecutorClass(), configuration);
    this.ignoredTopics = balancerConfigs.ignoredTopics();
    // TODO: add support for security-enabled cluster
    this.admin = Admin.of(balancerConfigs.bootstrapServers());

    this.fetcherOwnership =
        costFunctions.stream()
            .collect(Collectors.toMap(cf -> cf, cf -> new IdentifiedFetcher(cf.fetcher())));
    this.fetcherOwnership.put(planExecutor, new IdentifiedFetcher(planExecutor.fetcher()));

    this.metricSource =
        new JmxMetricSampler(
            balancerConfigs, balancerConfigs.jmxServers(), fetcherOwnership.values());

    this.isClosed = new AtomicBoolean(false);
  }

  public void start() {
    // run
    while (!Thread.currentThread().isInterrupted()) {
      boolean shouldDrainMetrics = false;
      // let metric warm up
      metricSource.awaitMetricReady();

      try {
        // TODO: find a way to show the progress, make it a standalone module, don't pollute the code here
        System.out.println("Run " + planGenerator.getClass().getName());
        var clusterInfo = newClusterInfo();
        var bestProposal = seekingRebalancePlan(clusterInfo);
        if (bestProposal.rebalancePlan().isEmpty()) break;
        System.out.println("Run " + planExecutor.getClass().getName());
        shouldDrainMetrics = true;
        executePlan(clusterInfo, bestProposal);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        // drain old metrics, these metrics is probably invalid after the rebalance operation performed.
        if(shouldDrainMetrics)
          metricSource.drainMetrics();
        // wait for a while
        Utils.packException(() -> TimeUnit.SECONDS.sleep(5));
      }
    }
  }

  private ClusterInfo newClusterInfo() {
    var topics =
        admin.topicNames().stream()
            .filter(name -> !ignoredTopics.contains(name))
            .collect(Collectors.toUnmodifiableSet());
    return admin.clusterInfo(topics);
  }

  private RebalancePlanProposal seekingRebalancePlan(ClusterInfo clusterInfo) {
    var tries = balancerConfigs.rebalancePlanSearchingIteration();
    var counter = new AtomicInteger(0);
    var thread = progressWatch(
            "Searching for Good Rebalance Plan",
            tries, counter::get);
    try {
      thread.start();

      var bestMigrationProposal =
              planGenerator
                      .generate(clusterInfo)
                      .parallel()
                      .limit(tries)
                      .peek(ignore -> counter.incrementAndGet())
                      .flatMap(plan -> plan.rebalancePlan().stream().map(x -> Map.entry(plan, x)))
                      .map(
                              alloc -> {
                                var proposedCluster =
                                        BalancerUtils.mockClusterInfoAllocation(clusterInfo, alloc.getValue());
                                var scores =
                                        costFunctions.stream()
                                                .map(cf -> Map.entry(cf, this.costFunctionScore(proposedCluster, cf)))
                                                .collect(
                                                        Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
                                var finalScore = aggregateFunction(scores);
                                return Map.entry(finalScore, alloc.getKey());
                              })
                      .min(Map.Entry.comparingByKey());

      // find the target with the highest score, return it
      return bestMigrationProposal
              .map(Map.Entry::getValue)
              .orElseThrow(() -> new NoSuchElementException("No Better Plan Found"));
    } finally {
      thread.interrupt();
      Utils.packException(() -> thread.join());
    }
  }

  private void executePlan(ClusterInfo clusterInfo, RebalancePlanProposal proposal) {
    // prepare context
    var allocation =
        proposal.rebalancePlan().orElseThrow(() -> new NoSuchElementException("No Proposal"));
    try (Admin newAdmin = Admin.of(balancerConfigs.bootstrapServers())) {
      var topicFilter = (Predicate<String>) (topicName) -> !ignoredTopics.contains(topicName);
      var executorFetcher = this.fetcherOwnership.get(planExecutor);
      var metricSource =
          (Supplier<Map<Integer, Collection<HasBeanObject>>>)
              () ->
                  this.metricSource.metrics(
                      clusterInfo.nodes().stream()
                          .map(NodeInfo::id)
                          .collect(Collectors.toUnmodifiableSet()),
                      executorFetcher);
      var rebalanceAdmin = RebalanceAdmin.of(newAdmin, metricSource, topicFilter);
      var context = RebalanceExecutionContext.of(rebalanceAdmin, allocation);

      // execute
      planExecutor.run(context);
    }
  }

  /** the lower, the better. */
  private double aggregateFunction(Map<CostFunction, Double> scores) {
    // use the simple summation result, treat every cost equally.
    return scores.values().stream().mapToDouble(x -> x).sum();
  }

  private double costFunctionScore(ClusterInfo logAllocation, CostFunction costFunction) {
    var itsFetcher = this.fetcherOwnership.get(costFunction);
    var metrics =
        this.metricSource.metrics(
            logAllocation.nodes().stream()
                .map(NodeInfo::id)
                .collect(Collectors.toUnmodifiableSet()),
            itsFetcher);
    var clusterInfo = ClusterInfo.of(logAllocation, metrics);

    if (costFunction instanceof HasBrokerCost) {
      return brokerCostScore(clusterInfo, (HasBrokerCost) costFunction);
    } else if (costFunction instanceof HasPartitionCost) {
      return partitionCostScore(clusterInfo, (HasPartitionCost) clusterInfo);
    } else {
      throw new IllegalArgumentException(
          "Unable to extract score from this cost function: " + costFunction.getClass().getName());
    }
  }

  private <T extends HasBrokerCost> double brokerCostScore(
      ClusterInfo clusterInfo, T costFunction) {
    // TODO: revise the default usage
    return costFunction
        .brokerCost(clusterInfo)
        .normalize(Normalizer.proportion())
        .value()
        .values()
        .stream()
        .mapToDouble(x -> x)
        .max()
        .orElseThrow();
  }

  private <T extends HasPartitionCost> double partitionCostScore(
      ClusterInfo clusterInfo, T costFunction) {
    // TODO: support this
    throw new UnsupportedOperationException();
  }


  // TODO: this usage will be remove some day
  private static Thread progressWatch(String title, int totalTasks, Supplier<Integer> accTasks) {
    AtomicInteger counter = new AtomicInteger();

    Supplier<String> nextProgressBar = () -> {
      int blockCount = 20;
      double percentagePerBlock = 1.0 / blockCount;
      int now = accTasks.get();
      double currentProgress = (double)now / totalTasks;
      int fulfilled = Math.min((int)(currentProgress / percentagePerBlock), blockCount);
      int rollingBlock = blockCount - fulfilled >= 1 ? 1 : 0;
      int emptyBlocks = blockCount - rollingBlock - fulfilled;

      String rollingText = "-\\|/";
      String filled = String.join("", Collections.nCopies(fulfilled, "-"));
      String rolling = String.join("", Collections.nCopies(rollingBlock, "" + rollingText.charAt(counter.getAndIncrement() % 4)));
      String empty = String.join("", Collections.nCopies(emptyBlocks, " "));
      return String.format("[%s%s%s] (%d/%d)", filled, rolling, empty, now, totalTasks);
    };

    Runnable progressWatch = () -> {
      while (!Thread.currentThread().isInterrupted()) {
        System.out.println("[" + title + "] " + nextProgressBar.get());
        try {
          TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
          break;
        }
      }
    };

    return new Thread(progressWatch);
  }

  @Override
  public void close() {
    // avoid being closed twice
    if (isClosed.getAndSet(true)) return;
    admin.close();
    metricSource.close();
  }
}
