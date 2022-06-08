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
package org.astraea.app.balancer.alpha;

import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.admin.Admin;
import org.astraea.app.argument.DurationField;
import org.astraea.app.argument.Field;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.balancer.alpha.cost.NumberOfLeaderCost;
import org.astraea.app.balancer.alpha.cost.ReplicaDiskInCost;
import org.astraea.app.balancer.alpha.cost.ReplicaMigrationSpeedCost;
import org.astraea.app.balancer.alpha.cost.ReplicaSizeCost;
import org.astraea.app.balancer.alpha.cost.TopicPartitionDistributionCost;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.executor.StraightPlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.generator.ShufflePlanGenerator;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.common.DataSize;
import org.astraea.app.common.DataUnit;
import org.astraea.app.common.Utils;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.CostFunction;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.cost.HasPartitionCost;
import org.astraea.app.cost.PartitionCost;

public class Balancer implements Runnable {

  private final Argument argument;
  private final Thread balancerThread;
  private final Map<Integer, JMXServiceURL> jmxServiceURLMap;
  private final MetricCollector metricCollector;
  private final Set<CostFunction> registeredCostFunction;
  private final ScheduledExecutorService scheduledExecutorService;
  private final RebalancePlanGenerator rebalancePlanGenerator;
  private final Admin topicAdmin;
  private final RebalancePlanExecutor rebalancePlanExecutor;
  private final Set<String> topicIgnoreList;

  public Balancer(Argument argument) {
    // initialize member variables
    this.argument = argument;
    this.jmxServiceURLMap = argument.jmxServiceURLMap;
    this.registeredCostFunction =
        Set.of(
            new ReplicaDiskInCost(argument.brokerBandwidthCap),
            new NumberOfLeaderCost(),
            new TopicPartitionDistributionCost(),
            new ReplicaSizeCost(argument.totalBrokerCapacity),
            new ReplicaMigrationSpeedCost());
    this.scheduledExecutorService = Executors.newScheduledThreadPool(8);

    // initialize main component
    this.balancerThread = new Thread(this);
    this.metricCollector =
        new MetricCollector(
            this.jmxServiceURLMap,
            this.registeredCostFunction.stream()
                .map(CostFunction::fetcher)
                .collect(Collectors.toUnmodifiableList()),
            this.scheduledExecutorService,
            argument);
    this.topicAdmin = Admin.of(argument.configs());
    this.rebalancePlanGenerator = new ShufflePlanGenerator(3, 8);
    this.rebalancePlanExecutor = new StraightPlanExecutor();

    this.topicIgnoreList = BalancerUtils.privateTopics(this.topicAdmin);
  }

  public void start() {
    balancerThread.start();
  }

  public void run() {
    this.metricCollector.start();

    System.out.println("Ignored topics: " + topicIgnoreList);

    while (!Thread.interrupted()) {
      try {
        work();
      } catch (Exception exception) {
        System.out.println("Failed to calculate cost functions, skip this iteration");
        exception.printStackTrace();
      }
      try {
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        break;
      }
    }
  }

  private void work() throws Exception {
    // generate cluster info
    final var clusterInfo =
        ClusterInfo.of(
            BalancerUtils.clusterSnapShot(topicAdmin, topicIgnoreList),
            metricCollector.fetchMetrics());
    final var currentAllocation = LayeredClusterLogAllocation.of(clusterInfo);

    // friendly info
    if (clusterInfo.topics().isEmpty()) {
      System.out.println("No topic in this cluster, there is nothing to do.");
      return;
    }

    // warm up the metrics, each broker must have at least a certain amount of metrics collected.
    attemptWarmUpMetrics(argument.metricWarmUpCount);

    // dump metrics into cost function
    final var brokerCosts =
        registeredCostFunction.parallelStream()
            .filter(costFunction -> costFunction instanceof HasBrokerCost)
            .map(costFunction -> (HasBrokerCost) costFunction)
            .map(costFunction -> Map.entry(costFunction, costFunction.brokerCost(clusterInfo)))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    final var topicPartitionCosts =
        registeredCostFunction.parallelStream()
            .filter(costFunction -> costFunction instanceof HasPartitionCost)
            .map(costFunction -> (HasPartitionCost) costFunction)
            .map(costFunction -> Map.entry(costFunction, costFunction.partitionCost(clusterInfo)))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    // print out current score
    System.out.println("[Cost of Current Cluster]");
    BalancerUtils.printBrokerCost(brokerCosts);
    BalancerUtils.printPartitionCost(topicPartitionCosts, clusterInfo.nodes());

    final var rankedProposal =
        new TreeSet<ScoredProposal>(Comparator.comparingDouble(x -> x.score));

    final AtomicInteger progress = new AtomicInteger();
    final int iteration = 1000;
    final var watcherTask =
        scheduledExecutorService.schedule(
            BalancerUtils.generationWatcher(iteration, progress), 0, TimeUnit.SECONDS);
    final var proposalStream = rebalancePlanGenerator.generate(clusterInfo).iterator();
    for (int i = 0; i < iteration; i++) {
      final var proposal = proposalStream.next();
      final var proposedClusterInfo = BalancerUtils.clusterInfoFromProposal(clusterInfo, proposal);

      final var proposedBrokerCosts =
          registeredCostFunction.parallelStream()
              .filter(costFunction -> costFunction instanceof HasBrokerCost)
              .map(costFunction -> (HasBrokerCost) costFunction)
              .map(
                  costFunction ->
                      Map.entry(costFunction, costFunction.brokerCost(proposedClusterInfo)))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      final var proposedTopicPartitionCosts =
          registeredCostFunction.parallelStream()
              .filter(costFunction -> costFunction instanceof HasPartitionCost)
              .map(costFunction -> (HasPartitionCost) costFunction)
              .map(
                  costFunction ->
                      Map.entry(costFunction, costFunction.partitionCost(proposedClusterInfo)))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

      final var estimatedCostSum =
          costSum(
              proposal.rebalancePlan().get(),
              currentAllocation,
              proposedBrokerCosts,
              proposedTopicPartitionCosts);

      rankedProposal.add(
          new ScoredProposal(
              estimatedCostSum, proposedBrokerCosts, proposedTopicPartitionCosts, proposal));
      while (rankedProposal.size() > 5) rankedProposal.pollLast();

      progress.incrementAndGet();
    }
    watcherTask.cancel(true);

    final var selectedProposal = rankedProposal.first();
    final var currentCostSum =
        costSum(
            selectedProposal.proposal.rebalancePlan().get(),
            currentAllocation,
            brokerCosts,
            topicPartitionCosts);
    final var proposedCostSum = selectedProposal.score;
    if (proposedCostSum < currentCostSum) {
      System.out.println("[New Proposal Found]");
      System.out.println("Current cost sum: " + currentCostSum);
      System.out.println("Proposed cost sum: " + proposedCostSum);
      BalancerUtils.describeProposal(
          selectedProposal.proposal, LayeredClusterLogAllocation.of(clusterInfo));
      System.out.println("[Detail of the cost of current Proposal]");
      BalancerUtils.printBrokerCost(selectedProposal.brokerCosts);
      BalancerUtils.printPartitionCost(selectedProposal.partitionCosts, clusterInfo.nodes());

      System.out.println("[Balance Execution Started]");
      if (rebalancePlanExecutor != null) {
        // TODO: fix this
        rebalancePlanExecutor.run(null);
        Utils.packException(
            () -> {
              TimeUnit.SECONDS.sleep(60);
              return 0;
            });
      }
    } else {
      System.out.println("[No Usable Proposal Found]");
      System.out.println("Current cost sum: " + currentCostSum);
      System.out.println("Best proposed cost sum calculated: " + proposedCostSum);
    }
  }

  private void attemptWarmUpMetrics(int requiredFetch) throws InterruptedException {
    Supplier<Boolean> isWarmed = () -> metricCollector.fetchCount() >= requiredFetch;
    Supplier<Double> warpUpProgress =
        () -> Math.min(((double) metricCollector.fetchCount()) / requiredFetch, 1);

    while (!isWarmed.get()) {
      System.out.printf(
          "Attempts to warm up metric (%.2f%%/100.00%%)%n", warpUpProgress.get() * 100.0);
      TimeUnit.SECONDS.sleep(3);
      if (isWarmed.get()) {
        System.out.printf(
            "Attempts to warm up metric (%.2f%%/100.00%%)%n", warpUpProgress.get() * 100.0);
      }
    }
  }

  /**
   * Given a final score for this all the cost function results, the value will be a non-negative
   * real number. Basically, 0 means the most ideal state given all the cost functions.
   */
  private double costSum(
      ClusterLogAllocation proposedCluster,
      ClusterLogAllocation originalCluster,
      Map<HasBrokerCost, BrokerCost> costOfProposal,
      Map<HasPartitionCost, PartitionCost> costOfProposal2) {
    final BrokerCost replicaDiskInCost =
        costOfProposal.entrySet().stream()
            .filter(x -> x.getKey().getClass() == ReplicaDiskInCost.class)
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow();
    final BrokerCost leaderCountCost =
        costOfProposal.entrySet().stream()
            .filter(x -> x.getKey().getClass() == NumberOfLeaderCost.class)
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow();
    final BrokerCost topicPartitionDistributionCost =
        costOfProposal.entrySet().stream()
            .filter(x -> x.getKey().getClass() == TopicPartitionDistributionCost.class)
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow();
    final PartitionCost replicaMigrateCost =
        costOfProposal2.entrySet().stream()
            .filter(x -> x.getKey().getClass() == ReplicaSizeCost.class)
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow();

    // replicaMigrationCost
    final var topicPartitionCopyMap =
        BalancerUtils.diffAllocation(proposedCluster, originalCluster);
    // TODO: this cost doesn't consider data folder?
    final Map<Integer, Double> brokerMigrationCost =
        topicPartitionCopyMap.entrySet().stream()
            .flatMap(
                entry -> entry.getValue().stream().map(x -> Map.entry(x.broker(), entry.getKey())))
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(Map.Entry::getValue, Collectors.toUnmodifiableList())))
            .entrySet()
            .stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().stream()
                            .mapToDouble(tp -> replicaMigrateCost.value(tp.topic()).get(tp))
                            .sum()))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    final var totalMigrationSize = brokerMigrationCost.values().stream().mapToDouble(x -> x).sum();

    // how many hours does it take to move all the log, for each broker, find the maximum possible
    // time
    final var dataRateOfOneBrokerForMigration =
        argument.affordableMigrationBandwidth.dataRate(ChronoUnit.SECONDS);
    final var staticDataMigrationCost =
        brokerMigrationCost.values().stream()
            .map(BigDecimal::valueOf)
            .map(
                x ->
                    x.divide(
                        dataRateOfOneBrokerForMigration.toBigDecimal(
                            DataUnit.Byte, Duration.ofHours(1)),
                        MathContext.DECIMAL32))
            .max(BigDecimal::compareTo)
            .map(BigDecimal::doubleValue)
            .orElse(0.0);
    final var totalMovingCount = topicPartitionCopyMap.values().stream().mapToInt(List::size).sum();

    // replicaDiskInCost
    final var covOfDiskIn = BalancerUtils.coefficientOfVariance(replicaDiskInCost.value().values());

    // leaderCountCost
    final var covOfLeader = BalancerUtils.coefficientOfVariance(leaderCountCost.value().values());

    // topicPartitionDistributionCost
    final var covOfTopicPartition =
        BalancerUtils.coefficientOfVariance(topicPartitionDistributionCost.value().values());

    return (covOfDiskIn * 3 + covOfTopicPartition * 0 + covOfLeader * 0);
  }

  public void stop() {
    this.metricCollector.close();
    this.scheduledExecutorService.shutdownNow();
  }

  public static void main(String[] args) throws InterruptedException {
    final Argument argument = org.astraea.app.argument.Argument.parse(new Argument(), args);
    final Balancer balancer = new Balancer(argument);
    balancer.start();
    balancer.balancerThread.join();
    balancer.stop();
  }

  static class Argument extends org.astraea.app.argument.Argument {

    @Parameter(
        names = {"--jmx.server.file"},
        description =
            "Path to a java properties file that contains all the jmxServiceUrl definitions and their corresponding broker.id",
        converter = JmxServiceUrlMappingFileField.class,
        required = true)
    Map<Integer, JMXServiceURL> jmxServiceURLMap;

    @Parameter(
        names = {"--broker.bandwidthCap.file"},
        description =
            "Path to a java properties file that contains all the bandwidth upper limit(MB/s) and their corresponding broker.id",
        converter = BrokerBandwidthCapMapField.class,
        required = true)
    Map<Integer, Integer> brokerBandwidthCap;

    @Parameter(
        names = {"--broker.capacity.file"},
        description =
            "Path to a java properties file that contains all the total hard disk space(MB) and their corresponding log path",
        converter = BrokerBandwidthCapMapField.class,
        required = true)
    Map<Integer, Integer> totalBrokerCapacity;

    @Parameter(
        names = {"--metrics-scraping-interval"},
        description = "The time interval between metric fetching",
        converter = DurationField.class)
    Duration metricScrapingInterval = Duration.ofSeconds(1);

    @Parameter(
        names = {"--metric-warm-up"},
        description = "Ensure the balance have fetched a certain amount of metrics before continue")
    int metricWarmUpCount = 5;

    @Parameter(
        names = {"--affordable-migration-bandwidth"},
        description = "The bandwidth threshold for a affordable migration",
        converter = DataSize.Field.class)
    DataSize affordableMigrationBandwidth = DataUnit.MiB.of(50000);

    public static class JmxServiceUrlMappingFileField extends Field<Map<Integer, JMXServiceURL>> {
      static final Pattern serviceUrlKeyPattern =
          Pattern.compile("broker\\.(?<brokerId>[1-9][0-9]{0,9})");

      static Map.Entry<Integer, JMXServiceURL> transformEntry(Map.Entry<String, String> entry) {
        final Matcher matcher = serviceUrlKeyPattern.matcher(entry.getKey());
        if (matcher.matches()) {
          try {
            int brokerId = Integer.parseInt(matcher.group("brokerId"));
            final JMXServiceURL jmxServiceURL = new JMXServiceURL(entry.getValue());
            return Map.entry(brokerId, jmxServiceURL);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
          } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                "Bad JmxServiceURL format for " + entry.getValue(), e);
          }
        } else {
          throw new IllegalArgumentException(
              "Bad key format for "
                  + entry.getKey()
                  + " no match for the following format :"
                  + serviceUrlKeyPattern.pattern());
        }
      }

      @Override
      public Map<Integer, JMXServiceURL> convert(String value) {
        final Properties properties = new Properties();

        try (var reader = Files.newBufferedReader(Path.of(value))) {
          properties.load(reader);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        return properties.entrySet().stream()
            .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
            .map(
                entry -> {
                  try {
                    return transformEntry(entry);
                  } catch (Exception e) {
                    throw new IllegalArgumentException(
                        "Failed to process JMX service URL map:" + value, e);
                  }
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    }

    static class BrokerBandwidthCapMapField extends Field<Map<Integer, Integer>> {
      static final Pattern serviceUrlKeyPattern =
          Pattern.compile("broker\\.(?<brokerId>[1-9][0-9]{0,9})");

      static Map.Entry<Integer, Integer> transformEntry(Map.Entry<String, String> entry) {
        final Matcher matcher = serviceUrlKeyPattern.matcher(entry.getKey());
        if (matcher.matches()) {
          try {
            int brokerId = Integer.parseInt(matcher.group("brokerId"));
            return Map.entry(brokerId, Integer.parseInt(entry.getValue()));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
          }
        } else {
          throw new IllegalArgumentException(
              "Bad key format for "
                  + entry.getKey()
                  + " no match for the following format :"
                  + serviceUrlKeyPattern.pattern());
        }
      }

      @Override
      public Map<Integer, Integer> convert(String value) {
        final Properties properties = new Properties();

        try (var reader = Files.newBufferedReader(Path.of(value))) {
          properties.load(reader);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return properties.entrySet().stream()
            .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
            .map(BrokerBandwidthCapMapField::transformEntry)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    }

    static class FolderCapacityMapField extends Field<Map<String, Integer>> {
      static Map.Entry<String, Integer> transformEntry(Map.Entry<String, String> entry) {
        try {
          return Map.entry(entry.getKey(), Integer.parseInt(entry.getValue()));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Bad integer format for " + entry.getKey(), e);
        }
      }

      @Override
      public Map<String, Integer> convert(String value) {
        final Properties properties = new Properties();

        try (var reader = Files.newBufferedReader(Path.of(value))) {
          properties.load(reader);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return properties.entrySet().stream()
            .map(entry -> Map.entry((String) entry.getKey(), (String) entry.getValue()))
            .map(FolderCapacityMapField::transformEntry)
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    }
  }

  private static class ScoredProposal {
    private final double score;
    private final RebalancePlanProposal proposal;
    private final Map<HasBrokerCost, BrokerCost> brokerCosts;
    private final Map<HasPartitionCost, PartitionCost> partitionCosts;

    public ScoredProposal(
        double estimatedCostSum,
        Map<HasBrokerCost, BrokerCost> proposedBrokerCosts,
        Map<HasPartitionCost, PartitionCost> proposedTopicPartitionCosts,
        RebalancePlanProposal proposal) {
      this.score = estimatedCostSum;
      this.brokerCosts = proposedBrokerCosts;
      this.partitionCosts = proposedTopicPartitionCosts;
      this.proposal = proposal;
    }
  }
}