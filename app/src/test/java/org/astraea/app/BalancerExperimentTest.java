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
package org.astraea.app;

import java.awt.image.BandCombineOp;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.astraea.balancer.bench.BalancerBenchmark;
import org.astraea.common.ByteUtils;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.BrokerConfigs;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.ResourceBalancer;
import org.astraea.common.balancer.executor.StraightPlanExecutor;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.NetworkEgressCost;
import org.astraea.common.cost.NetworkIngressCost;
import org.astraea.common.cost.NoSufficientMetricsException;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.ClusterBean;
import org.astraea.common.metrics.ClusterBeanSerializer;
import org.astraea.common.metrics.ClusterInfoSerializer;
import org.astraea.common.metrics.JndiClient;
import org.astraea.common.metrics.collector.MetricSensor;
import org.astraea.common.metrics.collector.MetricStore;
import org.astraea.it.Service;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import scala.concurrent.impl.FutureConvertersImpl;

public class BalancerExperimentTest {

  record Benchmark(String clusterInfo, String clusterBean, String originalAfter) {}

  Benchmark bench0 = new Benchmark("/home/garyparrot/cluster-file-new-1.bin", "/home/garyparrot/bean-file-new-1.bin", "");
  Benchmark clusterScale4to6 = new Benchmark(
      "/home/garyparrot/clusters/cluster-info-before5711282531669254849.bin",
      "/home/garyparrot/clusters/cluster-bean10583960853500379886.bin",
      "/home/garyparrot/clusters/cluster-info-after13687119735971491405.bin");

  Benchmark funnyScale = new Benchmark(
      "/home/garyparrot/clusters/fe99-cluster-info-before-1829908888049525671.bin",
      "/home/garyparrot/clusters/fe99-cluster-bean-15032755707454245272.bin",
      "/home/garyparrot/clusters/fe99-cluster-info-after-15377270052954832616.bin");

  Benchmark imbalanceFollowers = new Benchmark(
      "/home/garyparrot/clusters/preserved/003-imbalance-with-followers-before.bin",
      "/home/garyparrot/clusters/preserved/003-imbalance-with-followers-bean.bin",
      "/home/garyparrot/clusters/preserved/003-imbalance-with-followers-after.bin");

  Benchmark thesisExp2greedy = new Benchmark(
      "/home/garyparrot/Programming/ncku-thesis-template-latex/thesis/context/performance/experiments/exp2-cluster-info-before.bin",
      "/home/garyparrot/Programming/ncku-thesis-template-latex/thesis/context/performance/experiments/exp2-cluster-bean.bin",
      "/home/garyparrot/Programming/ncku-thesis-template-latex/thesis/context/performance/experiments/exp2-cluster-info-after-greedy.bin");

  String fileName0 = "";
  String fileName1 = "";

  public static final String realCluster =
      "192.168.103.177:25655,192.168.103.178:25655,192.168.103.179:25655,192.168.103.180:25655,192.168.103.181:25655,192.168.103.182:25655";

  public static void main(String[] args) {
    new BalancerExperimentTest().testProfiling();
  }

  @Disabled
  @Test
  void testProfiling() {
    // load
    var usedBench = bench0;
    try (var admin = Admin.of(realCluster);
        var stream0 = new FileInputStream(usedBench.clusterInfo);
        var stream1 = new FileInputStream(usedBench.clusterBean)) {
      // ClusterInfo clusterInfo =
      //     admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();

      Map<HasClusterCost, Double> costMap =
          Map.of(
              // new ReplicaLeaderCost(Configuration.EMPTY), 3.0,
              new NetworkIngressCost(Configuration.EMPTY), 3.0,
              new NetworkEgressCost(Configuration.EMPTY), 3.0);
      HasMoveCost moveCost = HasMoveCost.EMPTY;
         //  new ReplicaLeaderCost(
         //   new Configuration(Map.of(ReplicaLeaderCost.MAX_MIGRATE_LEADER_KEY, "100")));
      var costFunction = HasClusterCost.of(costMap);

      System.out.println("Serialize ClusterInfo");
      ClusterInfo clusterInfo = ByteUtils.readClusterInfo(stream0.readAllBytes());
      System.out.println("Serialize ClusterBean");
      ClusterBean clusterBean = asClusterBean(costMap.keySet(), ByteUtils.readBeanObjects(stream1.readAllBytes()));
      System.out.println("Done!");


      var balancer = new ResourceBalancer();
      var result =
          BalancerBenchmark.costProfiling()
              .setClusterInfo(clusterInfo)
              .setClusterBean(clusterBean)
              .setBalancer(balancer)
              .setExecutionTimeout(Duration.ofSeconds(120))
              .setAlgorithmConfig(
                  AlgorithmConfig.builder().clusterCost(costFunction).moveCost(moveCost).build())
              .start()
              .toCompletableFuture()
              .join();

      var meanClusterCostTime =
          Duration.ofNanos((long) result.clusterCostProcessingTimeNs().getAverage());
      var meanMoveCostTime =
          Duration.ofNanos((long) result.moveCostProcessingTimeNs().getAverage());
      System.out.println("Total Run time: " + result.executionTime().toMillis() + " ms");
      System.out.println(
          "Total ClusterCost Evaluation: " + result.clusterCostProcessingTimeNs().getCount());
      System.out.println(
          "Average ClusterCost Processing: " + meanClusterCostTime.toMillis() + "ms");
      System.out.println("Average MoveCost Processing: " + meanMoveCostTime.toMillis() + "ms");
      System.out.println("Initial Cost: " + result.initial());
      System.out.println(
          "Final Cost: " + result.plan().map(Balancer.Plan::proposalClusterCost).orElse(null));
      var profilingFile = Utils.packException(() -> Files.createTempFile("profile-", ".csv"));
      System.out.println("Profiling File: " + profilingFile.toString());
      System.out.println(
          "Total affected partitions: "
              + ClusterInfo.findNonFulfilledAllocation(
                      clusterInfo, result.plan().orElseThrow().proposal())
                  .size());
      System.out.println();
      try (var stream = Files.newBufferedWriter(profilingFile)) {
        var start = result.costTimeSeries().keySet().stream().mapToLong(x -> x).min().orElseThrow();
        Utils.packException(() -> stream.write("time, cost\n"));
        result.costTimeSeries().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(
                (e) -> {
                  var time = e.getKey();
                  var cost = e.getValue();
                  Utils.packException(
                      () -> stream.write(String.format("%d, %.7f%n", time - start, cost.value())));
                });
      } catch (IOException e) {
        e.printStackTrace();
      }

      System.out.println("Run the plan? (yes/no)");
      while (true) {
        var scanner = new Scanner(System.in);
        String next = scanner.next();
        if (next.equals("yes")) {
          System.out.println("Run the Plan");
          new StraightPlanExecutor(Configuration.EMPTY)
              .run(admin, result.plan().orElseThrow().proposal(), Duration.ofHours(1))
              .toCompletableFuture()
              .join();
          return;
        } else if (next.equals("no")) {
          return;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Disabled
  @Test
  void testSaveScenario() {
    try (Admin admin = Admin.of(realCluster)) {
      var clusterInfo =
          admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();
      Map<HasClusterCost, Double> costMap =
          Map.of(
              new NetworkIngressCost(Configuration.EMPTY), 3.0,
              new NetworkEgressCost(Configuration.EMPTY), 3.0,
              new ReplicaNumberCost(Configuration.EMPTY), 1.0);

      try (var metricStore =
          MetricStore.builder()
              .beanExpiration(Duration.ofSeconds(180))
              .sensorsSupplier(
                  () ->
                      costMap.keySet().stream()
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  CostFunction::metricSensor, x -> (i0, i1) -> {})))
              .receivers(
                  List.of(
                      MetricStore.Receiver.local(
                          () ->
                              admin
                                  .brokers()
                                  .thenApply(
                                      (brokers) ->
                                          brokers.stream()
                                              .collect(
                                                  Collectors.toUnmodifiableMap(
                                                      Broker::id,
                                                      (Broker b) ->
                                                          JndiClient.of(b.host(), 16926)))))))
              .build()) {
        var clusterBean = (ClusterBean) null;
        var balancer = new GreedyBalancer();

        while (!Thread.currentThread().isInterrupted()) {
          clusterBean = metricStore.clusterBean();

          System.out.println(
              clusterBean.all().entrySet().stream()
                  .collect(
                      Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> x.getValue().size())));
          try {
            var costFunction = HasClusterCost.of(costMap);
            Optional<Balancer.Plan> offer =
                balancer.offer(
                    AlgorithmConfig.builder()
                        .clusterInfo(clusterInfo)
                        .clusterBean(clusterBean)
                        .clusterCost(costFunction)
                        .timeout(Duration.ofSeconds(10))
                        .build());
            if (offer.isPresent()) {
              System.out.println("Find one");
              break;
            }
          } catch (NoSufficientMetricsException e) {
            System.out.println("No Plan, try later: " + e.getMessage());
            Utils.sleep(Duration.ofSeconds(3));
          }
        }

        // save
        try (var stream0 = new FileOutputStream(fileName0);
            var stream1 = new FileOutputStream(fileName1)) {
          System.out.println("Serialize ClusterInfo");
          stream0.write(ByteUtils.toBytes(clusterInfo));
          System.out.println("Serialize ClusterBean");
          stream1.write(ByteUtils.toBytes(clusterBean.all()));
        } catch (IOException e) {
          e.printStackTrace();
        }

        // load
        try (var stream0 = new FileInputStream(fileName0);
            var stream1 = new FileInputStream(fileName1)) {
          System.out.println("Serialize ClusterInfo");
          ClusterInfo a = ByteUtils.readClusterInfo(stream0.readAllBytes());
          System.out.println("Serialize ClusterBean");
          var receiver = MetricStore.Receiver.fixed(ByteUtils.readBeanObjects(stream1.readAllBytes())
              .entrySet()
              .stream()
              .collect(Collectors.toUnmodifiableMap(
                  Map.Entry::getKey,
                  x -> (Collection<BeanObject>) x.getValue())));
          var store = MetricStore.builder()
              .sensorsSupplier(() ->
                  costMap.keySet().stream()
                      .collect(
                          Collectors.toUnmodifiableMap(
                              CostFunction::metricSensor, x -> (i0, i1) -> {
                              })))
              .receivers(List.of(receiver))
              .beanExpiration(Duration.ofDays(3))
              .build();
          store.wait(cb -> !cb.all().isEmpty(), Duration.ofSeconds(10));
          ClusterBean clusterBean1 = store.clusterBean();

          System.out.println("Done!");
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  ClusterBean testUseableClusterBean(HasClusterCost costFunction, HasMoveCost moveCost) {
    try (Admin admin = Admin.of(realCluster)) {
      var clusterInfo =
          admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();

      var sensors = MetricSensor.of(List.of(costFunction.metricSensor(), moveCost.metricSensor()));

      try (var metricStore =
          MetricStore.builder()
              .receivers(
                  List.of(
                      MetricStore.Receiver.local(
                          () ->
                              admin
                                  .brokers()
                                  .thenApply(
                                      (brokers) ->
                                          brokers.stream()
                                              .collect(
                                                  Collectors.toUnmodifiableMap(
                                                      Broker::id,
                                                      (Broker b) ->
                                                          JndiClient.of(b.host(), 16926)))))))
              .beanExpiration(Duration.ofSeconds(180))
              .sensorsSupplier(() -> Map.of(sensors, (x, y) -> {}))
              .build()) {
        var clusterBean = (ClusterBean) null;
        var balancer = new GreedyBalancer();

        while (!Thread.currentThread().isInterrupted()) {
          clusterBean = metricStore.clusterBean();

          System.out.println(
              clusterBean.all().entrySet().stream()
                  .collect(
                      Collectors.toUnmodifiableMap(Map.Entry::getKey, x -> x.getValue().size())));
          try {
            Optional<Balancer.Plan> offer =
                balancer.offer(
                    AlgorithmConfig.builder()
                        .clusterInfo(clusterInfo)
                        .clusterBean(clusterBean)
                        .clusterCost(costFunction)
                        .moveCost(moveCost)
                        .timeout(Duration.ofSeconds(10))
                        .build());
            if (offer.isPresent()) {
              System.out.println("Find one");
              break;
            }
          } catch (NoSufficientMetricsException e) {
            System.out.println("No Plan, try later: " + e.getMessage());
            Utils.sleep(Duration.ofSeconds(3));
          }
        }

        return clusterBean;
      }
    }
  }

  @Disabled
  @Test
  void testSerialization() {
    try (var service =
        Service.builder()
            .numberOfBrokers(3)
            .brokerConfigs(Map.of(BrokerConfigs.LOG_RETENTION_BYTES_CONFIG, "5566"))
            .build()) {
      try (var admin = Admin.of(realCluster)) {
        var clusterInfo =
            admin.topicNames(false).thenCompose(admin::clusterInfo).toCompletableFuture().join();

        byte[] serialized = ByteUtils.toBytes(clusterInfo);
        ClusterInfo deserialized = ByteUtils.readClusterInfo(serialized);

        Assertions.assertEquals(clusterInfo.topics().keySet(), deserialized.topics().keySet());
        for (String topic : clusterInfo.topics().keySet()) {
          Assertions.assertEquals(
              clusterInfo.topics().get(topic).topicPartitions(),
              deserialized.topics().get(topic).topicPartitions());
          Assertions.assertEquals(
              clusterInfo.topics().get(topic).config().raw(),
              deserialized.topics().get(topic).config().raw());
        }
        Assertions.assertEquals(clusterInfo.replicas().size(), deserialized.replicas().size());
        for (TopicPartition topicPartition : clusterInfo.topicPartitions()) {
          Assertions.assertEquals(
              Set.copyOf(clusterInfo.replicas(topicPartition)),
              Set.copyOf(deserialized.replicas(topicPartition)));
        }
      }
    }
  }

  static ClusterBean asClusterBean(Set<? extends CostFunction> sensors, Map<Integer, List<BeanObject>> beans) {
    var receiver = MetricStore.Receiver.fixed(beans.entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(
            Map.Entry::getKey,
            x -> (Collection<BeanObject>) x.getValue())));
    try (MetricStore build = MetricStore.builder()
        .receivers(List.of(receiver))
        .beanExpiration(Duration.ofDays(3))
        .sensorsSupplier(() -> sensors.stream()
            .map(CostFunction::metricSensor)
            .collect(Collectors.toUnmodifiableMap(
                x -> x,
                x -> (a, b) -> {})))
        .build()) {
      build.wait(Predicate.not(x -> x.all().isEmpty()), Duration.ofSeconds(3));
      return build.clusterBean();
    }
  }
}
