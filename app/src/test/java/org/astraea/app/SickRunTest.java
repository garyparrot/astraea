package org.astraea.app;

import org.astraea.balancer.bench.BalancerBenchmark;
import org.astraea.common.ByteUtils;
import org.astraea.common.Configuration;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.balancer.algorithms.ResourceBalancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.NetworkEgressCost;
import org.astraea.common.cost.NetworkIngressCost;
import org.astraea.common.cost.RecordSizeCost;
import org.astraea.common.cost.ReplicaLeaderCost;
import org.astraea.common.cost.ReplicaNumberCost;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SickRunTest {

  BalancerExperimentTest.Benchmark bench0 = new BalancerExperimentTest.Benchmark("/home/garyparrot/cluster-file-new-1.bin", "/home/garyparrot/bean-file-new-1.bin", "");

  BalancerExperimentTest.Benchmark imbalanceFollowers = new BalancerExperimentTest.Benchmark(
      "/home/garyparrot/clusters/preserved/003-imbalance-with-followers-before.bin",
      "/home/garyparrot/clusters/preserved/003-imbalance-with-followers-bean.bin",
      "/home/garyparrot/clusters/preserved/003-imbalance-with-followers-after.bin");

  BalancerExperimentTest.Benchmark thesisExp2greedy = new BalancerExperimentTest.Benchmark(
      "/home/garyparrot/Programming/ncku-thesis-template-latex/thesis/context/performance/experiments/exp2-cluster-info-before-greedy.bin",
      "/home/garyparrot/Programming/ncku-thesis-template-latex/thesis/context/performance/experiments/exp2-cluster-bean-greedy.bin",
      "/home/garyparrot/Programming/ncku-thesis-template-latex/thesis/context/performance/experiments/exp2-cluster-info-after-greedy.bin");
  @Test
  void testOne() {
    var usedBench = imbalanceFollowers;
    try (var stream0 = new FileInputStream(usedBench.clusterInfo());
         var stream1 = new FileInputStream(usedBench.clusterBean())) {

      Map<HasClusterCost, Double> costMap =
          Map.of(
              new NetworkIngressCost(Configuration.EMPTY), 3.0,
              new NetworkEgressCost(Configuration.EMPTY), 3.0);
      // HasMoveCost moveCost = HasMoveCost.EMPTY;
      HasMoveCost moveCost =
          HasMoveCost.of(List.of(
              new RecordSizeCost(new Configuration(Map.of(RecordSizeCost.MAX_MIGRATE_SIZE_KEY, "1GB")))));
      var costFunction = HasClusterCost.of(costMap);

      System.out.println("Serialize ClusterInfo");
      ClusterInfo clusterInfo = ByteUtils.readClusterInfo(stream0.readAllBytes());
      System.out.println("Serialize ClusterBean");
      ClusterBean clusterBean = BalancerExperimentTest.asClusterBean(costMap.keySet(), ByteUtils.readBeanObjects(stream1.readAllBytes()));
      System.out.println("Done!");

      var balancer = new ResourceBalancer();
      var result =
          BalancerBenchmark.costProfiling()
              .setClusterInfo(clusterInfo)
              .setClusterBean(clusterBean)
              .setBalancer(balancer)
              .setExecutionTimeout(Duration.ofSeconds(60))
              .setAlgorithmConfig(
                  AlgorithmConfig.builder().clusterCost(costFunction).moveCost(moveCost).build())
              .start()
              .toCompletableFuture()
              .join();

      Map<HasClusterCost, ClusterCost> expose = result.plan().map(x -> x.proposalClusterCost().expose()).orElse(Map.of());

      synchronized (SickRunTest.class) {
        ClusterCost clusterCost0 = expose.entrySet().stream().filter(x -> x.getKey() instanceof NetworkIngressCost).map(x -> x.getValue()).findFirst().orElseThrow();
        ClusterCost clusterCost1 = expose.entrySet().stream().filter(x -> x.getKey() instanceof NetworkEgressCost).map(x -> x.getValue()).findFirst().orElseThrow();
        // ClusterCost clusterCost2 = expose.entrySet().stream().filter(x -> x.getKey() instanceof ReplicaNumberCost).map(x -> x.getValue()).findFirst().orElseThrow();
        System.out.println();
        System.out.println();
        System.out.printf("%.6f, %.6f, %s%n%n", clusterCost0.value(), clusterCost1.value(), result.plan().get().proposalClusterCost());
        // System.out.printf("%.6f, %.6f, %.6f, %s%n%n", clusterCost0.value(), clusterCost1.value(), clusterCost2.value(), result.plan().get().proposalClusterCost());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

    @Test
  void test() {
    var usedBench = imbalanceFollowers;
    try (var stream0 = new FileInputStream(usedBench.clusterInfo());
         var stream1 = new FileInputStream(usedBench.clusterBean())) {

      Map<HasClusterCost, Double> costMap =
          Map.of(
              new ReplicaNumberCost(Configuration.EMPTY), 3.0,
              new NetworkIngressCost(Configuration.EMPTY), 3.0,
              new NetworkEgressCost(Configuration.EMPTY), 3.0);
      // HasMoveCost moveCost = HasMoveCost.EMPTY;
      HasMoveCost moveCost = new ReplicaLeaderCost(
           new Configuration(Map.of(ReplicaLeaderCost.MAX_MIGRATE_LEADER_KEY, "100")));
      var costFunction = HasClusterCost.of(costMap);

      System.out.println("Serialize ClusterInfo");
      ClusterInfo clusterInfo = ByteUtils.readClusterInfo(stream0.readAllBytes());
      System.out.println("Serialize ClusterBean");
      ClusterBean clusterBean = BalancerExperimentTest.asClusterBean(costMap.keySet(), ByteUtils.readBeanObjects(stream1.readAllBytes()));
      System.out.println("Done!");

      Runnable run = () -> {
        var balancer = new GreedyBalancer();
        var result =
            BalancerBenchmark.costProfiling()
                .setClusterInfo(clusterInfo)
                .setClusterBean(clusterBean)
                .setBalancer(balancer)
                .setExecutionTimeout(Duration.ofSeconds(60))
                .setAlgorithmConfig(
                    AlgorithmConfig.builder().clusterCost(costFunction).moveCost(moveCost).build())
                .start()
                .toCompletableFuture()
                .join();

        Map<HasClusterCost, ClusterCost> expose = result.plan().map(x -> x.proposalClusterCost().expose()).orElse(Map.of());

        System.out.println(result.plan().get().proposalClusterCost().value());
        expose.forEach((hcc, cc) -> {
          if(hcc instanceof NetworkIngressCost)
            System.out.println("IngressScore:" + cc.value());
          else if(hcc instanceof NetworkEgressCost)
            System.out.println("EgressScore:" + cc.value());
        });

        synchronized (SickRunTest.class) {
          try (var writer = new BufferedWriter(new FileWriter("/home/garyparrot/greedy-3cost-limit-60sec", true))) {
            ClusterCost clusterCost0 = expose.entrySet().stream().filter(x -> x.getKey() instanceof NetworkIngressCost).map(x -> x.getValue()).findFirst().orElseThrow();
            ClusterCost clusterCost1 = expose.entrySet().stream().filter(x -> x.getKey() instanceof NetworkEgressCost).map(x -> x.getValue()).findFirst().orElseThrow();
            ClusterCost clusterCost2 = expose.entrySet().stream().filter(x -> x.getKey() instanceof ReplicaNumberCost).map(x -> x.getValue()).findFirst().orElseThrow();
            writer.write("%.6f, %.6f, %.6f, %s%n".formatted(clusterCost0.value(), clusterCost1.value(), clusterCost2.value(),
                result.plan().get().proposalClusterCost()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };

      var exe = Executors.newFixedThreadPool(10);

      for(int i = 0;i < 50; i++)
        exe.submit(run);

      exe.awaitTermination(1000000, TimeUnit.DAYS);

    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
