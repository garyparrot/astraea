package org.astraea.app;

import org.astraea.common.Configuration;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.admin.TopicPartition;
import org.astraea.common.balancer.AlgorithmConfig;
import org.astraea.common.balancer.Balancer;
import org.astraea.common.balancer.algorithms.GreedyBalancer;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.cost.HasMoveCost;
import org.astraea.common.cost.NetworkCost;
import org.astraea.common.cost.NetworkEgressCost;
import org.astraea.common.cost.NetworkIngressCost;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ThesisTest {

  @Test
  void testRun() throws InterruptedException {
    var exe = Executors.newFixedThreadPool(10);
    var counting = new CountDownLatch(100);

    for(int i = 0; i < 100; i++) {
      var ii = i;
      exe.submit(() -> {
        test(new Random(ii));
        counting.countDown();
      });
    }

    counting.await();
    exe.shutdownNow();
  }

  void test(Random rand) {
    var gen = BalancingProblemGenerator.generate(300, rand);

    var cluster = ClusterInfo.builder()
        .addNode(Set.copyOf(gen.brokerIds))
        .addFolders(gen.brokerIds.stream()
            .collect(Collectors.toUnmodifiableMap(
                x -> x,
                x -> Set.of("/folder")
            )))
        .build();
    var actualCluster = ClusterInfo.of("", cluster.brokers(), Map.of(),
        gen.replicaPosition.entrySet()
            .stream()
            .flatMap(e -> {
              var topic = e.getKey().substring(0, e.getKey().lastIndexOf('-'));
              var partition = e.getKey().substring(e.getKey().lastIndexOf('-') + 1);
              var in = gen.partitionNetIn.get(e.getKey());
              var pid = Integer.parseInt(partition);

              return Stream.concat(
                  e.getValue().stream().limit(1)
                      .map(id -> Replica.builder()
                          .topic(topic)
                          .partition(pid)
                          .brokerId(id)
                          .path("/folder")
                          .isLeader(true)
                          .isPreferredLeader(true)
                          .size(in * 10)
                          .build()),
                  e.getValue().stream().skip(1)
                      .map(id -> Replica.builder()
                          .topic(topic)
                          .partition(pid)
                          .brokerId(id)
                          .path("/folder")
                          .isLeader(false)
                          .isPreferredLeader(false)
                          .size(in * 10)
                          .build()));
            })
            .toList());

    var cb = ClusterBean.EMPTY;
    var costIngress = new NetworkIngressCost(Configuration.EMPTY);
    var costEgress = new NetworkEgressCost(Configuration.EMPTY);
    costIngress.calculationCache.put(cb,
        costIngress.enclosing(
            gen.partitionNetIn.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                    e -> TopicPartition.of(e.getKey()),
                    e -> e.getValue() * 1000)),
            gen.partitionNetOut.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                    e -> TopicPartition.of(e.getKey()),
                    e -> e.getValue() * 1000))));
    costEgress.calculationCache.put(cb,
        costEgress.enclosing(
            gen.partitionNetIn.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                    e -> TopicPartition.of(e.getKey()),
                    e -> e.getValue() * 1000)),
            gen.partitionNetOut.entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                    e -> TopicPartition.of(e.getKey()),
                    e -> e.getValue() * 1000))));

    var balancer = new GreedyBalancer();

    Optional<Balancer.Plan> offer = balancer.offer(AlgorithmConfig.builder()
        .clusterInfo(actualCluster)
        .clusterBean(cb)
        .clusterCost(HasClusterCost.of(
            Map.ofEntries(
                Map.entry(costIngress, 2.0),
                Map.entry(costEgress, 1.0))))
        .moveCost(HasMoveCost.EMPTY)
        .timeout(Duration.ofSeconds(180))
        .build());

    ClusterCost clusterCost = offer.get().proposalClusterCost();
    var ingressSummary = clusterCost.expose()
        .entrySet()
        .stream()
        .filter(e -> e.getKey() instanceof NetworkIngressCost)
        .map(e -> (NetworkCost.NetworkClusterCost) e.getValue())
        .findFirst()
        .orElseThrow()
        .brokerRate
        .values()
        .stream()
        .mapToLong(x -> x)
        .summaryStatistics();
    var egressSummary = clusterCost.expose()
        .entrySet()
        .stream()
        .filter(e -> e.getKey() instanceof NetworkEgressCost)
        .map(e -> (NetworkCost.NetworkClusterCost) e.getValue())
        .findFirst()
        .orElseThrow()
        .brokerRate
        .values()
        .stream()
        .mapToLong(x -> x)
        .summaryStatistics();

    var inMaxMin = ingressSummary.getMax() - ingressSummary.getMin();
    var outMaxMin = egressSummary.getMax() - egressSummary.getMin();
    System.out.println(inMaxMin + " " + outMaxMin);
  }
}
