package org.astraea.balancer.executor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.admin.Admin;
import org.astraea.admin.TopicPartition;
import org.astraea.common.Utils;
import org.astraea.metrics.HasBeanObject;
import org.astraea.producer.Producer;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RebalanceAdminTest extends RequireBrokerCluster {

  @Test
  void alterReplicaPlacements() {}

  @Test
  void syncingProgress() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);

      final var name = "RebalanceAdminTest" + Utils.randomString(6);
      admin.creator().topic(name).numberOfPartitions(3).numberOfReplicas((short) 2).create();
      IntStream.range(0, 3)
          .forEach(
              partition ->
                  Utils.packException(
                      () -> {
                        try (var producer = Producer.of(bootstrapServers())) {
                          producer
                              .sender()
                              .topic(name)
                              .partition(partition)
                              .value(new byte[1024 * (partition + 1)])
                              .run()
                              .toCompletableFuture()
                              .get();
                        }
                      }));

      final var tps =
          IntStream.range(0, 3)
              .mapToObj(x -> new TopicPartition(name, x))
              .collect(Collectors.toUnmodifiableSet());
      final var replicas = admin.replicas(Set.of(name));

      final var syncingProgress = rebalanceAdmin.syncingProgress(tps);
      Assertions.assertEquals(tps, syncingProgress.keySet());
      tps.forEach(tp -> Assertions.assertEquals(2, syncingProgress.get(tp).size()));
      tps.forEach(
          tp -> {
            Assertions.assertEquals(tp, syncingProgress.get(tp).get(0).topicPartition());
            Assertions.assertEquals(tp, syncingProgress.get(tp).get(1).topicPartition());

            Assertions.assertEquals(
                replicas.get(tp).get(0).broker(), syncingProgress.get(tp).get(0).brokerId());
            Assertions.assertEquals(
                replicas.get(tp).get(1).broker(), syncingProgress.get(tp).get(1).brokerId());

            Assertions.assertTrue(syncingProgress.get(tp).get(0).synced());
            Assertions.assertTrue(syncingProgress.get(tp).get(1).synced());

            long size = 1024L * (tp.partition()) + 1;

            Assertions.assertEquals(size, syncingProgress.get(tp).get(0).logSize());
            Assertions.assertEquals(size, syncingProgress.get(tp).get(1).logSize());

            Assertions.assertEquals(size, syncingProgress.get(tp).get(0).leaderLogSize());
            Assertions.assertEquals(size, syncingProgress.get(tp).get(1).leaderLogSize());
          });
    }
  }

  @Test
  void clusterInfo() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);
      final var clusterInfo = rebalanceAdmin.clusterInfo();
      Assertions.assertEquals(admin.topicNames(), clusterInfo.topics());

      final var name = "RebalanceAdminTest" + Utils.randomString(6);
      admin.creator().topic(name).numberOfPartitions(3).create();
      final var rebalanceAdmin1 = RebalanceAdmin.of(admin, Map::of, name::equals);
      final var clusterInfo1 = rebalanceAdmin1.clusterInfo();
      Assertions.assertEquals(Set.of(name), clusterInfo1.topics());
    }
  }

  @Test
  void refreshMetrics() {
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var next = new AtomicInteger();
      Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource =
          () -> Map.of(next.get(), List.of());

      final var rebalanceAdmin = RebalanceAdmin.of(admin, metricSource, (ignore) -> true);
      final var clusterInfo = rebalanceAdmin.refreshMetrics(rebalanceAdmin.clusterInfo());

      Assertions.assertEquals(List.of(), clusterInfo.beans(0));
      next.incrementAndGet();
      Assertions.assertEquals(List.of(), clusterInfo.beans(1));
      next.incrementAndGet();
      Assertions.assertEquals(List.of(), clusterInfo.beans(2));
    }
  }
}
