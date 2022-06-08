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
package org.astraea.app.balancer.executor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.producer.Producer;
import org.astraea.app.service.RequireBrokerCluster;
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

            // the log contain metadata and record content, it supposed to be bigger than the actual
            // data
            Assertions.assertTrue(size < syncingProgress.get(tp).get(0).logSize());
            Assertions.assertTrue(size < syncingProgress.get(tp).get(1).logSize());

            Assertions.assertTrue(size < syncingProgress.get(tp).get(0).leaderLogSize());
            Assertions.assertTrue(size < syncingProgress.get(tp).get(1).leaderLogSize());
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
