package org.astraea.balancer.executor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.admin.Admin;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.log.LayeredClusterLogAllocation;
import org.astraea.balancer.log.LogPlacement;
import org.astraea.common.Utils;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StraightPlanExecutorTest extends RequireBrokerCluster {

  @Test
  void testRun() {
    // arrange
    try (Admin admin = Admin.of(bootstrapServers())) {
      final var topicName = "StraightPlanExecutorTest_" + Utils.randomString(8);
      admin.creator().topic(topicName).numberOfPartitions(10).numberOfReplicas((short) 2).create();
      final var broker0 = 0;
      final var broker1 = 1;
      final var logFolder0 = logFolders().get(broker0).stream().findAny().orElseThrow();
      final var logFolder1 = logFolders().get(broker1).stream().findAny().orElseThrow();
      final var onlyPlacement =
          List.of(LogPlacement.of(broker0, logFolder0), LogPlacement.of(broker1, logFolder1));
      final var allocationMap =
          IntStream.range(0, 10)
              .mapToObj(i -> new TopicPartition(topicName, i))
              .collect(Collectors.toUnmodifiableMap(tp -> tp, tp -> onlyPlacement));
      final var expectedAllocation = LayeredClusterLogAllocation.of(allocationMap);
      final var expectedTopicPartition =
          expectedAllocation.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
      final var rebalanceAdmin = RebalanceAdmin.of(admin, Map::of, (ignore) -> true);
      final var context = RebalanceExecutionContext.of(rebalanceAdmin, expectedAllocation);

      // act
      final var result = new StraightPlanExecutor().run(context);

      // assert
      final var currentAllocation =
          LayeredClusterLogAllocation.of(admin.clusterInfo(Set.of(topicName)));
      final var currentTopicPartition =
          currentAllocation.topicPartitionStream().collect(Collectors.toUnmodifiableSet());
      Assertions.assertTrue(result.isDone(), () -> {
        result.exception().orElseThrow().printStackTrace();
        return result.exception().toString();
      });
      Assertions.assertEquals(expectedTopicPartition, currentTopicPartition);
      expectedTopicPartition.forEach(
          topicPartition ->
              Assertions.assertEquals(
                  expectedAllocation.logPlacements(topicPartition),
                  currentAllocation.logPlacements(topicPartition),
                  "Testing for " + topicPartition));
    }
  }
}
