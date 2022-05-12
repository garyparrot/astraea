package org.astraea.balancer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.astraea.cost.ClusterInfoProvider;
import org.astraea.cost.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ClusterLogAllocationTest {

  @Test
  void of() {
    final ClusterLogAllocation immutable = ClusterLogAllocation.of(Map.of());
    final TopicPartition topicPartition = TopicPartition.of("topic", 0);
    final List<LogPlacement> logPlacements = List.of(LogPlacement.of(0));

    Assertions.assertThrows(
        Exception.class, () -> immutable.allocation().put(topicPartition, logPlacements));
  }

  @Test
  void ofMutable() {
    final ClusterLogAllocation mutable = ClusterLogAllocation.ofMutable(Map.of());
    final TopicPartition topicPartition = TopicPartition.of("topic", 0);
    final List<LogPlacement> logPlacements = List.of(LogPlacement.of(0));

    Assertions.assertDoesNotThrow(() -> mutable.allocation().put(topicPartition, logPlacements));
  }

  @Test
  void migrateReplica() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 1, (i) -> Set.of("topic"));
    final var clusterLogAllocation = ClusterLogAllocation.ofMutable(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", 0);

    clusterLogAllocation.migrateReplica(sourceTopicPartition, 0, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.allocation().get(sourceTopicPartition).get(0).broker());
  }

  @Test
  void letReplicaBecomeLeader() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 2, (i) -> Set.of("topic"));
    final var clusterLogAllocation = ClusterLogAllocation.ofMutable(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", 0);

    clusterLogAllocation.letReplicaBecomeLeader(sourceTopicPartition, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.allocation().get(sourceTopicPartition).get(0).broker());
    Assertions.assertEquals(
        0, clusterLogAllocation.allocation().get(sourceTopicPartition).get(1).broker());
  }

  @Test
  void letLeaderBecomeFollower() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 2, (i) -> Set.of("topic"));
    final var clusterLogAllocation = ClusterLogAllocation.ofMutable(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", 0);

    clusterLogAllocation.letLeaderBecomeFollower(sourceTopicPartition, 1);

    Assertions.assertEquals(
        1, clusterLogAllocation.allocation().get(sourceTopicPartition).get(0).broker());
    Assertions.assertEquals(
        0, clusterLogAllocation.allocation().get(sourceTopicPartition).get(1).broker());
  }

  @Test
  void changeDataDirectory() {
    final var fakeClusterInfo =
        ClusterInfoProvider.fakeClusterInfo(3, 1, 1, 1, (i) -> Set.of("topic"));
    final var clusterLogAllocation = ClusterLogAllocation.ofMutable(fakeClusterInfo);
    final var sourceTopicPartition = TopicPartition.of("topic", 0);

    clusterLogAllocation.changeDataDirectory(sourceTopicPartition, 0, "/path/to/somewhere");

    Assertions.assertEquals(
        "/path/to/somewhere",
        clusterLogAllocation
            .allocation()
            .get(sourceTopicPartition)
            .get(0)
            .logDirectory()
            .orElseThrow());
  }

  @Test
  void testOf() {}
}
