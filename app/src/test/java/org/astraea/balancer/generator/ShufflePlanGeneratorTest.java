package org.astraea.balancer.generator;

import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.balancer.alpha.LogPlacement;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.NodeInfo;
import org.astraea.cost.ReplicaInfo;
import org.astraea.cost.TopicPartition;
import org.astraea.metrics.HasBeanObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ShufflePlanGeneratorTest {

  ClusterInfo fakeClusterInfo(int nodeCount, int topicCount, int partitionCount, int replicaCount) {
    final var random = new Random();
    random.setSeed(0);

    return fakeClusterInfo(
        nodeCount,
        topicCount,
        partitionCount,
        replicaCount,
        (partition) ->
            IntStream.range(0, partition)
                .mapToObj(
                    (ignore) -> {
                      byte[] topicSuffix = new byte[6];
                      random.nextBytes(topicSuffix);
                      return "fake-topic-"
                          + Base64.getEncoder()
                              .encodeToString(topicSuffix)
                              .replace("+", "_")
                              .replace("/", ".")
                              .replace("=", "-");
                    })
                .collect(Collectors.toUnmodifiableSet()));
  }

  ClusterInfo fakeClusterInfo(
      int nodeCount,
      int topicCount,
      int partitionCount,
      int replicaCount,
      Function<Integer, Set<String>> topicNameGenerator) {
    final var nodes =
        IntStream.range(0, nodeCount)
            .mapToObj(nodeId -> NodeInfo.of(nodeId, "host" + nodeId, 9092))
            .collect(Collectors.toUnmodifiableList());
    final var topics = topicNameGenerator.apply(topicCount);
    final var replicas =
        topics.stream()
            .flatMap(
                topic ->
                    IntStream.range(0, partitionCount).mapToObj(p -> TopicPartition.of(topic, p)))
            .flatMap(
                tp ->
                    IntStream.range(0, replicaCount)
                        .mapToObj(
                            r ->
                                ReplicaInfo.of(
                                    tp.topic(), tp.partition(), nodes.get(r), r == 0, true, false)))
            .collect(Collectors.groupingBy(ReplicaInfo::topic));

    return new ClusterInfo() {
      @Override
      public List<NodeInfo> nodes() {
        return nodes;
      }

      @Override
      public Set<String> dataDirectories(int brokerId) {
        // TODO: fix this
        return Set.of();
      }

      @Override
      public List<ReplicaInfo> availablePartitionLeaders(String topic) {
        return partitions(topic).stream()
            .filter(ReplicaInfo::isLeader)
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public List<ReplicaInfo> availablePartitions(String topic) {
        return partitions(topic);
      }

      @Override
      public Set<String> topics() {
        return topics;
      }

      @Override
      public List<ReplicaInfo> partitions(String topic) {
        return replicas.get(topic);
      }

      @Override
      public Collection<HasBeanObject> beans(int brokerId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<Integer, Collection<HasBeanObject>> allBeans() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Test
  void testRun() {
    final var shufflePlanGenerator = new ShufflePlanGenerator(5, 10);
    final var fakeCluster = fakeClusterInfo(5, 10, 10, 3);
    final var stream = shufflePlanGenerator.generate(fakeCluster);
    final var iterator = stream.iterator();

    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
    Assertions.assertDoesNotThrow(() -> System.out.println(iterator.next()));
  }

  @Test
  void testMovement() {
    final var fakeClusterInfo =
        fakeClusterInfo(10, 3, 10, 1, (ignore) -> Set.of("breaking-news", "chess", "animal"));
    final var shuffleCount = 1;
    final var shuffleSourceTopicPartition = TopicPartition.of("breaking-news", 0);
    final var shuffleSourceLog = LogPlacement.of(0);
    final var shuffleDestinationBroker = 9;
    final var shufflePlanGenerator =
        new ShufflePlanGenerator(() -> shuffleCount) {
          @Override
          int sourceTopicPartitionSelector(List<TopicPartition> migrationCandidates) {
            return super.sourceTopicPartitionSelector(migrationCandidates);
          }

          @Override
          int sourceLogPlacementSelector(List<LogPlacement> migrationCandidates) {
            return super.sourceLogPlacementSelector(migrationCandidates);
          }

          @Override
          int migrationSelector(List<ShufflePlanGenerator.Movement> movementCandidates) {
            return super.migrationSelector(movementCandidates);
          }
        };
  }

  @Test
  void test() {
    System.out.println(fakeClusterInfo(10, 3, 3, 3).topics());
  }
}
