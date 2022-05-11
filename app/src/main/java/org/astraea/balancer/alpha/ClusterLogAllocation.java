package org.astraea.balancer.alpha;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.ReplicaInfo;
import org.astraea.cost.TopicPartition;

public class ClusterLogAllocation {

  private final Map<TopicPartition, List<LogPlacement>> allocation;

  private ClusterLogAllocation(Map<TopicPartition, List<LogPlacement>> allocation) {
    this.allocation = Map.copyOf(allocation);
  }

  public static ClusterLogAllocation of(Map<TopicPartition, List<LogPlacement>> allocation) {
    return new ClusterLogAllocation(allocation);
  }

  public Map<TopicPartition, List<LogPlacement>> allocation() {
    return allocation;
  }

  // TODO: add a method to calculate the difference between two ClusterLogAllocation
  public static ClusterLogAllocation of(ClusterInfo clusterInfo) {
    final var allocation =
        clusterInfo.topics().stream()
            .map(clusterInfo::partitions)
            .flatMap(Collection::stream)
            .collect(
                Collectors.groupingBy(
                    replica -> TopicPartition.of(replica.topic(), replica.partition())))
            .entrySet()
            .stream()
            .map(
                (entry) -> {
                  // validate if the given log placements are valid
                  if (entry.getValue().stream().filter(ReplicaInfo::isLeader).count() != 1)
                    throw new IllegalArgumentException(
                        "The " + entry.getKey() + " leader count mismatch 1.");

                  final var topicPartition = entry.getKey();
                  final var logPlacements =
                      entry.getValue().stream()
                          .sorted(Comparator.comparingInt(replica -> replica.isLeader() ? 0 : 1))
                          .map(
                              replica ->
                                  LogPlacement.of(
                                      replica.nodeInfo().id(), replica.dataFolder().orElse(null)))
                          .collect(Collectors.toUnmodifiableList());

                  return Map.entry(topicPartition, logPlacements);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    return ClusterLogAllocation.of(allocation);
  }
}
