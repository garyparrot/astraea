package org.astraea.balancer.log;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.log.migration.LogMigration;

/**
 * Describe the log allocation state of a Kafka cluster. The implementation have to keep the cluster
 * log allocation information, provide method for query the placement, and offer a set of log
 * placement change operation.
 */
public interface ClusterLogAllocation {

  /** let specific broker leave the replica set and let another broker join the replica set. */
  void migrateReplica(TopicPartition topicPartition, int atBroker, int toBroker);

  /** let specific follower log become the leader log of this topic/partition. */
  void letReplicaBecomeLeader(TopicPartition topicPartition, int followerReplica);

  /** change the data directory of specific log */
  void changeDataDirectory(TopicPartition topicPartition, int atBroker, String newPath);

  /** Retrieve the log placements of specific {@link TopicPartition}. */
  List<LogPlacement> logPlacements(TopicPartition topicPartition);

  /** Retrieve the stream of all topic/partition pairs in allocation. */
  Stream<TopicPartition> topicPartitionStream();

  /**
   * Return the migration steps to make the source log allocation become the target log allocation
   */
  static Map<TopicPartition, LogMigration> migrationSets(ClusterLogAllocation from, ClusterLogAllocation to) {
    final var fromPartitionSet= from.topicPartitionStream().collect(Collectors.toUnmodifiableSet());

    if(!to.topicPartitionStream().allMatch(fromPartitionSet::contains)) {
      // target partition set must be the superset of source partition set.
      throw new IllegalArgumentException("target log allocation is not a superset of source log allocation, is partition disappeared?");
    }

    return fromPartitionSet.stream()
            .collect(Collectors.toUnmodifiableMap(
                    tp -> tp,
                    tp -> {
                      final var sourcePlacement = from.logPlacements(tp);
                      final var destinationPlacement  = to.logPlacements(tp);
                      return null;
                    }
            ));
  }
}
