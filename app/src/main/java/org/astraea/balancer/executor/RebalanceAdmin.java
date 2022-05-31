package org.astraea.balancer.executor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.admin.Admin;
import org.astraea.admin.Replica;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.log.LogPlacement;
import org.astraea.cost.ClusterInfo;
import org.astraea.metrics.HasBeanObject;

/**
 * The wrapper of {@link Admin}. Offer only the essential functionalities & some utilities to
 * perform rebalance operation.
 */
public interface RebalanceAdmin {

  static RebalanceAdmin of(
      Admin admin,
      Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource,
      Predicate<String> topicFilter) {
    return new RebalanceAdmin() {

      private void ensureTopicPermitted(TopicPartition topicPartition) {
        if (!topicFilter.test(topicPartition.topic()))
          throw new IllegalArgumentException(
              "Operation to topic/partition " + topicPartition + "is not permitted");
      }

      private List<LogPlacement> fetchCurrentPlacement(TopicPartition topicPartition) {
        ensureTopicPermitted(topicPartition);
        // TODO: we only need the info related to the topic/partition, but current Admin
        // implementation force us to fetch everything
        return admin.replicas(Set.of(topicPartition.topic())).get(topicPartition).stream()
            .map(replica -> LogPlacement.of(replica.broker(), replica.path()))
            .collect(Collectors.toUnmodifiableList());
      }

      /**
       * Declare the preferred data directory at certain brokers.
       *
       * <p>By default, upon a new log creation with JBOD enabled broker. Kafka broker will pick up
       * a data directory that has the fewest log maintained to be the data directory for the new
       * log. This method declares the preferred data directory for a specific topic/partition on
       * the certain broker. Upon the new log creation for the specific topic/partition on the
       * certain broker. The preferred data directory will be used as the data directory for the new
       * log, which replaces the default approach. This gives you the control to decide which data
       * directory the replica log you are about to migrate will be.
       *
       * @param topicPartition the topic/partition to declare preferred data directory
       * @param preferredPlacements the replica placements with their desired data directory at
       *     certain brokers
       */
      private void declarePreferredDataDirectories(
          TopicPartition topicPartition, List<LogPlacement> preferredPlacements) {
        ensureTopicPermitted(topicPartition);

        final var currentPlacement = fetchCurrentPlacement(topicPartition);

        final var currentBrokerAllocation =
            currentPlacement.stream()
                .map(LogPlacement::broker)
                .collect(Collectors.toUnmodifiableSet());

        // TODO: this operation is not supposed to trigger a log movement. But there might be a
        // small window of time to actually trigger it (race condition).
        final var declareMap =
            preferredPlacements.stream()
                .filter(
                    futurePlacement -> !currentBrokerAllocation.contains(futurePlacement.broker()))
                .filter(futurePlacement -> futurePlacement.logDirectory().isPresent())
                .collect(
                    Collectors.toUnmodifiableMap(
                        LogPlacement::broker, x -> x.logDirectory().orElseThrow()));

        admin
            .migrator()
            .partition(topicPartition.topic(), topicPartition.partition())
            .moveTo(declareMap);
      }

      @Override
      public void alterReplicaPlacements(
          TopicPartition topicPartition, List<LogPlacement> expectedPlacement) {
        ensureTopicPermitted(topicPartition);

        // ensure replica will be placed in the correct data directory at destination broker.
        declarePreferredDataDirectories(topicPartition, expectedPlacement);

        // do cross broker migration
        admin
            .migrator()
            .partition(topicPartition.topic(), topicPartition.partition())
            .moveTo(
                expectedPlacement.stream()
                    .map(LogPlacement::broker)
                    .collect(Collectors.toUnmodifiableList()));
        // do inter-data-directories migration
        admin
            .migrator()
            .partition(topicPartition.topic(), topicPartition.partition())
            .moveTo(
                expectedPlacement.stream()
                    .filter(placement -> placement.logDirectory().isPresent())
                    .collect(
                        Collectors.toUnmodifiableMap(
                            LogPlacement::broker, x -> x.logDirectory().orElseThrow())));
        // TODO: do leader election
      }

      @Override
      public Map<TopicPartition, List<SyncingProgress>> syncingProgress(
          Set<TopicPartition> topicPartitions) {
        topicPartitions.forEach(this::ensureTopicPermitted);

        final var topics =
            topicPartitions.stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toUnmodifiableSet());
        final var targetReplicas =
            admin.replicas(topics).entrySet().stream()
                .filter(entry -> topicPartitions.contains(entry.getKey()))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        final var leaderReplica =
            targetReplicas.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream().filter(Replica::leader).findFirst()));

        return targetReplicas.entrySet().stream()
            .map(
                entry -> {
                  final var topicPartition = entry.getKey();
                  final var migrationProgresses =
                      entry.getValue().stream()
                          .map(
                              replica ->
                                  SyncingProgress.of(
                                      topicPartition,
                                      leaderReplica.get(topicPartition).orElseThrow(),
                                      replica))
                          .collect(Collectors.toUnmodifiableList());
                  return Map.entry(topicPartition, migrationProgresses);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public ClusterInfo clusterInfo() {
        return admin.clusterInfo(
            admin.topicNames().stream()
                .filter(topicFilter)
                .collect(Collectors.toUnmodifiableSet()));
      }

      @Override
      public ClusterInfo refreshMetrics(ClusterInfo oldClusterInfo) {
        return ClusterInfo.of(oldClusterInfo, metricSource.get());
      }
    };
  }

  /**
   * Attempt to migrate the target topic/partition to given replica log placement state. This method
   * will perform both replica list migration and data directory migration. This method will return
   * after triggering the migration. It won't wait until the migration processes are fulfilled.
   *
   * @param topicPartition the topic/partition to perform migration
   * @param expectedPlacement the expected placement after this request accomplished
   */
  void alterReplicaPlacements(TopicPartition topicPartition, List<LogPlacement> expectedPlacement);

  /** Access the syncing progress of the specific topic/partitions */
  Map<TopicPartition, List<SyncingProgress>> syncingProgress(Set<TopicPartition> topicPartitions);

  ClusterInfo clusterInfo();

  ClusterInfo refreshMetrics(ClusterInfo oldClusterInfo);

  // TODO: add method to apply reassignment bandwidth throttle.
  // TODO: add method to fetch topic configuration
  // TODO: add method to fetch broker configuration
}
