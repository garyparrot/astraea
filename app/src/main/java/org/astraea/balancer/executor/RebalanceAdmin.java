package org.astraea.balancer.executor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.astraea.admin.Admin;
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

      private List<LogPlacement> fetchCurrentPlacement(TopicPartition topicPartition) {
        // TODO: we only need the info related to the topic/partition, but current Admin
        // implementation force us to fetch everything
        return admin.replicas(Set.of(topicPartition.topic())).get(topicPartition).stream()
            .map(replica -> LogPlacement.of(replica.broker(), replica.path()))
            .collect(Collectors.toUnmodifiableList());
      }

      private void declarePreferredLogDirectory(
          TopicPartition topicPartition,
          List<LogPlacement> currentPlacement,
          List<LogPlacement> preferredPlacement) {
        final var currentBrokerAllocation =
            currentPlacement.stream()
                .map(LogPlacement::broker)
                .collect(Collectors.toUnmodifiableSet());

        // TODO: this operation is not supposed to trigger a log movement. But there might be a
        // small window of time to actually trigger it (race condition).
        final var declareMap =
            preferredPlacement.stream()
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
        final var currentLogAllocation = fetchCurrentPlacement(topicPartition);

        // ensure replica will be placed in the correct data directory at destination broker.
        declarePreferredLogDirectory(topicPartition, currentLogAllocation, expectedPlacement);

        // TODO: are we supposed to declare these operations in async way?
        // do cross broker migration
        new Thread(
                () ->
                    admin
                        .migrator()
                        .partition(topicPartition.topic(), topicPartition.partition())
                        .moveTo(
                            expectedPlacement.stream()
                                .map(LogPlacement::broker)
                                .collect(Collectors.toUnmodifiableList())))
            .start();
        // do inter-data-directories migration
        new Thread(
                () ->
                    admin
                        .migrator()
                        .partition(topicPartition.topic(), topicPartition.partition())
                        .moveTo(
                            expectedPlacement.stream()
                                .filter(placement -> placement.logDirectory().isPresent())
                                .collect(
                                    Collectors.toUnmodifiableMap(
                                        LogPlacement::broker,
                                        x -> x.logDirectory().orElseThrow()))))
            .start();
        // TODO: do leader election

        // TODO: made this method return a watch to watch over the migration progress.
      }

      @Override
      public ClusterInfo clusterInfo() {
        return ClusterInfo.of(ClusterInfo.of(admin, topicFilter), metricSource.get());
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

  ClusterInfo clusterInfo();

  ClusterInfo refreshMetrics(ClusterInfo oldClusterInfo);
}
