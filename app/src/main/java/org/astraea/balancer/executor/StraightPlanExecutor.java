package org.astraea.balancer.executor;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.balancer.log.LayeredClusterLogAllocation;
import org.astraea.balancer.log.LogPlacement;
import org.astraea.common.Utils;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  @Override
  public RebalanceExecutionResult run(RebalanceExecutionContext context) {
    try {
      final var clusterInfo = context.rebalanceAdmin().clusterInfo();
      final var currentLogAllocation = LayeredClusterLogAllocation.of(clusterInfo);
      final var nonFulfilledTopicPartitions =
          ClusterLogAllocation.findNonFulfilledAllocation(
              context.expectedAllocation(), currentLogAllocation);

      if (!nonFulfilledTopicPartitions.isEmpty()) {
        nonFulfilledTopicPartitions.forEach(
            topicPartition -> {
              final var expectedPlacements =
                  context.expectedAllocation().logPlacements(topicPartition);
              context.rebalanceAdmin().alterReplicaPlacements(topicPartition, expectedPlacements);
            });

        Supplier<Boolean> copyWorkDone =
            () ->
                context
                    .rebalanceAdmin()
                    .syncingProgress(nonFulfilledTopicPartitions)
                    .entrySet()
                    .stream()
                    .flatMap(list -> list.getValue().stream())
                    .allMatch(SyncingProgress::synced);
        Supplier<Boolean> allocationMatch =
            () -> {
              final var alloc =
                  LayeredClusterLogAllocation.of(context.rebalanceAdmin().clusterInfo());

              return context
                  .expectedAllocation()
                  .topicPartitionStream()
                  .allMatch(
                      tp ->
                          LogPlacement.isMatch(
                              context.expectedAllocation().logPlacements(tp),
                              alloc.logPlacements(tp)));
            };

        while (!(copyWorkDone.get() && allocationMatch.get())) {
          Utils.packException(() -> TimeUnit.SECONDS.sleep(3));
        }
      }
      return RebalanceExecutionResult.done();
    } catch (Exception e) {
      return RebalanceExecutionResult.failed(e);
    }
  }
}
