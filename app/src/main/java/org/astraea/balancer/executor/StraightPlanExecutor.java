package org.astraea.balancer.executor;

import java.util.concurrent.TimeUnit;
import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.balancer.log.LayeredClusterLogAllocation;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  @Override
  public RebalanceExecutionResult run(RebalanceExecutionContext context) {
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

      boolean workDone;
      do {
        workDone =
            context
                .rebalanceAdmin()
                .syncingProgress(nonFulfilledTopicPartitions)
                .entrySet()
                .stream()
                .flatMap(list -> list.getValue().stream())
                .allMatch(SyncingProgress::synced);

        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          return RebalanceExecutionResult.failed(e);
        }
      } while (!workDone);
    }

    return RebalanceExecutionResult.done();
  }
}
