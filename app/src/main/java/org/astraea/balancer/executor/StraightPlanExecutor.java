package org.astraea.balancer.executor;

import java.util.stream.Collectors;
import org.astraea.admin.ReplicaSyncingMonitor;
import org.astraea.admin.TopicPartition;
import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.balancer.log.LayeredClusterLogAllocation;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  @Override
  public RebalanceExecutionResult run(RebalanceExecutionContext context) {

    do {
      final var clusterInfo = context.rebalanceAdmin().clusterInfo();
      final var currentLogAllocation = LayeredClusterLogAllocation.of(clusterInfo);
      final var nonFulfilledTopicPartitions =
          ClusterLogAllocation.findNonFulfilledAllocation(
              context.expectedAllocation(), currentLogAllocation);

      // no more work to do
      if (nonFulfilledTopicPartitions.isEmpty()) break;

      nonFulfilledTopicPartitions.forEach(
          topicPartition -> {
            final var expectedPlacements =
                context.expectedAllocation().logPlacements(topicPartition);
            context.rebalanceAdmin().alterReplicaPlacements(topicPartition, expectedPlacements);
          });

      // TODO: replace the replica syncing monitor usage after the RebalanceAdmin support migration
      // watch
      ReplicaSyncingMonitor.main(
          new String[] {
            "--topics",
            nonFulfilledTopicPartitions.stream()
                .map(TopicPartition::topic)
                .distinct()
                .collect(Collectors.joining(","))
          });

    } while (true);

    return RebalanceExecutionResult.done();
  }
}
