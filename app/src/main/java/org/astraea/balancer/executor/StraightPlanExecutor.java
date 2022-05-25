package org.astraea.balancer.executor;

import java.util.stream.Collectors;
import org.astraea.admin.Admin;
import org.astraea.admin.ReplicaSyncingMonitor;
import org.astraea.balancer.RebalancePlanProposal;
import org.astraea.balancer.alpha.BalancerUtils;
import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.balancer.log.LayeredClusterLogAllocation;
import org.astraea.balancer.log.LogPlacement;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {
  private final String bootstrapServer;
  private final Admin topicAdmin;

  public StraightPlanExecutor(String bootstrapServer, Admin topicAdmin) {
    this.bootstrapServer = bootstrapServer;
    this.topicAdmin = topicAdmin;
  }

  @Override
  public void run(RebalancePlanProposal proposal) {
    if (proposal.rebalancePlan().isEmpty()) return;
    final ClusterLogAllocation clusterNow =
        LayeredClusterLogAllocation.of(BalancerUtils.clusterSnapShot(topicAdmin));
    final org.astraea.balancer.log.ClusterLogAllocation clusterLogAllocation =
        proposal.rebalancePlan().get();

    clusterLogAllocation
        .topicPartitionStream()
        .forEach(
            (topicPartition) -> {
              // TODO: Add support for data folder migration
              final var logPlacements = clusterLogAllocation.logPlacements(topicPartition);
              final var a =
                  clusterNow.logPlacements(topicPartition).stream()
                      .map(LogPlacement::broker)
                      .collect(Collectors.toUnmodifiableList());
              final var b =
                  logPlacements.stream()
                      .map(LogPlacement::broker)
                      .collect(Collectors.toUnmodifiableList());
              if (a.equals(b)) return;
              System.out.printf(
                  "Move %s-%d to %s%n", topicPartition.topic(), topicPartition.partition(), b);
              topicAdmin
                  .migrator()
                  .partition(topicPartition.topic(), topicPartition.partition())
                  .moveTo(b);
            });

    // wait until everything is ok
    System.out.println("Launch Replica Syncing Monitor");
    ReplicaSyncingMonitor.main(new String[] {"--bootstrap.servers", bootstrapServer});
  }
}
