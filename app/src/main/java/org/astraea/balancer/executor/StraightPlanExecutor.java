package org.astraea.balancer.executor;

import java.util.stream.Collectors;
import org.astraea.balancer.alpha.BalancerUtils;
import org.astraea.balancer.alpha.ClusterLogAllocation;
import org.astraea.balancer.alpha.LogPlacement;
import org.astraea.balancer.alpha.RebalancePlanProposal;
import org.astraea.topic.ReplicaSyncingMonitor;
import org.astraea.topic.TopicAdmin;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {
  private final String bootstrapServer;
  private final TopicAdmin topicAdmin;

  public StraightPlanExecutor(String bootstrapServer, TopicAdmin topicAdmin) {
    this.bootstrapServer = bootstrapServer;
    this.topicAdmin = topicAdmin;
  }

  @Override
  public void run(RebalancePlanProposal proposal) {
    if (proposal.rebalancePlan().isEmpty()) return;
    final ClusterLogAllocation clusterNow =
        ClusterLogAllocation.of(BalancerUtils.clusterSnapShot(topicAdmin));
    final ClusterLogAllocation clusterLogAllocation = proposal.rebalancePlan().get();

    clusterLogAllocation
        .allocation()
        .forEach(
            (topicPartition, logPlacements) -> {
              // TODO: Add support for data folder migration
              final var a =
                  clusterNow.allocation().get(topicPartition).stream()
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
