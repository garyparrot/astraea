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
  public RebalanceExecutionResult run(RebalanceExecutionContext context) {

    return RebalanceExecutionResult.done();
  }
}
