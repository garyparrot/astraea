package org.astraea.balancer.generator;

import java.util.stream.Stream;
import org.astraea.balancer.alpha.RebalancePlanProposal;
import org.astraea.cost.ClusterInfo;

/** */
public interface RebalancePlanGenerator {

  /**
   * Generate a rebalance proposal, noted that this function doesn't require proposing exactly the
   * same plan for the same input argument. There can be some randomization that takes part in this
   * process.
   *
   * @param clusterInfo the cluster state
   * @return a {@link Stream} generating rebalance plan regarding the given {@link ClusterInfo}
   */
  Stream<RebalancePlanProposal> generate(ClusterInfo clusterInfo);
}
