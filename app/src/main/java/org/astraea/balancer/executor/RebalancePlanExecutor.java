package org.astraea.balancer.executor;

import org.astraea.balancer.RebalancePlanProposal;

/**
 * This class associate with the logic of fulfill given rebalance plan. This process can take a
 * period of time. Once the {@link RebalancePlanExecutor#run(RebalancePlanProposal)} finished
 * normally, the given rebalance plan is considered fulfilled.
 */
public interface RebalancePlanExecutor {

  void run(RebalancePlanProposal proposal);
}
