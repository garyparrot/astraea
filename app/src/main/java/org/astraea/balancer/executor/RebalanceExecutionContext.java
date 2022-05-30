package org.astraea.balancer.executor;

import org.astraea.balancer.log.ClusterLogAllocation;

/** The states and variables related to current rebalance execution */
public interface RebalanceExecutionContext {

  /** The migration interface */
  RebalanceAdmin rebalanceAdmin();

  /** The expected log allocation after the rebalance execution */
  ClusterLogAllocation expectedAllocation();

  // TODO: add a method to fetch balancer related configuration
}
