package org.astraea.balancer.executor;

import org.astraea.balancer.log.ClusterLogAllocation;

/** The states and variables related to current rebalance execution */
public interface RebalanceExecutionContext {

  /** The migration interface */
  RebalanceAdmin rebalanceAdmin();

  /** The original log allocation used to generate the rebalance plan */
  ClusterLogAllocation initialAllocation();

  /** The expected log allocation after the rebalance execution */
  ClusterLogAllocation expectedAllocation();

  /** Retrieve specific configuration of Balancer */
  String balancerConfig(String configName);
}
