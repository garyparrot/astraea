package org.astraea.balancer.executor;

import org.astraea.balancer.log.ClusterLogAllocation;

/** The states and variables related to current rebalance execution */
public interface RebalanceExecutionContext {

  static RebalanceExecutionContext of(
      RebalanceAdmin rebalanceAdmin, ClusterLogAllocation expectedAllocation) {
    return new RebalanceExecutionContext() {
      @Override
      public RebalanceAdmin rebalanceAdmin() {
        return rebalanceAdmin;
      }

      @Override
      public ClusterLogAllocation expectedAllocation() {
        return expectedAllocation;
      }
    };
  }

  /** The migration interface */
  RebalanceAdmin rebalanceAdmin();

  /** The expected log allocation after the rebalance execution */
  ClusterLogAllocation expectedAllocation();

  // TODO: add a method to fetch balancer related configuration
}
