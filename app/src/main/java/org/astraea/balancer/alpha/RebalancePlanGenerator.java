package org.astraea.balancer.alpha;

import org.astraea.cost.ClusterInfo;

@FunctionalInterface
public interface RebalancePlanGenerator {

  RebalancePlanProposal generate(ClusterInfo clusterNow);
}
