package org.astraea.balancer.executor;

import org.astraea.balancer.log.ClusterLogAllocation;

/** The states and variables related to current rebalance execution */
public interface RebalanceExecutionContext {

  /** The migration interface */
  RebalanceAdmin rebalanceAdmin();

  /** The log allocation before starting the rebalance execution */
  ClusterLogAllocation initialAllocation();

  /** The expected log allocation after the rebalance execution */
  ClusterLogAllocation expectedAllocation();

  /** Retrieve specific configuration of Balancer */
  String balancerConfig(String configName);

  /** Retrieve the configuration of specific topic */
  String topicConfig(String topic, String configName);

  /** Retrieve the configuration of specific broker */
  String brokerConfig(int broker, String configName);
}
