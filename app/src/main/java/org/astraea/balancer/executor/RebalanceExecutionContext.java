package org.astraea.balancer.executor;

import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.balancer.log.migration.LogMigration;

import java.util.Set;

/**
 * The states and variables related to current rebalance execution
 */
public interface RebalanceExecutionContext {

    /**
     * The log allocation before start the rebalance execution
     */
    ClusterLogAllocation initialAllocation();

    /**
     * The expected log allocation after the rebalance execution
     */
    ClusterLogAllocation expectedAllocation();

    /**
     * Set of rebalance operations supposed to execute.
     */
    default Set<LogMigration> migrations() {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieve specific configuration of Balancer
     */
    String balancerConfig(String configName);

    /**
     * Retrieve the configuration of specific topic
     */
    String topicConfig(String topic, String configName);

    /**
     * Retrieve the configuration of specific broker
     */
    String brokerConfig(int broker, String configName);

}