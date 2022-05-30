package org.astraea.balancer.executor;

import java.util.List;
import org.astraea.metrics.collector.Fetcher;

/**
 * This class associate with the logic of fulfill given rebalance plan.
 */
public interface RebalancePlanExecutor {

  /**
   * This method responsible for fulfill a rebalance plan.
   */
  RebalanceExecutionResult run(RebalanceExecutionContext executionContext);

  /**
   * @return the metric fetcher of this executor requested. An executor can take advantage of
   *     metrics to monitor cluster state, perform appropriate migration decision.
   */
  default Fetcher fetcher() {
    return Fetcher.of(List.of());
  }
}
