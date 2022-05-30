package org.astraea.balancer.executor;

import org.astraea.balancer.generator.ShufflePlanGenerator;
import org.astraea.balancer.log.ClusterLogAllocation;
import org.astraea.cost.ClusterInfoProvider;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StraightPlanExecutorTest extends RequireBrokerCluster {

  @Test
  void doMigration() {
    final var generator = new ShufflePlanGenerator(5, 10);
    final var clusterInfoNow = ClusterInfoProvider.fakeClusterInfo(10, 10, 10, 3);
    final var allocationExpected =
        generator.generate(clusterInfoNow).findFirst().orElseThrow().rebalancePlan().orElseThrow();
    final var straightPlanExecutor = new StraightPlanExecutor();
    final var mockedAdmin = Mockito.mock(RebalanceAdmin.class);
    Mockito.when(mockedAdmin.clusterInfo()).thenReturn(clusterInfoNow);

    final var mockedContext =
        new RebalanceExecutionContext() {
          @Override
          public RebalanceAdmin rebalanceAdmin() {
            return mockedAdmin;
          }

          @Override
          public ClusterLogAllocation expectedAllocation() {
            return allocationExpected;
          }
        };

    final var result = straightPlanExecutor.run(mockedContext);

    Assertions.assertTrue(result.isDone());
  }
}
