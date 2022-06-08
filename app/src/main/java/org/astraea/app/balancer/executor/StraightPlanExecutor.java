/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.balancer.executor;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  @Override
  public RebalanceExecutionResult run(RebalanceExecutionContext context) {
    try {
      final var clusterInfo = context.rebalanceAdmin().clusterInfo();
      final var currentLogAllocation = LayeredClusterLogAllocation.of(clusterInfo);
      final var migrationTargets =
          ClusterLogAllocation.findNonFulfilledAllocation(
              context.expectedAllocation(), currentLogAllocation);

      if (!migrationTargets.isEmpty()) {
        // migrate everything
        migrationTargets.forEach(
            topicPartition ->
                context
                    .rebalanceAdmin()
                    .alterReplicaPlacements(
                        topicPartition,
                        context.expectedAllocation().logPlacements(topicPartition)));

        // wait until all target is done
        context.rebalanceAdmin().waitLogSynced(migrationTargets, ChronoUnit.DAYS.getDuration());

        // perform leader election
        migrationTargets.forEach(tp -> context.rebalanceAdmin().leaderElection(tp));

        // wait until the leader election done
        final var expected =
            context
                .expectedAllocation()
                .topicPartitionStream()
                .map(tp -> Map.entry(tp, context.expectedAllocation().logPlacements(tp)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        context.rebalanceAdmin().waitLeaderSynced(expected, ChronoUnit.DAYS.getDuration());
      }
      return RebalanceExecutionResult.done();
    } catch (Exception e) {
      return RebalanceExecutionResult.failed(e);
    }
  }
}
