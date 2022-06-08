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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.balancer.log.LayeredClusterLogAllocation;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.common.Utils;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {

  public StraightPlanExecutor() {}

  @Override
  public RebalanceExecutionResult run(RebalanceExecutionContext context) {
    try {
      final var clusterInfo = context.rebalanceAdmin().clusterInfo();
      final var currentLogAllocation = LayeredClusterLogAllocation.of(clusterInfo);
      final var nonFulfilledTopicPartitions =
          ClusterLogAllocation.findNonFulfilledAllocation(
              context.expectedAllocation(), currentLogAllocation);

      if (!nonFulfilledTopicPartitions.isEmpty()) {
        nonFulfilledTopicPartitions.forEach(
            topicPartition -> {
              final var expectedPlacements =
                  context.expectedAllocation().logPlacements(topicPartition);
              context.rebalanceAdmin().alterReplicaPlacements(topicPartition, expectedPlacements);
            });

        Supplier<Boolean> copyWorkDone =
            () ->
                context
                    .rebalanceAdmin()
                    .syncingProgress(nonFulfilledTopicPartitions)
                    .entrySet()
                    .stream()
                    .flatMap(list -> list.getValue().stream())
                    .allMatch(SyncingProgress::synced);
        Supplier<Boolean> allocationMatch =
            () -> {
              final var alloc =
                  LayeredClusterLogAllocation.of(context.rebalanceAdmin().clusterInfo());

              return context
                  .expectedAllocation()
                  .topicPartitionStream()
                  .allMatch(
                      tp ->
                          LogPlacement.isMatch(
                              context.expectedAllocation().logPlacements(tp),
                              alloc.logPlacements(tp)));
            };

        while (!(copyWorkDone.get() && allocationMatch.get())) {
          Utils.packException(() -> TimeUnit.SECONDS.sleep(3));
        }
      }
      return RebalanceExecutionResult.done();
    } catch (Exception e) {
      return RebalanceExecutionResult.failed(e);
    }
  }
}
