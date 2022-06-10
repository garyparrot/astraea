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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartitionReplica;
import org.astraea.app.admin.Admin;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.balancer.log.LogPlacement;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.metrics.HasBeanObject;

/**
 * The wrapper of {@link Admin}. Offer only the essential functionalities & some utilities to
 * perform rebalance operation.
 */
public interface RebalanceAdmin {

  static RebalanceAdmin of(
      Admin admin,
      Supplier<Map<Integer, Collection<HasBeanObject>>> metricSource,
      Predicate<String> topicFilter) {
    return new RebalanceAdminImpl(topicFilter, admin, metricSource);
  }

  /**
   * Attempt to migrate the target topic/partition to given replica log placement state. This method
   * will perform both replica list migration and data directory migration. This method will return
   * after triggering the migration. It won't wait until the migration processes are fulfilled.
   *
   * @param topicPartition the topic/partition to perform migration
   * @param expectedPlacement the expected placement after this request accomplished
   */
  List<RebalanceTask<TopicPartitionReplica, SyncingProgress>> alterReplicaPlacements(
      TopicPartition topicPartition, List<LogPlacement> expectedPlacement);

  /** Access the syncing progress of the specific topic/partitions */
  SyncingProgress syncingProgress(TopicPartitionReplica topicPartitionReplica);

  /** Wait until all given topic/partitions are synced. */
  boolean waitLogSynced(TopicPartitionReplica log, Duration timeout);

  /**
   * Wait until the given topic/partition have its preferred leader be the actual leader.
   *
   * @param topicPartition the topic/partition to wait
   * @param timeout for the waiting process
   * @return
   */
  boolean waitPreferredLeaderSynced(TopicPartition topicPartition, Duration timeout);

  /** Perform preferred leader election for specific topic/partition. */
  RebalanceTask<TopicPartition, Boolean> leaderElection(TopicPartition topicPartition);

  ClusterInfo clusterInfo();

  ClusterInfo refreshMetrics(ClusterInfo oldClusterInfo);

  // TODO: add method to apply reassignment bandwidth throttle.
  // TODO: add method to fetch topic configuration
  // TODO: add method to fetch broker configuration
}
