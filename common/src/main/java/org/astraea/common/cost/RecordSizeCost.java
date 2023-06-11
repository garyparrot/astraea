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
package org.astraea.common.cost;

import static org.astraea.common.cost.CostUtils.changedRecordSizeOverflow;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.admin.Broker;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.admin.Replica;
import org.astraea.common.metrics.ClusterBean;

public class RecordSizeCost
    implements HasClusterCost, HasBrokerCost, HasMoveCost, HasPartitionCost {
  private final Configuration config;
  public static final String MAX_MIGRATE_SIZE_KEY = "max.migrated.size";

  public RecordSizeCost() {
    this.config = Configuration.EMPTY;
  }

  public RecordSizeCost(Configuration config) {
    this.config = config;
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.brokers().stream()
            .collect(
                Collectors.toMap(
                    Broker::id,
                    n -> clusterInfo.replicaStream(n.id()).mapToDouble(Replica::size).sum()));
    return () -> result;
  }

  @Override
  public MoveCost moveCost(ClusterInfo before, ClusterInfo after, ClusterBean clusterBean) {
    var maxMigratedSize =
        config
            .string(MAX_MIGRATE_SIZE_KEY)
            .map(DataSize::of)
            .map(DataSize::bytes)
            .orElse(Long.MAX_VALUE);
    var overflow = changedRecordSizeOverflow(before, after, ignored -> true, maxMigratedSize);
    return () -> overflow;
  }

  @Override
  public Collection<ResourceUsageHint> movementResourceHint(ClusterInfo sourceCluster, ClusterBean clusterBean) {
    var brokerSize = sourceCluster.replicas().stream()
        .collect(Collectors.groupingBy(Replica::brokerId,
            Collectors.summingLong(Replica::size)));
    var maxSize = sourceCluster.replicas().stream()
        .mapToDouble(Replica::size)
        .max().orElse(1);
    var allSize = sourceCluster.replicas().stream()
        .mapToDouble(Replica::size)
        .sum();

    return List.of(
        new ResourceUsageHint(){
          @Override
          public String description() {
            return "";
          }

          @Override
          public ResourceUsage evaluateReplicaResourceUsage(Replica target) {
            return new ResourceUsage(Map.of("size", (double) target.size()));
          }

          @Override
          public ResourceUsage evaluateClusterResourceUsage(Replica target) {
            var noMovement = sourceCluster.replicas(target.topicPartition()).contains(target);

            if(noMovement)
              return new ResourceUsage(Map.of("size_" + target.brokerId(), (double) target.size()));

            return new ResourceUsage(Map.of("migrate_size", (double) target.size()));
          }

          @Override
          public double importance(ResourceUsage replicaResourceUsage) {
            return (maxSize - replicaResourceUsage.usage().get("size")) / maxSize;
          }

          @Override
          public double idealness(ResourceUsage clusterResourceUsage) {
            var remove = sourceCluster.brokers().stream()
                .mapToDouble(x -> clusterResourceUsage.usage().getOrDefault("size_" + x.id(), 0.0)
                    - brokerSize.get(x.id()))
                .sum() / allSize;
            var add = clusterResourceUsage.usage().getOrDefault("migrated_size", 0.0) / allSize;

            return (remove + add) / 2.0;
          }
        });
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result =
        clusterInfo.replicaLeaders().stream()
            .collect(
                Collectors.groupingBy(
                    Replica::topicPartition,
                    Collectors.mapping(
                        r -> (double) r.size(), Collectors.reducing(0D, Math::max))));
    return () -> result;
  }

  @Override
  public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
    var result = clusterInfo.replicaStream().mapToLong(Replica::size).sum();
    return () -> result;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  public Configuration config() {
    return this.config;
  }
}
