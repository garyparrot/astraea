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
package org.astraea.app.balancer.alpha.cost;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.HasPartitionCost;
import org.astraea.app.cost.PartitionCost;
import org.astraea.app.metrics.collector.Fetcher;
import org.astraea.app.metrics.kafka.KafkaMetrics;

public class ReplicaMigrationSpeedCost implements HasPartitionCost {

  @Override
  public Fetcher fetcher() {
    return Fetcher.of(List.of(KafkaMetrics.TopicPartition.Size::fetch));
  }

  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    final var tpDataRate =
        ReplicaDiskInCost.topicPartitionDataRate(clusterInfo, Duration.ofSeconds(3));

    return new PartitionCost() {
      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return tpDataRate;
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return tpDataRate;
      }
    };
  }
}
