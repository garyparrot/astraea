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
package org.astraea.balancer.alpha.cost;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.app.cost.BrokerCost;
import org.astraea.app.cost.ClusterInfo;
import org.astraea.app.cost.HasBrokerCost;
import org.astraea.app.metrics.collector.Fetcher;

public class TopicPartitionDistributionCost implements HasBrokerCost {

  @Override
  public Fetcher fetcher() {
    return Fetcher.of(List.of());
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    final Map<String, Integer> partitionCount =
        clusterInfo.topics().stream()
            .collect(Collectors.toUnmodifiableMap(x -> x, x -> clusterInfo.replicas(x).size()));

    final Map<Integer, Map<String, Long>> topicPartitionCountOnEachBroker =
        clusterInfo.nodes().stream()
            .map(
                node -> {
                  // map to Entry(node, partition count of each topic)

                  final Map<String, Long> partitionOnThisNode =
                      clusterInfo.topics().stream()
                          .map(topic -> Map.entry(topic, clusterInfo.replicas(topic)))
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  Map.Entry::getKey,
                                  x ->
                                      x.getValue().stream()
                                          .filter(pInfo -> pInfo.nodeInfo().id() == node.id())
                                          .count()));

                  return Map.entry(node.id(), partitionOnThisNode);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    final var costOfEachBroker =
        topicPartitionCountOnEachBroker.entrySet().stream()
            .map(
                entry -> {
                  final var brokerId = entry.getKey();
                  final var cost =
                      entry.getValue().entrySet().stream()
                          .mapToDouble(
                              entry2 ->
                                  ((double) entry2.getValue())
                                      / (partitionCount.get(entry2.getKey())))
                          .max()
                          .orElseThrow();
                  return Map.entry(brokerId, cost);
                })
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    return () -> costOfEachBroker;
  }
}
