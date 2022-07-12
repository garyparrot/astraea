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
package org.astraea.app.cost;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.admin.ReplicaInfo;
import org.astraea.app.admin.TopicPartition;
import org.astraea.app.admin.TopicPartitionReplica;
import org.astraea.app.balancer.log.ClusterLogAllocation;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.broker.HasValue;
import org.astraea.app.metrics.collector.Fetcher;

/**
 * The result is computed by "Size.Value". "Size.Value" responds to the replica log size of brokers.
 * The calculation method of the score is the replica log usage space divided by the available space
 * on the hard disk
 */
public class ReplicaSizeCost implements HasBrokerCost, HasPartitionCost, HasClusterCost {
  Map<Integer, Map<String, Integer>> totalBrokerCapacity;
  Map<Integer, Map<String, Long>> totalReplicaSizeInPath;

  public ReplicaSizeCost(Map<Integer, Map<String, Integer>> totalBrokerCapacity) {
    this.totalBrokerCapacity = totalBrokerCapacity;
  }

  @Override
  public Optional<Fetcher> fetcher() {
    return Optional.of(KafkaMetrics.TopicPartition.Size::fetch);
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the ratio of the used space to the free space of each broker
   */
  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    var sizeOfReplica = getReplicaSize(clusterInfo);
    totalReplicaSizeInPath =
        clusterInfo.topics().stream()
            .flatMap(
                topic ->
                    clusterInfo.replicas(topic).stream()
                        .map(
                            replicaInfo ->
                                Map.entry(
                                    replicaInfo.nodeInfo().id(),
                                    Map.of(
                                        Objects.requireNonNull(
                                            replicaInfo.dataFolder().orElse(null)),
                                        sizeOfReplica.get(
                                            TopicPartitionReplica.of(
                                                replicaInfo.topic(),
                                                replicaInfo.partition(),
                                                replicaInfo.nodeInfo().id()))))))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (x1, x2) -> {
                      var path = x2.keySet().iterator().next();
                      var map = new HashMap<String, Long>();
                      map.putAll(x1);
                      map.putAll(x2);
                      if (x1.containsKey(path)) map.put(path, x1.get(path) + x2.get(path));
                      return map;
                    }));
    checkBrokerPath(totalReplicaSizeInPath);
    var brokerSizeScore = brokerScore();

    return () -> brokerSizeScore;
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a BrokerCost contains the ratio of the used space to the available space of replicas in
   *     each broker
   */
  @Override
  public PartitionCost partitionCost(ClusterInfo clusterInfo) {
    final long ONEMEGA = Math.round(Math.pow(2, 20));
    var sizeOfReplica = getReplicaSize(clusterInfo);
    TreeMap<TopicPartitionReplica, Double> replicaCost =
        new TreeMap<>(
            Comparator.comparing(TopicPartitionReplica::brokerId)
                .thenComparing(TopicPartitionReplica::topic)
                .thenComparing(TopicPartitionReplica::partition));
    sizeOfReplica.forEach((tpr, size) -> replicaCost.put(tpr, null));

    var scoreForTopic =
        clusterInfo.topics().stream()
            .map(
                topic ->
                    Map.entry(
                        topic,
                        clusterInfo.replicas(topic).stream()
                            .filter(ReplicaInfo::isLeader)
                            .map(
                                partitionInfo ->
                                    new TopicPartitionReplica(
                                        partitionInfo.topic(),
                                        partitionInfo.partition(),
                                        partitionInfo.nodeInfo().id()))
                            .map(
                                tpr -> {
                                  var score =
                                      replicaCost.entrySet().stream()
                                          .filter(
                                              x ->
                                                  x.getKey().topic().equals(tpr.topic())
                                                      && (x.getKey().partition()
                                                          == tpr.partition()))
                                          .mapToDouble(Map.Entry::getValue)
                                          .max()
                                          .orElseThrow(
                                              () ->
                                                  new IllegalStateException(
                                                      tpr + " topic/partition size not found"));
                                  if (score > 1) score = 1.0;
                                  return Map.entry(
                                      new TopicPartition(tpr.topic(), tpr.partition()), score);
                                })
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    Map.Entry::getKey, Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    var scoreForBroker =
        clusterInfo.nodes().stream()
            .map(
                node ->
                    Map.entry(
                        node.id(),
                        replicaCost.entrySet().stream()
                            .filter((tprScore) -> tprScore.getKey().brokerId() == node.id())
                            .collect(
                                Collectors.toMap(
                                    x ->
                                        new TopicPartition(
                                            x.getKey().topic(), x.getKey().partition()),
                                    Map.Entry::getValue))))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    return new PartitionCost() {

      @Override
      public Map<TopicPartition, Double> value(String topic) {
        return scoreForTopic.get(topic);
      }

      @Override
      public Map<TopicPartition, Double> value(int brokerId) {
        return scoreForBroker.get(brokerId);
      }
    };
  }

  void checkBrokerPath(Map<Integer, Map<String, Long>> totalReplicaSizeInPath) {
    totalReplicaSizeInPath.forEach(
        (key, value) -> {
          if (!totalBrokerCapacity.get(key).keySet().containsAll(value.keySet()))
            throw new IllegalArgumentException(
                "Path is not mount at broker ,check properties file");
        });
  }

  /**
   * @param clusterInfo the clusterInfo that offers the metrics related to topic/partition size
   * @return a map contain the replica log size of each topic/partition
   */
  public Map<TopicPartitionReplica, Long> getReplicaSize(ClusterInfo clusterInfo) {
    return clusterInfo.clusterBean().mapByReplica().entrySet().stream()
        .flatMap(
            e ->
                e.getValue().stream()
                    .filter(x -> x instanceof HasValue)
                    .filter(x -> x.beanObject().domainName().equals("kafka.log"))
                    .filter(x -> x.beanObject().properties().get("type").equals("Log"))
                    .filter(x -> x.beanObject().properties().get("name").equals("Size"))
                    .map(x -> (HasValue) x)
                    .map(x -> Map.entry(e.getKey(), x.value())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x1, x2) -> x2));
  }

  public Map<Integer, Double> brokerScore() {
    return totalReplicaSizeInPath.entrySet().stream()
        .map(
            brokerPath ->
                Map.entry(
                    brokerPath.getKey(),
                    brokerPath.getValue().entrySet().stream()
                        .mapToDouble(
                            x ->
                                x.getValue()
                                    / 1024.0
                                    / 1024.0
                                    / totalBrokerCapacity.get(brokerPath.getKey()).get(x.getKey())
                                    / totalBrokerCapacity.get(brokerPath.getKey()).size())
                        .sum()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public ClusterCost clusterCost(
      ClusterInfo clusterInfo, ClusterLogAllocation clusterLogAllocation) {
    var brokerSizeScore = brokerScore();
    var mean = brokerSizeScore.values().stream().mapToDouble(x -> x).sum() / brokerSizeScore.size();
    var sd =
        brokerSizeScore.values().stream().mapToDouble(score -> Math.sqrt(score - mean)).sum()
            / brokerSizeScore.size();
    return () -> sd;
  }
}
