package org.astraea.balancer.alpha.cost;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.cost.BrokerCost;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasBrokerCost;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.KafkaMetrics;

public class TopicPartitionDistributionCost implements HasBrokerCost {

  @Override
  public Fetcher fetcher() {
    return Fetcher.of(List.of(KafkaMetrics.TopicPartition.Size::fetch));
  }

  @Override
  public BrokerCost brokerCost(ClusterInfo clusterInfo) {
    final Map<String, Integer> partitionCount =
        clusterInfo.topics().stream()
            .collect(Collectors.toUnmodifiableMap(x -> x, x -> clusterInfo.partitions(x).size()));

    final Map<Integer, Map<String, Long>> topicPartitionCountOnEachBroker =
        clusterInfo.nodes().stream()
            .map(
                node -> {
                  // map to Entry(node, partition count of each topic)

                  final Map<String, Long> partitionOnThisNode =
                      clusterInfo.topics().stream()
                          .map(topic -> Map.entry(topic, clusterInfo.partitions(topic)))
                          .collect(
                              Collectors.toUnmodifiableMap(
                                  Map.Entry::getKey,
                                  x ->
                                      x.getValue().stream()
                                          .filter(pInfo -> pInfo.replicas().contains(node))
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
