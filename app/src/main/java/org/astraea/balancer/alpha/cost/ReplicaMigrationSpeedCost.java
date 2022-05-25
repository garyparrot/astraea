package org.astraea.balancer.alpha.cost;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.astraea.admin.TopicPartition;
import org.astraea.cost.ClusterInfo;
import org.astraea.cost.HasPartitionCost;
import org.astraea.cost.PartitionCost;
import org.astraea.metrics.collector.Fetcher;
import org.astraea.metrics.kafka.KafkaMetrics;

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
